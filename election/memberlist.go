package election

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"runtime"
	"strconv"

	"github.com/davecgh/go-spew/spew"
	ml "github.com/hashicorp/memberlist"
	"github.com/mailgun/holster/v3/clock"
	"github.com/mailgun/holster/v3/errors"
	"github.com/mailgun/holster/v3/retry"
	"github.com/mailgun/holster/v3/setter"
	"github.com/sirupsen/logrus"
)

type PeerInfo struct {
	// The http address:port of the peer
	HTTPAddress string `json:"http-address"`
	// The grpc address:port of the peer
	GRPCAddress string `json:"grpc-address"`
	// Is true if PeerInfo matches the Name as given in the memberlist config
	IsOwner bool `json:"is-owner,omitempty"`
}

type UpdateFunc func([]PeerInfo)

type MemberListPool struct {
	log        logrus.FieldLogger
	memberList *ml.Memberlist
	conf       MemberListPoolConfig
	events     *eventDelegate
}

type MemberListPoolConfig struct {
	// This is the address:port the member list protocol will advertise to other members. (Defaults to BindAddress)
	AdvertiseAddress string

	// This is the address:port the member list protocol listen for other members on
	BindAddress string

	// The information about this peer which should be shared with all other members
	PeerInfo PeerInfo

	// A list of nodes this member list instance can contact to find other members.
	KnownNodes []string

	// A callback function which is called when the member list changes
	OnUpdate UpdateFunc

	// An interface through which logging will occur (Usually *logrus.Entry)
	Logger logrus.FieldLogger
}

func NewMemberListPool(ctx context.Context, conf MemberListPoolConfig) (*MemberListPool, error) {
	setter.SetDefault(&conf.Logger, logrus.WithField("category", "gubernator"))
	setter.SetDefault(&conf.AdvertiseAddress, conf.BindAddress)
	conf.PeerInfo.IsOwner = false

	m := &MemberListPool{
		log:  conf.Logger,
		conf: conf,
		events: &eventDelegate{
			peers: make(map[string]PeerInfo, 1),
			conf:  conf,
			log:   conf.Logger,
		},
	}

	// Create the member list config
	config, err := m.newMLConfig(conf)
	if err != nil {
		return nil, err
	}

	// Create a new member list instance
	m.memberList, err = ml.Create(config)
	if err != nil {
		return nil, err
	}

	// Attempt to join the member list using a list of known nodes
	err = retry.Until(ctx, retry.Interval(clock.Millisecond*300), func(ctx context.Context, i int) error {
		// Join member list
		_, err = m.memberList.Join(m.conf.KnownNodes)
		if err != nil {
			return errors.Wrap(err, "while joining member-list")
		}
		return nil
	})
	return m, errors.Wrap(err, "timed out attempting to join member list")
}

func (m *MemberListPool) newMLConfig(conf MemberListPoolConfig) (*ml.Config, error) {
	config := ml.DefaultWANConfig()
	config.Name = conf.PeerInfo.HTTPAddress
	config.LogOutput = newLogWriter(conf.Logger)

	var err error
	config.BindAddr, config.BindPort, err = splitAddress(conf.BindAddress)
	if err != nil {
		return nil, errors.Wrap(err, "BindAddress=`%s` is invalid;")
	}

	config.AdvertiseAddr, config.AdvertisePort, err = splitAddress(conf.AdvertiseAddress)
	if err != nil {
		return nil, errors.Wrap(err, "AdvertiseAddress=`%s` is invalid;")
	}

	b, err := json.Marshal(&conf.PeerInfo)
	if err != nil {
		return nil, errors.Wrap(err, "error marshalling metadata")
	}
	config.Delegate = &delegate{meta: b}
	config.Events = m.events
	return config, nil
}

func (m *MemberListPool) Close() {
	err := m.memberList.Leave(clock.Second)
	if err != nil {
		m.log.Warn(errors.Wrap(err, "while leaving member-list"))
	}
}

type eventDelegate struct {
	peers map[string]PeerInfo
	log   logrus.FieldLogger
	conf  MemberListPoolConfig
}

func (e *eventDelegate) NotifyJoin(node *ml.Node) {
	var metadata PeerInfo
	if err := json.Unmarshal(node.Meta, &metadata); err != nil {
		fmt.Printf("Join Error: '%s' - %s\n", node.Meta, err)
		e.log.WithError(err).Warn("while deserialize member-list metadata")
		return
	}
	e.peers[node.Address()] = metadata
	spew.Printf("Join: %#v\n", e.peers)
	e.callOnUpdate()
}

func (e *eventDelegate) NotifyLeave(node *ml.Node) {
	delete(e.peers, node.Address())
	spew.Printf("Leave: %#v\n", e.peers)
	e.callOnUpdate()
}

func (e *eventDelegate) NotifyUpdate(node *ml.Node) {
	// Deserialize metadata
	var metadata PeerInfo
	if err := json.Unmarshal(node.Meta, &metadata); err != nil {
		e.log.WithError(err).Warn("while updating member-list")
		return
	}
	e.peers[node.Address()] = metadata
	spew.Printf("Update: %#v\n", e.peers)
	e.callOnUpdate()
}

func (e *eventDelegate) callOnUpdate() {
	fmt.Printf("CALL ON UPDATE\n")
	var peers []PeerInfo

	for _, p := range e.peers {
		if p.HTTPAddress == e.conf.PeerInfo.HTTPAddress {
			p.IsOwner = true
		}
		peers = append(peers, p)
	}
	e.conf.OnUpdate(peers)
}

type delegate struct {
	meta []byte
}

func (m *delegate) NodeMeta(limit int) []byte {
	return m.meta
}
func (m *delegate) NotifyMsg([]byte)                {}
func (m *delegate) GetBroadcasts(int, int) [][]byte { return nil }
func (m *delegate) LocalState(bool) []byte          { return nil }
func (m *delegate) MergeRemoteState([]byte, bool)   {}

func newLogWriter(log logrus.FieldLogger) *io.PipeWriter {
	reader, writer := io.Pipe()

	go func() {
		scanner := bufio.NewScanner(reader)
		for scanner.Scan() {
			log.Info(scanner.Text())
		}
		if err := scanner.Err(); err != nil {
			log.Errorf("Error while reading from Writer: %s", err)
		}
		reader.Close()
	}()
	runtime.SetFinalizer(writer, func(w *io.PipeWriter) {
		writer.Close()
	})

	return writer
}

func split(addr string) (string, int, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return host, 0, errors.New(" expected format is `address:port`")
	}

	intPort, err := strconv.Atoi(port)
	if err != nil {
		return host, intPort, errors.Wrap(err, "port must be a number")
	}
	return host, intPort, nil
}

func splitAddress(addr string) (string, int, error) {
	host, port, err := split(addr)
	if err != nil {
		return "", 0, err
	}
	// Member list requires the address to be an ip address
	if ip := net.ParseIP(host); ip == nil {
		addrs, err := net.LookupHost(host)
		if err != nil {
			return "", 0, errors.Wrapf(err, "while preforming host lookup for '%s'", host)
		}
		if len(addrs) == 0 {
			return "", 0, errors.Wrapf(err, "net.LookupHost() returned no addresses for '%s'", host)
		}
		host = addrs[0]
	}
	return host, port, nil
}
