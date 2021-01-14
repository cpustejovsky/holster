package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/mailgun/holster/v3/election"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func sendRPC(ctx context.Context, peer string, req election.RPCRequest, resp *election.RPCResponse) error {
	// Marshall the RPC request to json
	b, err := json.Marshal(req)
	if err != nil {
		return errors.Wrap(err, "while encoding request")
	}

	// Create a new http request with context
	hr, err := http.NewRequest(http.MethodPost, fmt.Sprintf("http://%s/rpc", peer), bytes.NewBuffer(b))
	if err != nil {
		return errors.Wrap(err, "while creating request")
	}
	hr.WithContext(ctx)

	// Send the request
	hp, err := http.DefaultClient.Do(hr)
	if err != nil {
		return errors.Wrap(err, "while sending http request")
	}

	// Decode the response from JSON
	dec := json.NewDecoder(hp.Body)
	if err := dec.Decode(&resp); err != nil {
		return errors.Wrap(err, "while decoding response")
	}
	return nil
}

func newHandler(node election.Node) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		dec := json.NewDecoder(r.Body)
		var req election.RPCRequest
		if err := dec.Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(err.Error()))
		}
		var resp election.RPCResponse
		node.ReceiveRPC(req, &resp)

		enc := json.NewEncoder(w)
		if err := enc.Encode(resp); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
		}
	}
}

func main() {
	if len(os.Args) != 4 {
		logrus.Fatal("usage: <election-address:8080> <memberlist-address:8180> <known-address:8180>")
	}

	electionAddr, memberListAddr, knownAddr := os.Args[1], os.Args[2], os.Args[3]
	logrus.SetLevel(logrus.DebugLevel)
	fmt.Printf("%s - %s - %s\n", electionAddr, memberListAddr, knownAddr)

	node, err := election.SpawnNode(election.Config{
		// A unique identifier used to identify us in a list of peers
		Name: electionAddr,
		// Called whenever the library detects a change in leadership
		Observer: func(leader string) {
			logrus.Printf("Current Leader: %s\n", leader)
		},
		// Called when the library wants to contact other peers
		SendRPC: sendRPC,
	})
	if err != nil {
		logrus.Fatal(err)
	}

	// Create a member list pool
	pool, err := election.NewMemberListPool(context.Background(), election.MemberListPoolConfig{
		BindAddress: memberListAddr,
		PeerInfo: election.PeerInfo{
			HTTPAddress: electionAddr,
		},
		KnownNodes: []string{knownAddr},
		OnUpdate: func(peers []election.PeerInfo) {
			var result []string
			for _, p := range peers {
				result = append(result, p.HTTPAddress)
			}
			logrus.Infof("Update Peers: %s", result)
			if err := node.SetPeers(result); err != nil {
				logrus.Fatal(err)
			}
		},
	})
	if err != nil {
		logrus.Fatal(err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/rpc", newHandler(node))
	go func() {
		logrus.Fatal(http.ListenAndServe(electionAddr, mux))
	}()

	// Wait here for signals to clean up our mess
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	for range c {
		logrus.Info("pool close")
		pool.Close()
		logrus.Info("node close")
		node.Close()
		os.Exit(0)
	}
}
