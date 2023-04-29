package raft

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"modist/orchestrator/node"
	pb "modist/proto"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

// RETRIES is the number of retries upon failure. By default, we have 3
const RETRIES = 3

// RETRY_TIME is the amount of time to wait between retries
const RETRY_TIME = 100 * time.Millisecond

type State struct {
	// In-memory key value store
	store map[string]string
	mu    sync.RWMutex

	// Channels given back by the underlying Raft implementation
	proposeC chan<- []byte
	commitC  <-chan *commit

	// Observability
	log *log.Logger

	// The public-facing API that this replicator must implement
	pb.ReplicatorServer

	successChans      map[string]chan string
	successChansMutex sync.Mutex
}

type Args struct {
	Node   *node.Node
	Config *Config
}

// Configure is called by the orchestrator to start this node
//
// The "args" are any to support any replicator that might need arbitrary
// set of configuration values.
//
// You are free to add any fields to the State struct that you feel may help
// with implementing the ReplicateKey or GetReplicatedKey functions. Likewise,
// you may call any additional functions following the second TODO. Apart
// from the two TODOs, DO NOT EDIT any other part of this function.
func Configure(args any) *State {
	a := args.(Args)

	node := a.Node
	config := a.Config

	proposeC := make(chan []byte)
	commitC := NewRaftNode(node, config, proposeC)

	s := &State{
		store:    make(map[string]string),
		proposeC: proposeC,
		commitC:  commitC,

		log: node.Log,

		// TODO(students): [Raft] Initialize any additional fields and add to State struct
		successChans: make(map[string]chan string),
	}

	// We registered RaftRPCServer when calling NewRaftNode above so we only need to
	// register ReplicatorServer here
	s.log.Printf("starting gRPC server at %s", node.Addr.Host)
	grpcServer := node.GrpcServer
	pb.RegisterReplicatorServer(grpcServer, s)
	go grpcServer.Serve(node.Listener)

	// TODO(students): [Raft] Call helper functions if needed
	go func() {
		s.log.Printf("starting commit channel listener on node %v", node.ID)
		for {
			bytes, ok := <-s.commitC
			if !ok {
				s.log.Printf("commit channel closed, exit")
				return
			}
			kv := RaftKVPair{}
			json.Unmarshal(*bytes, &kv)
			s.mu.Lock()
			s.store[kv.Key] = kv.Value
			s.mu.Unlock()

			s.successChansMutex.Lock()
			successChan, ok := s.successChans[kv.UUID]
			s.successChansMutex.Unlock()

			if !ok {
				s.log.Printf("no success channel for %s", kv.UUID)
			} else {
				successChan <- "success"
			}
			s.log.Printf("store updated: %s -> %s", kv.Key, kv.Value)
		}
	}()

	return s
}

type RaftKVPair struct {
	Key   string
	Value string
	UUID  string
}

// ReplicateKey replicates the (key, value) given in the PutRequest by relaying it to the
// underlying Raft cluster/implementation.
//
// You should first marshal the KV struct using the encoding/json library. Then, put the
// marshalled struct in the proposeC channel and wait until the Raft cluster has confirmed that
// it has been committed. Think about how you can use the commitC channel to find out when
// something has been committed.
//
// If the proposal has not been committed after RETRY_TIME, you should retry it. You should retry
// the proposal a maximum of RETRIES times, at which point you can return a nil reply and an error.
func (s *State) ReplicateKey(ctx context.Context, r *pb.PutRequest) (*pb.PutReply, error) {
	// TODO(students): [Raft] Implement me!
	kvUUID := uuid.New().String()
	msg := RaftKVPair{
		Key:   r.Key,
		Value: r.Value,
		UUID:  kvUUID,
	}
	bytes, err := json.Marshal(msg)
	if err != nil {
		s.log.Printf("error marshalling: %v", err)
		return nil, err
	}
	successChan := make(chan string)

	s.successChansMutex.Lock()
	s.successChans[kvUUID] = successChan
	s.successChansMutex.Unlock()

	defer func() {
		s.successChansMutex.Lock()
		delete(s.successChans, kvUUID)
		s.successChansMutex.Unlock()
	}()

	s.proposeC <- bytes
	t := time.NewTimer(RETRY_TIME)
	var retries atomic.Int32
	retries.Store(0)
	for retries.Load() < RETRIES {
		select {
		case <-t.C:
			// need to retry
			s.proposeC <- bytes
			retries.Add(1)
		case <-successChan:
			// success
			return &pb.PutReply{}, nil
		}
	}
	return nil, errors.New("ReplicateKey error: retries exceeded")
}

// GetReplicatedKey reads the given key from s.store. The implementation of
// this method should be pretty short (roughly 8-12 lines).
func (s *State) GetReplicatedKey(ctx context.Context, r *pb.GetRequest) (*pb.GetReply, error) {

	// TODO(students): [Raft] Implement me!
	reply := pb.GetReply{
		Value: s.store[r.Key],
	}
	return &reply, nil
}
