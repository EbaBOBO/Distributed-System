package raft

import (
	"context"
	pb "modist/proto"
	"time"

	"google.golang.org/grpc"
)

// doLeader implements the logic for a Raft node in the leader state.
func (rn *RaftNode) doLeader() stateFunction {
	rn.log.Printf("transitioning to leader state at term %d", rn.GetCurrentTerm())
	rn.state = LeaderState

	// TODO(students): [Raft] Implement me!
	// Hint: perform any initial work, and then consider what a node in the
	// leader state should do when it receives an incoming message on every
	// possible channel.
	t := time.NewTicker(rn.heartbeatTimeout)
	for {
		select {
		case <-t.C:
			for k, v := range rn.node.PeerConns {
				if k == rn.node.ID {
					continue
				}
				go func(nodeId uint64, conn *grpc.ClientConn) {
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					remoteNode := pb.NewRaftRPCClient(conn)
					msgReq := &pb.AppendEntriesRequest{
						From:         rn.node.ID,
						To:           nodeId,
						Term:         rn.GetCurrentTerm(),
						PrevLogIndex: rn.lastApplied - 1,
						PrevLogTerm:  rn.GetLog(rn.lastApplied - 1).Term,
					}
					reply, err := remoteNode.AppendEntries(ctx, msgReq)
					if err != nil {
						rn.log.Printf("AppendEntries error: %v", err)
						return
					}
				}(k, v)
			}
		}
	}
	return nil
}
