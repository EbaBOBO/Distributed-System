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

	// initial heartbeat
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
				PrevLogIndex: rn.LastLogIndex() - 1,
				PrevLogTerm:  rn.GetLog(rn.LastLogIndex() - 1).Term,
				Entries:      []*pb.LogEntry{},
				LeaderCommit: rn.commitIndex,
			}
			reply, err := remoteNode.AppendEntries(ctx, msgReq)
			rn.log.Printf("AppendEntries reply from leader: %v", reply)
			if err != nil {
				rn.log.Printf("AppendEntries error: %v", err)
				return
			}
		}(k, v)
	}

	t := time.NewTicker(rn.heartbeatTimeout)

	for {
		for k, v := range rn.nextIndex {
			// last log index >= next index
			if rn.LastLogIndex() >= v {
				// send appendEntries to peer
				go func(nodeId uint64, idx uint64) {
					conn := rn.node.PeerConns[nodeId]
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					remoteNode := pb.NewRaftRPCClient(conn)
					req := &pb.AppendEntriesRequest{
						From:         rn.node.ID,
						To:           nodeId,
						Term:         rn.GetCurrentTerm(),
						PrevLogIndex: rn.LastLogIndex() - 1,
						PrevLogTerm:  rn.GetLog(rn.LastLogIndex() - 1).Term,
						Entries:      []*pb.LogEntry{rn.GetLog(idx)},
						LeaderCommit: rn.commitIndex,
					}
					reply, err := remoteNode.AppendEntries(ctx, req)
					if err != nil {
						rn.log.Printf("AppendEntries error: %v", err)
					}
					// Update nextIndex and matchIndex for the follower if successful
					rn.leaderMu.Lock()
					defer rn.leaderMu.Unlock()
					if reply.Success {
						rn.nextIndex[nodeId] += 1
						rn.matchIndex[nodeId] += 1
					} else {
						rn.nextIndex[nodeId] -= 1
					}
				}(k, v)

			}
		}
		// If there exists an N such that N > commitIndex, a majority
		// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
		// set commitIndex = N
		N := rn.commitIndex + 1
		// find min element in matchIndex
		minIdx := rn.matchIndex[rn.node.ID]
		for _, v := range rn.matchIndex {
			if v < minIdx {
				minIdx = v
			}
		}
		for N >= minIdx {
			cnt := 0
			for _, v := range rn.matchIndex {
				if v >= N {
					cnt += 1
				}
			}
			if cnt > len(rn.node.PeerConns)/2 && rn.GetLog(N).Term == rn.GetCurrentTerm() {
				rn.commitIndex = N
				break
			}
			N--
		}
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
						PrevLogIndex: rn.LastLogIndex() - 1,
						PrevLogTerm:  rn.GetLog(rn.LastLogIndex() - 1).Term,
						Entries:      []*pb.LogEntry{},
						LeaderCommit: rn.commitIndex,
					}
					reply, err := remoteNode.AppendEntries(ctx, msgReq)
					rn.log.Printf("AppendEntries reply from leader: %v", reply)
					if err != nil {
						rn.log.Printf("AppendEntries error: %v", err)
						return
					}
				}(k, v)
			}
		// If command received from client: append entry to local log,
		// respond after entry applied to state machine ???
		case cmd := <-rn.proposeC:
			entry := pb.LogEntry{
				Index: rn.LastLogIndex() + 1,
				Term:  rn.GetCurrentTerm(),
				Type:  pb.EntryType_NORMAL,
				Data:  cmd,
			}
			rn.StoreLog(&entry)
		}
	}
	return nil
}
