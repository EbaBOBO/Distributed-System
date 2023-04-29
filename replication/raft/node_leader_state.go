package raft

import (
	"context"
	pb "modist/proto"
	"time"

	"google.golang.org/grpc"
)

// doLeader implements the logic for a Raft node in the leader state.
func (rn *RaftNode) doLeader() stateFunction {
	rn.log.Printf("+++++++++++++++++++++++++transitioning to leader state at term %d+++++++++++++++++++++++++", rn.GetCurrentTerm())
	rn.state = LeaderState

	// TODO(students): [Raft] Implement me!
	// Hint: perform any initial work, and then consider what a node in the
	// leader state should do when it receives an incoming message on every
	// possible channel.

	rn.leader = rn.node.ID
	for k := range rn.node.PeerNodes {
		rn.matchIndex[k] = 0
		rn.nextIndex[k] = rn.LastLogIndex() + 1
	}
	// initial heartbeat
	rn.StoreLog(&pb.LogEntry{
		Term:  rn.GetCurrentTerm(),
		Data:  nil,
		Type:  pb.EntryType_NORMAL,
		Index: rn.LastLogIndex() + 1,
	})

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
				Entries:      nil,
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
		select {
		case <-t.C:
			for k, v := range rn.node.PeerConns {
				if k == rn.node.ID {
					continue
				}
				go func(nodeId uint64, conn *grpc.ClientConn) {
					rn.log.Printf("leader sent heartbeat to %v", nodeId)
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
		// respond after entry applied to state machine
		case cmd := <-rn.proposeC:
			entry := pb.LogEntry{
				Index: rn.LastLogIndex() + 1,
				Term:  rn.GetCurrentTerm(),
				Type:  pb.EntryType_NORMAL,
				Data:  cmd,
			}
			rn.StoreLog(&entry)

		case msg := <-rn.requestVoteC:

			if msg.request.Term < rn.GetCurrentTerm() {
				rn.log.Printf("requestVoteC From %v, To %v, false", msg.request.From, msg.request.To)
				reply := pb.RequestVoteReply{
					From:        rn.node.ID,
					To:          msg.request.From,
					Term:        rn.GetCurrentTerm(),
					VoteGranted: false,
				}
				msg.reply <- reply
			}
			if (rn.GetVotedFor() == None ||
				rn.GetVotedFor() == msg.request.From) &&
				rn.LastLogIndex() >= msg.request.LastLogIndex {
				rn.log.Printf("requestVoteC From %v, To %v, true", msg.request.From, msg.request.To)
				rn.setVotedFor(msg.request.From)
				reply := pb.RequestVoteReply{
					From:        rn.node.ID,
					To:          msg.request.From,
					Term:        rn.GetCurrentTerm(),
					VoteGranted: true,
				}
				msg.reply <- reply
				return rn.doFollower
			}
		case appendEntry := <-rn.appendEntriesC:
			if appendEntry.request.Term > rn.GetCurrentTerm() {
				rn.SetCurrentTerm(appendEntry.request.Term)
				rn.leader = appendEntry.request.From
				return rn.doFollower
			}
		default:
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
						rn.log.Print(rn.GetLog(idx))
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
			N := rn.commitIndex
			for {
				N += 1
				cnt := 0
				for _, v := range rn.matchIndex {
					if v >= N {
						cnt++
					}
				}
				if cnt >= (len(rn.node.PeerNodes)/2)+1 && rn.GetLog(N).Term == rn.GetCurrentTerm() {
					rn.commitIndex = N
					continue
				} else {
					break
				}
			}

			if rn.commitIndex > rn.lastApplied {
				rn.lastApplied++
				rn.commitC <- (*commit)(&rn.GetLog(rn.lastApplied).Data)
			}
		}

	}
	return nil
}
