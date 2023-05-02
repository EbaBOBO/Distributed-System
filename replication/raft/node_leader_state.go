package raft

import (
	"context"
	"encoding/json"
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
	// (Reinitialized after election)
	// nextIndex[]
	// for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	// matchIndex[]
	// for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
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
	// send initial empty AppendEntries RPCs (heartbeat) to each server
	higherTermChan := make(chan uint64, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for k, v := range rn.node.PeerConns {
		if k == rn.node.ID {
			continue
		}
		lastEntryIdx := rn.LastLogIndex()
		go func(nodeId uint64, conn *grpc.ClientConn) {
			remoteNode := pb.NewRaftRPCClient(conn)
			prevIdx := rn.nextIndex[nodeId] - 1
			msgReq := &pb.AppendEntriesRequest{
				From:         rn.node.ID,
				To:           nodeId,
				Term:         rn.GetCurrentTerm(),
				PrevLogIndex: prevIdx,
				PrevLogTerm:  rn.GetLog(prevIdx).Term,
				Entries:      nil,
				LeaderCommit: rn.commitIndex,
			}
			reply, err := remoteNode.AppendEntries(ctx, msgReq)
			rn.log.Printf("AppendEntries reply from leader: %v", reply)
			if err != nil {
				rn.log.Printf("AppendEntries error: %v", err)
				return
			}
			if reply.Term > rn.GetCurrentTerm() {
				higherTermChan <- reply.Term
				return
			}
			// Update nextIndex and matchIndex for the follower if successful
			rn.leaderMu.Lock()
			defer rn.leaderMu.Unlock()
			if reply.Success {
				rn.nextIndex[nodeId] += 1
				rn.matchIndex[nodeId] = lastEntryIdx
			} else {
				rn.nextIndex[nodeId] -= 1
				return
			}
			// If there exists an N such that N > commitIndex, a majority
			// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
			// set commitIndex = N
			N := rn.commitIndex
			rn.log.Printf("commitIdx before %v", rn.commitIndex)
			rn.log.Printf("nextIdx %v", rn.nextIndex)
			rn.log.Printf("matchIdx %v", rn.matchIndex)
			for {
				N += 1
				cnt := 0
				for _, v := range rn.matchIndex {
					if v >= N {
						cnt++
					}
				}
				if cnt >= (len(rn.node.PeerNodes)/2) && rn.GetLog(N).Term == rn.GetCurrentTerm() {
					rn.commitIndex = N
					continue
				} else {
					break
				}
			}
			rn.log.Printf("commitIdx after  %v", rn.commitIndex)
			for rn.commitIndex > rn.lastApplied {
				rn.lastApplied++
				if rn.GetLog(rn.lastApplied).Data == nil {
					continue
				}
				rn.commitC <- (*commit)(&rn.GetLog(rn.lastApplied).Data)
			}
		}(k, v)
	}

	t := time.NewTicker(rn.heartbeatTimeout)

	for {
		select {
		case <-t.C:
			rn.log.Printf("commitIdx %v", rn.commitIndex)
			rn.log.Printf("leader sending heartbeat, commitIdx %v", rn.commitIndex)
			// repeat during idle periods to prevent election timeouts
			for k, v := range rn.nextIndex {
				if k == rn.node.ID {
					continue
				}
				lastIdx := rn.LastLogIndex()
				// If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
				go func(nodeId uint64, nextIdx uint64, lastEntryIdx uint64) {
					entries := []*pb.LogEntry{}
					for i := nextIdx; i <= lastEntryIdx; i++ {
						rn.log.Printf("leader sent new entries to %v: %v", nodeId, rn.GetLog(i))
						entries = append(entries, rn.GetLog(i))
					}
					if rn.GetLog(nextIdx-1) == nil {
						panic("nextIdx - 1 is nil")
					}
					req := &pb.AppendEntriesRequest{
						From:         rn.node.ID,
						To:           nodeId,
						Term:         rn.GetCurrentTerm(),
						PrevLogIndex: nextIdx - 1,
						PrevLogTerm:  rn.GetLog(nextIdx - 1).Term,
						Entries:      entries,
						LeaderCommit: rn.commitIndex,
					}
					conn := rn.node.PeerConns[nodeId]
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					remoteNode := pb.NewRaftRPCClient(conn)
					reply, err := remoteNode.AppendEntries(ctx, req)
					if err != nil {
						rn.log.Printf("AppendEntries error: %v", err)
						return
					}
					if reply.Term > rn.GetCurrentTerm() {
						higherTermChan <- reply.Term
						return
					}
					// Update nextIndex and matchIndex for the follower if successful
					rn.leaderMu.Lock()
					defer rn.leaderMu.Unlock()
					if reply.Success {
						rn.log.Printf("nextIndex add to %v with %v, lastEntryIdx %v", nodeId, len(entries), lastEntryIdx)
						rn.nextIndex[nodeId] += uint64(len(entries))
						rn.matchIndex[nodeId] = lastEntryIdx
					} else {
						rn.nextIndex[nodeId] -= 1
						return
					}
					// If there exists an N such that N > commitIndex, a majority
					// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
					// set commitIndex = N
					N := rn.commitIndex
					rn.log.Printf("nextIdx %v", rn.nextIndex)
					rn.log.Printf("matchIdx %v", rn.matchIndex)
					rn.log.Printf("commitIdx %v", rn.commitIndex)
					for {
						N += 1
						cnt := 0
						for _, v := range rn.matchIndex {
							if v >= N {
								cnt++
							}
						}
						if cnt >= (len(rn.node.PeerNodes)/2) && rn.GetLog(N) != nil && rn.GetLog(N).Term == rn.GetCurrentTerm() {
							rn.commitIndex = N
							continue
						} else {
							break
						}
					}
					rn.log.Printf("commitIdx after %v", rn.commitIndex)
					for rn.commitIndex > rn.lastApplied {
						rn.lastApplied++
						if rn.GetLog(rn.lastApplied).Data == nil {
							continue
						}
						rn.commitC <- (*commit)(&rn.GetLog(rn.lastApplied).Data)
					}
					if reply.Term > rn.GetCurrentTerm() {
						higherTermChan <- reply.Term
					}
				}(k, v, lastIdx)
			}

		// If command received from client: append entry to local log,
		// respond after entry applied to state machine
		case msg, ok := <-rn.proposeC:
			rn.log.Printf("leader received proposal: %v", msg)
			rn.log.Printf("commitIdx %v", rn.commitIndex)
			rn.log.Printf("nextIdx %v", rn.nextIndex)
			rn.log.Printf("matchIdx %v", rn.matchIndex)
			if !ok {
				rn.log.Printf("Stop")
				rn.Stop()
				return nil
			}
			entry := pb.LogEntry{
				Index: rn.LastLogIndex() + 1,
				Term:  rn.GetCurrentTerm(),
				Type:  pb.EntryType_NORMAL,
				Data:  msg,
			}
			rn.StoreLog(&entry)
			kv := RaftKVPair{}
			json.Unmarshal(msg, &kv)

		case msg := <-rn.requestVoteC:
			rn.log.Printf("nextIdx %v", rn.nextIndex)
			rn.log.Printf("matchIdx %v", rn.matchIndex)
			rn.log.Printf("commitIdx %v", rn.commitIndex)
			rn.log.Printf("leader term %v received requestVote: %v", rn.GetCurrentTerm(), msg.request)
			reply := handleRequestVote(rn, msg.request)
			msg.reply <- reply
			rn.log.Printf("leader term %v received requestVote: %v", rn.GetCurrentTerm(), &reply)
			if reply.VoteGranted {
				rn.SetCurrentTerm(msg.request.Term)
				return rn.doFollower
			}

		case msg := <-rn.appendEntriesC:
			rn.log.Printf("leader term %v received AppendEntries: %v", rn.GetCurrentTerm(), msg.request)
			reply := handleAppendEntries(rn, msg.request)
			msg.reply <- reply
			if msg.request.Term > rn.GetCurrentTerm() {
				higherTermChan <- msg.request.Term
			}
		case msg := <-higherTermChan:
			rn.log.Printf("leader term %v received AppendEntries reply with higher term:%v", rn.GetCurrentTerm(), msg)
			rn.SetCurrentTerm(msg)
			return rn.doFollower
		}

	}
}
