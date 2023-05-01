package raft

import (
	"context"
	"encoding/json"
	pb "modist/proto"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
)

// doLeader implements the logic for a Raft node in the leader state.
func (rn *RaftNode) doLeader() stateFunction {
	nodeCurrentTerm := rn.GetCurrentTerm()
	rn.log.Printf("+++++++++++++++++++++++++transitioning to leader state at term %d+++++++++++++++++++++++++", nodeCurrentTerm)
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
		Term:  nodeCurrentTerm,
		Data:  nil,
		Type:  pb.EntryType_NORMAL,
		Index: rn.LastLogIndex() + 1,
	})
	// send initial empty AppendEntries RPCs (heartbeat) to each server
	var higherTerm atomic.Bool
	higherTerm.Store(false)
	higherTermChan := make(chan uint64, 1)

	for k, v := range rn.node.PeerConns {
		if k == rn.node.ID {
			continue
		}
		go func(nodeId uint64, conn *grpc.ClientConn) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			remoteNode := pb.NewRaftRPCClient(conn)
			prevIdx := rn.nextIndex[nodeId] - 1
			msgReq := &pb.AppendEntriesRequest{
				From:         rn.node.ID,
				To:           nodeId,
				Term:         nodeCurrentTerm,
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
			if reply.Term > nodeCurrentTerm && higherTerm.CompareAndSwap(false, true) {
				higherTermChan <- reply.Term
			}
		}(k, v)
	}

	t := time.NewTicker(rn.heartbeatTimeout)

	for {
		select {
		case <-t.C:
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
					req := &pb.AppendEntriesRequest{
						From:         rn.node.ID,
						To:           nodeId,
						Term:         nodeCurrentTerm,
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
					}
					if reply.Term > nodeCurrentTerm && higherTerm.CompareAndSwap(false, true) {
						higherTermChan <- reply.Term
					}
				}(k, v, lastIdx)
			}
			// If there exists an N such that N > commitIndex, a majority
			// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
			// set commitIndex = N
			N := rn.commitIndex
			rn.log.Print(rn.nextIndex)
			for {
				N += 1
				cnt := 0
				for _, v := range rn.matchIndex {
					if v >= N {
						cnt++
					}
				}
				if cnt >= (len(rn.node.PeerNodes)/2)+1 && rn.GetLog(N) != nil && rn.GetLog(N).Term == nodeCurrentTerm {
					rn.commitIndex = N
					continue
				} else {
					break
				}
			}
			rn.log.Print(N)
			if rn.commitIndex > rn.lastApplied {
				rn.lastApplied++
				rn.commitC <- (*commit)(&rn.GetLog(rn.lastApplied).Data)
			}

		// If command received from client: append entry to local log,
		// respond after entry applied to state machine
		case msg, ok := <-rn.proposeC:

			if !ok {
				return nil
			}
			entry := pb.LogEntry{
				Index: rn.LastLogIndex() + 1,
				Term:  nodeCurrentTerm,
				Type:  pb.EntryType_NORMAL,
				Data:  msg,
			}
			rn.StoreLog(&entry)
			kv := RaftKVPair{}
			json.Unmarshal(msg, &kv)
			rn.log.Printf("leader received proposal: %v", kv)

		case msg := <-rn.requestVoteC:
			reply := handleRequestVote(rn, nodeCurrentTerm, msg.request)
			msg.reply <- reply
			rn.log.Printf("leader term %v received requestVote: %v", nodeCurrentTerm, &reply)
			if msg.request.Term > nodeCurrentTerm && higherTerm.CompareAndSwap(false, true) {
				higherTermChan <- msg.request.Term
			}
		case msg := <-rn.appendEntriesC:
			reply := handleAppendEntries(rn, nodeCurrentTerm, msg.request)
			msg.reply <- reply
			if msg.request.Term > nodeCurrentTerm && higherTerm.CompareAndSwap(false, true) {
				higherTermChan <- msg.request.Term
			}
		case msg := <-higherTermChan:
			rn.log.Printf("leader term %v received AppendEntries reply with higher term:%v", nodeCurrentTerm, msg)
			rn.SetCurrentTerm(msg)
			return rn.doFollower
		}

	}
}
