package raft

import (
	"context"
	"fmt"
	pb "modist/proto"
)

func sendAppendEntries(rn *RaftNode, init bool, higherTermChan chan uint64) {
	rn.log.Print("updateCommitIndex")
	for nd, _ := range rn.node.PeerNodes {
		if nd == rn.node.ID {
			continue
		}
		rn.log.Printf("Leader send AppendEntries to %v, lastLogIdx %v,commitIdx %v, nextIdx %v, matchIdx %v", nd, rn.LastLogIndex(), rn.commitIndex, rn.nextIndex, rn.matchIndex)
		// If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
		go func(nodeId uint64) {
			defer updateCommitIndex(rn)
			defer rn.log.Printf("Leader after sending AppendEntries to %v, lastLogIdx %v, commitIdx %v, nextIdx %v, matchIdx %v", nodeId, rn.LastLogIndex(), rn.commitIndex, rn.nextIndex, rn.matchIndex)
			rn.leaderMu.Lock()
			nextIdx := rn.nextIndex[nodeId]
			prevIdx := nextIdx - 1
			lastEntryIdx := rn.LastLogIndex()
			if prevIdx > lastEntryIdx {
				panic("prevIdx > lastEntryIdx: " + fmt.Sprintf("%v %v", prevIdx, lastEntryIdx))
			}
			rn.leaderMu.Unlock()
			var req *pb.AppendEntriesRequest
			if init {
				req = &pb.AppendEntriesRequest{
					From:         rn.node.ID,
					To:           nodeId,
					Term:         rn.GetCurrentTerm(),
					PrevLogIndex: prevIdx,
					PrevLogTerm:  rn.GetLog(prevIdx).Term,
					Entries:      nil,
					LeaderCommit: rn.commitIndex,
				}
			} else {
				entries := []*pb.LogEntry{}
				for i := nextIdx; i <= lastEntryIdx; i++ {
					rn.log.Printf("leader sent new entries to %v: %v", nodeId, rn.GetLog(i))
					entries = append(entries, rn.GetLog(i))
				}
				req = &pb.AppendEntriesRequest{
					From:         rn.node.ID,
					To:           nodeId,
					Term:         rn.GetCurrentTerm(),
					PrevLogIndex: prevIdx,
					PrevLogTerm:  rn.GetLog(prevIdx).Term,
					Entries:      entries,
					LeaderCommit: rn.commitIndex,
				}
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
			if rn.nextIndex[nodeId] < 1 {
				panic("nextIndex < 1")
			}
			if reply.Success {
				rn.log.Printf("Before update: %v nextIdx %v, matchIdx %v, lastEntryIdx %v", nodeId, rn.nextIndex, rn.matchIndex, lastEntryIdx)
				if init {
					rn.nextIndex[nodeId] = rn.nextIndex[nodeId] + 1
					rn.matchIndex[nodeId] = rn.matchIndex[nodeId] + 1
				} else {
					rn.nextIndex[nodeId] = lastEntryIdx + 1
					rn.matchIndex[nodeId] = lastEntryIdx
				}
				rn.log.Printf("After update: %v nextIdx %v, matchIdx %v, lastEntryIdx %v", nodeId, rn.nextIndex, rn.matchIndex, lastEntryIdx)
			} else {
				if rn.nextIndex[nodeId] > 1 {
					rn.nextIndex[nodeId] -= 1
				}
				return
			}
		}(nd)
	}
	updateCommitIndex(rn)
}

func updateCommitIndex(rn *RaftNode) {
	rn.log.Print("updateCommitIndex")
	rn.leaderMu.Lock()
	defer rn.leaderMu.Unlock()
	// If there exists an N such that N > commitIndex, a majority
	// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
	// set commitIndex = N
	N := rn.commitIndex
	newIdx := N
	rn.log.Printf("nextIdx %v", rn.nextIndex)
	rn.log.Printf("matchIdx %v", rn.matchIndex)
	rn.log.Printf("commitIdx %v", rn.commitIndex)
	lastIdx := rn.LastLogIndex()
	for N <= lastIdx {
		N += 1
		cnt := 0
		for _, v := range rn.matchIndex {
			if v >= N {
				cnt++
			}
		}
		rn.log.Printf("cnt %v N %v", cnt, N)
		rn.log.Print(rn.GetLog(N) != nil && rn.GetLog(N).Term == rn.GetCurrentTerm())
		if cnt >= (len(rn.node.PeerNodes)/2) && rn.GetLog(N) != nil && rn.GetLog(N).Term == rn.GetCurrentTerm() {
			newIdx = N
		}
	}
	rn.commitIndex = newIdx
	rn.log.Printf("commitIdx after %v", rn.commitIndex)
	for rn.commitIndex > rn.lastApplied {
		rn.lastApplied++
		if rn.GetLog(rn.lastApplied) == nil || rn.GetLog(rn.lastApplied).Data == nil {
			continue
		}
		rn.commitC <- (*commit)(&rn.GetLog(rn.lastApplied).Data)
	}
}

// func max(a, b uint64) uint64 {
// 	if a >= b {
// 		return a
// 	}
// 	return b
// }
