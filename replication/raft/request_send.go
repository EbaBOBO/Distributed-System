package raft

import (
	"context"
	pb "modist/proto"
)

func sendAppendEntries(rn *RaftNode, init bool, higherTermChan chan uint64) {
	for nd, _ := range rn.node.PeerNodes {
		if nd == rn.node.ID {
			continue
		}
		// If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
		go func(nodeId uint64) {
			rn.leaderMu.Lock()
			nextIdx := rn.nextIndex[nodeId]
			prevIdx := nextIdx - 1
			lastEntryIdx := rn.LastLogIndex()
			rn.leaderMu.Unlock()
			var req *pb.AppendEntriesRequest
			entriesLength := 0

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
				entriesLength = 1
			} else {
				entries := []*pb.LogEntry{}
				for i := nextIdx; i <= lastEntryIdx; i++ {
					rn.log.Printf("leader sent new entries to %v: %v", nodeId, rn.GetLog(i))
					entries = append(entries, rn.GetLog(i))
				}
				if rn.GetLog(prevIdx) == nil {
					a := rn.LastLogIndex()
					rn.log.Print(a)
					panic("nextIdx - 1 is nil")
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
				entriesLength = len(entries)
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
				rn.log.Printf("nextIndex add to %v with %v, lastEntryIdx %v", nodeId, entriesLength, lastEntryIdx)
				rn.nextIndex[nodeId] = max(lastEntryIdx+1, rn.nextIndex[nodeId])
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
		}(nd)
	}
}

func max(a, b uint64) uint64 {
	if a >= b {
		return a
	}
	return b
}
