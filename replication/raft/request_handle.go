package raft

import pb "modist/proto"

func handleAppendEntries(rn *RaftNode, nodeCurrentTerm uint64, appendReq *pb.AppendEntriesRequest) pb.AppendEntriesReply {
	// rn.log.Printf("AppendEntries from %v, term: %v, node term: %v", appendReq.From, appendReq.Term, nodeCurrentTerm)

	// Reply false if term < nodeCurrentTerm
	if appendReq.Term < nodeCurrentTerm {
		return pb.AppendEntriesReply{
			From:    rn.node.ID,
			To:      appendReq.From,
			Term:    nodeCurrentTerm,
			Success: false,
		}

	}
	rn.leader = appendReq.From
	// Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm
	if l := rn.GetLog(appendReq.PrevLogIndex); l == nil || l.Term != appendReq.PrevLogTerm {
		return pb.AppendEntriesReply{
			From:    rn.node.ID,
			To:      appendReq.From,
			Term:    nodeCurrentTerm,
			Success: false,
		}

	}
	// If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it
	for _, newEntry := range appendReq.Entries {
		idx, term := newEntry.Index, newEntry.Term
		if prevEntry := rn.GetLog(idx); prevEntry != nil && prevEntry.Term != term {
			rn.TruncateLog(idx)
			break
		}
	}

	//if l := rn.GetLog(appendReq.PrevLogIndex + 1); (l != nil) && (l.Term != appendReq.Term) {
	//	rn.TruncateLog(appendReq.PrevLogIndex + 1)
	//}
	// Append any new entries not already in the log
	for _, it := range appendReq.Entries {
		rn.StoreLog(it)
	}
	// If leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit, index of last new entry)
	if appendReq.LeaderCommit > rn.commitIndex {
		if appendReq.LeaderCommit <= rn.LastLogIndex() {
			rn.commitIndex = appendReq.LeaderCommit
		} else {
			rn.commitIndex = rn.LastLogIndex()
		}
	}

	return pb.AppendEntriesReply{
		From:    rn.node.ID,
		To:      appendReq.From,
		Term:    nodeCurrentTerm,
		Success: true,
	}
}

func handleRequestVote(rn *RaftNode, nodeCurrentTerm uint64, voteReq *pb.RequestVoteRequest) pb.RequestVoteReply {
	// Reply false if term < nodeCurrentTerm
	if voteReq.Term < nodeCurrentTerm {
		rn.log.Printf("requestVoteC From %v, To %v, term %v, false", voteReq.From, voteReq.To, voteReq.Term)
		return pb.RequestVoteReply{
			From:        rn.node.ID,
			To:          voteReq.From,
			Term:        nodeCurrentTerm,
			VoteGranted: false,
		}
	}
	// 	If votedFor is null or candidateId, and candidate’s log is at
	// least as up-to-date as receiver’s log, grant vote
	if rn.GetVotedFor() == None || rn.GetVotedFor() == voteReq.From {
		rn.log.Printf("requestVoteC From %v, To %v, term  %v, true", voteReq.From, voteReq.To, voteReq.Term)
		if voteReq.GetLastLogTerm() >= rn.GetLog(rn.LastLogIndex()).Term {
			if voteReq.GetLastLogTerm() == rn.GetLog(rn.LastLogIndex()).Term && voteReq.GetLastLogIndex() >= rn.LastLogIndex() {
				rn.setVotedFor(voteReq.From)
				return pb.RequestVoteReply{
					From:        rn.node.ID,
					To:          voteReq.From,
					Term:        nodeCurrentTerm,
					VoteGranted: true,
				}
			}
		}
	}
	return pb.RequestVoteReply{
		From:        rn.node.ID,
		To:          voteReq.From,
		Term:        nodeCurrentTerm,
		VoteGranted: false,
	}
}
