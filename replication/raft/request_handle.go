package raft

import pb "modist/proto"

func handleAppendEntries(rn *RaftNode, appendReq *pb.AppendEntriesRequest) pb.AppendEntriesReply {
	rn.log.Printf("AppendEntries from %v", appendReq.From)

	rn.leader = appendReq.From
	// Reply false if term < currentTerm
	if appendReq.Term < rn.GetCurrentTerm() {
		return pb.AppendEntriesReply{
			From:    rn.node.ID,
			To:      appendReq.From,
			Term:    rn.GetCurrentTerm(),
			Success: false,
		}

	}
	if appendReq.Term > rn.GetCurrentTerm() {
		rn.SetCurrentTerm(appendReq.Term)
	}
	// Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm
	if l := rn.GetLog(appendReq.PrevLogIndex); l == nil || l.Term != appendReq.PrevLogTerm {
		return pb.AppendEntriesReply{
			From:    rn.node.ID,
			To:      appendReq.From,
			Term:    rn.GetCurrentTerm(),
			Success: false,
		}

	}
	// If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it
	if l := rn.GetLog(appendReq.PrevLogIndex + 1); (l != nil) && (l.Term != appendReq.Term) {
		rn.TruncateLog(appendReq.PrevLogIndex + 1)
	}
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
		Term:    rn.GetCurrentTerm(),
		Success: true,
	}
}

func handleRequestVote(rn *RaftNode, voteReq *pb.RequestVoteRequest) pb.RequestVoteReply {
	// Reply false if term < currentTerm
	if voteReq.Term < rn.GetCurrentTerm() {
		rn.log.Printf("requestVoteC From %v, To %v, term %v, false", voteReq.From, voteReq.To, voteReq.Term)
		return pb.RequestVoteReply{
			From:        rn.node.ID,
			To:          voteReq.From,
			Term:        rn.GetCurrentTerm(),
			VoteGranted: false,
		}
	}
	// 	If votedFor is null or candidateId, and candidate’s log is at
	// least as up-to-date as receiver’s log, grant vote
	if rn.GetVotedFor() == None || rn.GetVotedFor() == voteReq.From {
		rn.log.Printf("requestVoteC From %v, To %v, term  %v, true", voteReq.From, voteReq.To, voteReq.Term)
		rn.setVotedFor(voteReq.From)
		return pb.RequestVoteReply{
			From:        rn.node.ID,
			To:          voteReq.From,
			Term:        rn.GetCurrentTerm(),
			VoteGranted: true,
		}
	}
	return pb.RequestVoteReply{
		From:        rn.node.ID,
		To:          voteReq.From,
		Term:        rn.GetCurrentTerm(),
		VoteGranted: false,
	}
}