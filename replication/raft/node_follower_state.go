package raft

import (
	"context"
	pb "modist/proto"
	"time"
)

// doFollower implements the logic for a Raft node in the follower state.
func (rn *RaftNode) doFollower() stateFunction {
	rn.state = FollowerState
	rn.log.Printf("transitioning to %s state at term %d", rn.state, rn.GetCurrentTerm())

	// TODO(students): [Raft] Implement me!
	// Hint: perform any initial work, and then consider what a node in the
	// follower state should do when it receives an incoming message on every
	// possible channel.
	t := time.NewTicker(rn.electionTimeout)
	rn.setVotedFor(None)
	for {
		if rn.commitIndex > rn.lastApplied {
			rn.lastApplied++
			rn.commitC <- (*commit)(&rn.GetLog(rn.lastApplied).Data)
		}
		select {
		case <-t.C:
			// heartbeat timeout
			return rn.doCandidate
		case vote := <-rn.requestVoteC:
			t.Reset(rn.electionTimeout)
			req := vote.request
			replyChan := vote.reply
			if req.Term < rn.GetCurrentTerm() {
				replyChan <- pb.RequestVoteReply{
					From:        rn.node.ID,
					To:          req.From,
					Term:        rn.GetCurrentTerm(),
					VoteGranted: false,
				}
			}

			if req.Term > rn.GetCurrentTerm() {
				rn.SetCurrentTerm(req.Term)
				rn.setVotedFor(None)
			}
			if rn.GetVotedFor() == None || rn.GetVotedFor() == req.From {
				rn.setVotedFor(req.From)
				replyChan <- pb.RequestVoteReply{
					From:        rn.node.ID,
					To:          req.From,
					Term:        rn.GetCurrentTerm(),
					VoteGranted: true,
				}
			}
		case appendEntries := <-rn.appendEntriesC:
			t.Reset(rn.electionTimeout)
			req := appendEntries.request
			replyChan := appendEntries.reply
			if req.Term < rn.GetCurrentTerm() {
				replyChan <- pb.AppendEntriesReply{
					From:    rn.node.ID,
					To:      req.From,
					Term:    rn.GetCurrentTerm(),
					Success: false,
				}
				continue
			}
			if req.Term > rn.GetCurrentTerm() {
				rn.SetCurrentTerm(req.Term)
			}
			if l := rn.GetLog(req.PrevLogIndex); l == nil || l.Term != req.PrevLogTerm {
				replyChan <- pb.AppendEntriesReply{
					From:    rn.node.ID,
					To:      req.From,
					Term:    rn.GetCurrentTerm(),
					Success: false,
				}
				continue
			}

			startIdx := 0
			for _, entry := range req.Entries {
				idx, term := entry.Index, entry.Term
				if curTerm := rn.GetLog(idx).Term; curTerm != term {
					rn.TruncateLog(idx)
					startIdx = int(idx)
					break
				}
			}

			//if l := rn.GetLog(req.PrevLogIndex + 1); l != nil && l.Term != req.Term {
			//	rn.TruncateLog(req.PrevLogIndex + 1)
			//}

			for _, it := range req.Entries[startIdx:] {
				rn.StoreLog(it)
				rn.lastApplied++
			}
			// If leaderCommit > commitIndex, set commitIndex =
			// min(leaderCommit, index of last new entry)
			if req.LeaderCommit > rn.commitIndex {
				if req.LeaderCommit <= rn.LastLogIndex() {
					rn.commitIndex = req.LeaderCommit
				} else {
					rn.commitIndex = rn.LastLogIndex()
				}
			}
			rn.leader = req.From
			replyChan <- pb.AppendEntriesReply{
				From:    rn.node.ID,
				To:      req.From,
				Term:    rn.GetCurrentTerm(),
				Success: true,
			}
		case msg, ok := <-rn.proposeC:
			if !ok {
				return nil
			}
			conn := rn.node.PeerConns[rn.leader]
			leader := pb.NewRaftRPCClient(conn)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			_, err := leader.Propose(ctx, &pb.ProposalRequest{
				From: rn.node.ID,
				To:   rn.leader,
				Data: msg,
			})
			if err != nil {
				rn.log.Printf("propose forwarding error: %v", err)
			}
		}
	}
	return nil
}
