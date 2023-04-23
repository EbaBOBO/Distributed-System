package raft

import (
	"context"
	"math/rand"
	pb "modist/proto"
	"time"

	"google.golang.org/grpc"
)

// doCandidate implements the logic for a Raft node in the candidate state.
func (rn *RaftNode) doCandidate() stateFunction {
	rn.state = CandidateState
	rn.log.Printf("transitioning to %s state at term %d", rn.state, rn.GetCurrentTerm())

	// TODO(students): [Raft] Implement me!
	// Hint: perform any initial work, and then consider what a node in the
	// candidate state should do when it receives an incoming message on every
	// possible channel.

	// Increment currentTrem
	rn.SetCurrentTerm(rn.GetCurrentTerm() + 1)
	// Vote for self
	rn.setVotedFor(rn.node.ID)
	// Set election timer
	t := time.NewTicker(time.Duration(1+rand.Float64()) * rn.electionTimeout)
	// Send RequestVoteRPCs to all other servers
	replyChan := make(chan *pb.RequestVoteReply)
	for k, v := range rn.node.PeerConns {
		if k == rn.node.ID {
			continue
		}
		go func(nodeId uint64, conn *grpc.ClientConn) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			remoteNode := pb.NewRaftRPCClient(conn)
			msgReq := &pb.RequestVoteRequest{
				From:         rn.node.ID,
				To:           nodeId,
				Term:         rn.GetCurrentTerm(),
				LastLogIndex: rn.LastLogIndex(),
			}
			reply, err := remoteNode.RequestVote(ctx, msgReq)
			if err != nil {
				rn.log.Printf("RequestVote RPC failed: %v", err)
				return
			}
			replyChan <- reply
		}(k, v)
	}
	votesToWin := int(len(rn.node.PeerConns) / 2)
	votesCnt := 0
	for {
		if rn.commitIndex > rn.lastApplied {
			rn.lastApplied++
			rn.commitC <- (*commit)(&rn.GetLog(rn.lastApplied).Data)
		}
		select {
		case <-t.C:
			// Election timeout
			rn.log.Printf("election timeout")
			return rn.doCandidate
		case reply := <-replyChan:
			if reply.Term > rn.GetCurrentTerm() {
				rn.SetCurrentTerm(reply.Term)
				return rn.doFollower
			}
			if reply.VoteGranted {
				votesCnt++
			}
			if votesCnt >= votesToWin {
				return rn.doLeader
			}
		case appendEntries := <-rn.appendEntriesC:
			// Got AppendEntries RPC
			// Become follower
			req := appendEntries.request
			replyChan := appendEntries.reply
			if req.Term < rn.GetCurrentTerm() {
				replyChan <- pb.AppendEntriesReply{
					From:    rn.node.ID,
					To:      req.From,
					Term:    rn.GetCurrentTerm(),
					Success: false,
				}
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
			}
			if l := rn.GetLog(req.PrevLogIndex + 1); l != nil && l.Term != req.Term {
				rn.TruncateLog(req.PrevLogIndex + 1)
			}
			for _, it := range req.Entries {
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
			replyChan <- pb.AppendEntriesReply{
				From:    rn.node.ID,
				To:      req.From,
				Term:    rn.GetCurrentTerm(),
				Success: true,
			}
			return rn.doFollower
		case _, ok := <-rn.proposeC:
			// Stop
			if !ok {
				rn.Stop()
				return nil
			}
		}
	}
}
