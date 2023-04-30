package raft

import (
	"context"
	"math/rand"
	pb "modist/proto"
	"time"
)

// doFollower implements the logic for a Raft node in the follower state.
func (rn *RaftNode) doFollower() stateFunction {
	rn.state = FollowerState
	rn.log.Printf("+++++++++++++++++++++++++transitioning to %s state at term %d+++++++++++++++++++++++++", rn.state, rn.GetCurrentTerm())

	// TODO(students): [Raft] Implement me!
	// Hint: perform any initial work, and then consider what a node in the
	// follower state should do when it receives an incoming message on every
	// possible channel.
	timeout := time.Duration(1+rand.Float64()) * rn.electionTimeout
	if !((timeout >= rn.electionTimeout) && (timeout <= rn.electionTimeout*2)) {
		panic("timeout is out of range")
	}
	t := time.NewTicker(timeout)
	rn.setVotedFor(None)
	for {
		select {
		case <-t.C:
			// If election timeout elapses without receiving AppendEntries
			// RPC from current leader or granting vote to candidate: convert to candidate
			return rn.doCandidate
		case msg := <-rn.requestVoteC:
			t.Reset(rn.electionTimeout)
			reply := handleRequestVote(rn, msg.request)
			msg.reply <- reply
			if msg.request.Term > rn.GetCurrentTerm() {
				rn.SetCurrentTerm(msg.request.Term)
				return rn.doFollower
			}
		case msg := <-rn.appendEntriesC:
			t.Reset(rn.electionTimeout)
			reply := handleAppendEntries(rn, msg.request)
			msg.reply <- reply
			if msg.request.Term > rn.GetCurrentTerm() {
				rn.SetCurrentTerm(msg.request.Term)
				rn.leader = msg.request.From
				return rn.doFollower
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
		default:
			if rn.commitIndex > rn.lastApplied {
				rn.lastApplied++
				rn.commitC <- (*commit)(&rn.GetLog(rn.lastApplied).Data)
			}
		}
	}
}
