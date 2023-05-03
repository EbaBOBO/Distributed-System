package raft

import (
	"context"
	"encoding/json"
	"math/rand"
	pb "modist/proto"
	"time"
)

// doFollower implements the logic for a Raft node in the follower state.
func (rn *RaftNode) doFollower() stateFunction {
	rn.state = FollowerState
	rn.log.Printf("+++++++++++++++++++++++++transitioning to %s state at term %d+++++++++++++++++++++++++", rn.state, rn.GetCurrentTerm())
	rn.setVotedFor(None)
	// TODO(students): [Raft] Implement me!
	// Hint: perform any initial work, and then consider what a node in the
	// follower state should do when it receives an incoming message on every
	// possible channel.
	timeout := time.Duration(float64(rn.electionTimeout) * (1 + rand.Float64()))
	rn.log.Printf("follower start election timeout, timeout %v", timeout)
	if !((timeout >= rn.electionTimeout) && (timeout <= rn.electionTimeout*2)) {
		panic("timeout is out of range")
	}
	t := time.NewTicker(timeout)

	for {
		select {
		case <-t.C:
			// If election timeout elapses without receiving AppendEntries
			// RPC from current leader or granting vote to candidate: convert to candidate
			rn.log.Printf("election timeout, start new election")
			return rn.doCandidate
		case msg := <-rn.requestVoteC:
			reply := handleRequestVote(rn, msg.request)
			msg.reply <- reply
			if reply.VoteGranted {
				t.Stop()
				t.Reset(timeout)
			}
		case msg := <-rn.appendEntriesC:
			// t.Reset(timeout)
			reply := handleAppendEntries(rn, msg.request)
			msg.reply <- reply
			if msg.request.Term >= rn.GetCurrentTerm() {
				t.Stop()
				t.Reset(timeout)
			}
			if msg.request.Term > rn.GetCurrentTerm() {
				rn.SetCurrentTerm(msg.request.Term)
				rn.leader = msg.request.From
				return rn.doFollower
			}
		case msg, ok := <-rn.proposeC:
			if !ok {
				rn.Stop()
				close(rn.stopC)
				return nil
			}
			if rn.leader == 0 {
				rn.log.Printf("No leader, discard proposal")
				continue
			}
			kv := RaftKVPair{}
			json.Unmarshal(msg, &kv)
			rn.log.Printf("follower received proposal, forward to %v: %v", kv, rn.leader)
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
				rn.log.Printf("proposal forwarding error: %v", err)
			}
		default:
			for rn.commitIndex > rn.lastApplied {
				rn.lastApplied++
				if rn.GetLog(rn.lastApplied) == nil || rn.GetLog(rn.lastApplied).Data == nil {
					continue
				}
				rn.commitC <- (*commit)(&rn.GetLog(rn.lastApplied).Data)
			}
		}
	}
}
