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
	nodeCurrentTerm := rn.GetCurrentTerm()
	rn.log.Printf("+++++++++++++++++++++++++transitioning to %s state at term %d+++++++++++++++++++++++++", rn.state, nodeCurrentTerm)

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
			rn.log.Printf("election timeout, start new election")
			return rn.doCandidate
		case msg := <-rn.requestVoteC:
			t.Reset(timeout)
			reply := handleRequestVote(rn, nodeCurrentTerm, msg.request)
			msg.reply <- reply
			if msg.request.Term > nodeCurrentTerm {
				rn.SetCurrentTerm(msg.request.Term)
				return rn.doFollower
			}
		case msg := <-rn.appendEntriesC:
			t.Reset(timeout)
			reply := handleAppendEntries(rn, nodeCurrentTerm, msg.request)
			msg.reply <- reply
			if msg.request.Entries != nil && len(msg.request.Entries[0].Data) > 0 {
				kv := RaftKVPair{}
				json.Unmarshal(msg.request.Entries[0].Data, &kv)
				rn.log.Printf("follower received appendEntries %v, reply %v", kv, reply.Success)
			}
			if msg.request.Term > nodeCurrentTerm {
				rn.SetCurrentTerm(msg.request.Term)
				rn.leader = msg.request.From
				return rn.doFollower
			}
		case msg, ok := <-rn.proposeC:
			if !ok {
				return nil
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
			if rn.commitIndex > rn.lastApplied {
				rn.lastApplied++
				rn.commitC <- (*commit)(&rn.GetLog(rn.lastApplied).Data)
			}
		}
	}
}
