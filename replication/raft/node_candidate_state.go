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
	// Increment currentTrem
	rn.SetCurrentTerm(rn.GetCurrentTerm() + 1)
	nodeCurrentTerm := rn.GetCurrentTerm()
	rn.log.Printf("+++++++++++++++++++++++++transitioning to %s state at term %d+++++++++++++++++++++++++", rn.state, nodeCurrentTerm)

	// TODO(students): [Raft] Implement me!
	// Hint: perform any initial work, and then consider what a node in the
	// candidate state should do when it receives an incoming message on every
	// possible channel.

	// Vote for self
	rn.setVotedFor(rn.node.ID)
	// Set election timer
	timeout := time.Duration(float64(rn.electionTimeout) * (1 + rand.Float64()))
	if !((timeout >= rn.electionTimeout) && (timeout <= rn.electionTimeout*2)) {
		panic("timeout is out of range")
	}
	t := time.NewTicker(timeout)
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
				Term:         nodeCurrentTerm,
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
	rn.log.Print(nodeCurrentTerm)
	for {
		select {
		case <-t.C:
			// If election timeout elapses: start new election
			rn.log.Printf("Node %v: election timeout", rn.node.ID)
			return rn.doCandidate
		case reply := <-replyChan:
			// If votes received from majority of servers: become leader
			rn.log.Printf("Candidate %v: received reply from %v %v", rn.node.ID, reply.From, reply.VoteGranted)
			if reply.Term > nodeCurrentTerm {
				rn.SetCurrentTerm(reply.Term)
				return rn.doFollower
			}
			if reply.VoteGranted {
				votesCnt++
			}
			if votesCnt >= votesToWin {
				return rn.doLeader
			}
		case msg := <-rn.requestVoteC:
			reply := handleRequestVote(rn, nodeCurrentTerm, msg.request)
			msg.reply <- reply
			rn.log.Printf("requestVote term: %v, current term: %v", msg.request.Term, nodeCurrentTerm)
			if msg.request.Term > nodeCurrentTerm {
				rn.SetCurrentTerm(msg.request.Term)
				return rn.doFollower
			}
		case msg := <-rn.appendEntriesC:
			reply := handleAppendEntries(rn, nodeCurrentTerm, msg.request)
			msg.reply <- reply
			rn.log.Printf("appendEntries term: %v, current term: %v", msg.request.Term, nodeCurrentTerm)
			if msg.request.Term >= nodeCurrentTerm {
				rn.log.Printf("Change to follower state")
				rn.SetCurrentTerm(msg.request.Term)
				return rn.doFollower
			}
		case _, ok := <-rn.proposeC:
			// Stop
			if !ok {
				rn.Stop()
				return nil
			}
		default:
			if rn.commitIndex > rn.lastApplied {
				rn.lastApplied++
				rn.commitC <- (*commit)(&rn.GetLog(rn.lastApplied).Data)
			}
		}
	}
}
