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
	rn.log.Printf("+++++++++++++++++++++++++transitioning to %s state at term %d+++++++++++++++++++++++++", rn.state, rn.GetCurrentTerm())

	// TODO(students): [Raft] Implement me!
	// Hint: perform any initial work, and then consider what a node in the
	// candidate state should do when it receives an incoming message on every
	// possible channel.

	// Vote for self
	rn.setVotedFor(rn.node.ID)
	// Set election timer
	if len(rn.node.PeerConns) == 1 {
		return rn.doLeader
	}
	timeout := time.Duration(float64(rn.electionTimeout) * (1 + rand.Float64()))
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
				Term:         rn.GetCurrentTerm(),
				LastLogIndex: rn.LastLogIndex(),
				LastLogTerm:  rn.GetLog(rn.LastLogIndex()).Term,
			}
			reply, err := remoteNode.RequestVote(ctx, msgReq)
			if err == nil {
				replyChan <- reply
			}

		}(k, v)
	}
	majority := int(len(rn.node.PeerNodes)/2) + 1
	votesToWin := majority - 1
	votesToLose := majority
	voteGrantedCnt := 0
	voteRejectedCnt := 0
	rn.log.Print(rn.GetCurrentTerm())
	for {
		select {
		case <-t.C:
			// If election timeout elapses: start new election
			rn.log.Printf("Node %v: election timeout", rn.node.ID)
			return rn.doCandidate
		case reply := <-replyChan:
			// If votes received from majority of servers: become leader
			rn.log.Printf("Candidate %v: received reply from %v %v", rn.node.ID, reply.From, reply.VoteGranted)
			if reply.Term > rn.GetCurrentTerm() {
				rn.SetCurrentTerm(reply.Term)
				rn.setVotedFor(None)
				return rn.doFollower
			}
			if reply.VoteGranted {
				voteGrantedCnt++
			} else {
				voteRejectedCnt++
			}
			if voteGrantedCnt >= votesToWin {
				return rn.doLeader
			}
			if voteRejectedCnt >= votesToLose {
				rn.setVotedFor(None)
				return rn.doFollower
			}
		case msg := <-rn.requestVoteC:
			reply := handleRequestVote(rn, msg.request)
			msg.reply <- reply
			rn.log.Printf("requestVote term: %v, current term: %v", msg.request.Term, rn.GetCurrentTerm())
			if reply.VoteGranted {
				rn.SetCurrentTerm(msg.request.Term)
				rn.setVotedFor(None)
				return rn.doFollower
			}
		case msg := <-rn.appendEntriesC:
			reply := handleAppendEntries(rn, msg.request)
			msg.reply <- reply
			rn.log.Printf("appendEntries term: %v, current term: %v", msg.request.Term, rn.GetCurrentTerm())
			if msg.request.Term >= rn.GetCurrentTerm() {
				rn.SetCurrentTerm(msg.request.Term)
				rn.setVotedFor(None)
				rn.leader = msg.request.From
				return rn.doFollower
			}
		case _, ok := <-rn.proposeC:
			// Stop
			if !ok {
				rn.log.Printf("Stop")
				rn.Stop()
				close(rn.stopC)
				return nil
			}
		default:
			handleCommit(rn)
		}
	}
}
