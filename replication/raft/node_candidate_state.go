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
	rn.log.Printf("+++++++++++++++++++++++++transitioning to %s state at term %d+++++++++++++++++++++++++", rn.state, rn.GetCurrentTerm())

	// TODO(students): [Raft] Implement me!
	// Hint: perform any initial work, and then consider what a node in the
	// candidate state should do when it receives an incoming message on every
	// possible channel.

	// Increment currentTrem
	rn.SetCurrentTerm(rn.GetCurrentTerm() + 1)
	// Vote for self
	rn.setVotedFor(rn.node.ID)
	// Set election timer
	timeout := time.Duration(1+rand.Float64()) * rn.electionTimeout
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
		select {
		case <-t.C:
			// If election timeout elapses: start new election
			rn.log.Printf("Node %v: election timeout", rn.node.ID)
			return rn.doCandidate
		case reply := <-replyChan:
			// If votes received from majority of servers: become leader
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
		case msg := <-rn.requestVoteC:
			reply := handleRequestVote(rn, msg.request)
			msg.reply <- reply
			if msg.request.Term > rn.GetCurrentTerm() {
				rn.SetCurrentTerm(msg.request.Term)
				return rn.doFollower
			}
		case msg := <-rn.appendEntriesC:
			reply := handleAppendEntries(rn, msg.request)
			msg.reply <- reply
			if msg.request.Term > rn.GetCurrentTerm() {
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
