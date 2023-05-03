package raft

import (
	"encoding/json"
	pb "modist/proto"
	"time"
)

// doLeader implements the logic for a Raft node in the leader state.
func (rn *RaftNode) doLeader() stateFunction {
	rn.log.Printf("+++++++++++++++++++++++++transitioning to leader state at term %d+++++++++++++++++++++++++", rn.GetCurrentTerm())
	rn.state = LeaderState

	// TODO(students): [Raft] Implement me!
	// Hint: perform any initial work, and then consider what a node in the
	// leader state should do when it receives an incoming message on every
	// possible channel.

	rn.leader = rn.node.ID
	// (Reinitialized after election)
	// nextIndex[]
	// for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	// matchIndex[]
	// for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	rn.leaderMu.Lock()
	for k := range rn.node.PeerNodes {
		if k == rn.node.ID {
			continue
		}
		rn.matchIndex[k] = 0
		rn.nextIndex[k] = rn.LastLogIndex() + 1
	}
	// rn.leaderMu.Unlock()
	rn.StoreLog(&pb.LogEntry{
		Term:  rn.GetCurrentTerm(),
		Data:  nil,
		Type:  pb.EntryType_NORMAL,
		Index: rn.LastLogIndex() + 1,
	})
	rn.leaderMu.Unlock()
	// send initial empty AppendEntries RPCs (heartbeat) to each server
	higherTermChan := make(chan uint64, 1)
	// initial heartbeat
	sendAppendEntries(rn, true, higherTermChan)

	t := time.NewTicker(rn.heartbeatTimeout)

	for {
		select {
		case <-t.C:
			rn.log.Printf("commitIdx %v", rn.commitIndex)
			rn.log.Printf("leader sending heartbeat, commitIdx %v", rn.commitIndex)
			sendAppendEntries(rn, false, higherTermChan)
			t.Stop()
			t.Reset(rn.heartbeatTimeout)
		// If command received from client: append entry to local log,
		// respond after entry applied to state machine
		case msg, ok := <-rn.proposeC:
			if !ok {
				rn.log.Printf("Stop")
				rn.Stop()
				close(rn.stopC)
				return nil
			}
			rkv := RaftKVPair{}
			json.Unmarshal(msg, &rkv)
			rn.log.Printf("leader received proposal: %v", rkv)
			rn.log.Printf("commitIdx %v", rn.commitIndex)
			rn.log.Printf("nextIdx %v", rn.nextIndex)
			rn.log.Printf("matchIdx %v", rn.matchIndex)
			entry := pb.LogEntry{
				Index: rn.LastLogIndex() + 1,
				Term:  rn.GetCurrentTerm(),
				Type:  pb.EntryType_NORMAL,
				Data:  msg,
			}
			rn.StoreLog(&entry)
			rn.log.Printf("commitIdx %v", rn.commitIndex)
			rn.log.Printf("leader sending heartbeat, commitIdx %v", rn.commitIndex)
			// repeat during idle periods to prevent election timeouts
			sendAppendEntries(rn, false, higherTermChan)
			t.Stop()
			t.Reset(rn.heartbeatTimeout)
		case msg := <-rn.requestVoteC:
			rn.log.Printf("nextIdx %v", rn.nextIndex)
			rn.log.Printf("matchIdx %v", rn.matchIndex)
			rn.log.Printf("commitIdx %v", rn.commitIndex)
			rn.log.Printf("leader term %v received requestVote: %v", rn.GetCurrentTerm(), msg.request)
			reply := handleRequestVote(rn, msg.request)
			msg.reply <- reply
			rn.log.Printf("leader term %v received requestVote: %v", rn.GetCurrentTerm(), &reply)
			if reply.VoteGranted {
				rn.SetCurrentTerm(msg.request.Term)
				return rn.doFollower
			}

		case msg := <-rn.appendEntriesC:
			rn.log.Printf("leader term %v received AppendEntries: %v", rn.GetCurrentTerm(), msg.request)
			reply := handleAppendEntries(rn, msg.request)
			msg.reply <- reply
			if msg.request.Term > rn.GetCurrentTerm() {
				rn.leader = msg.request.From
				higherTermChan <- msg.request.Term
			}
		case msg := <-higherTermChan:
			rn.log.Printf("leader term %v received AppendEntries reply with higher term:%v", rn.GetCurrentTerm(), msg)
			rn.SetCurrentTerm(msg)
			return rn.doFollower
		}

	}
}
