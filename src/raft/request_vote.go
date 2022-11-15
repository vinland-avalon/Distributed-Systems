package raft

import (
	"sync"
)

func (rf *Raft) IssueElection(electionTerm int) {
	mu := sync.Mutex{}
	cond := sync.NewCond(&mu)
	count := 1
	finished := 1
	//if len(rf.log) != 0 {
	//	lastLogTerm = rf.termOfLog[len(rf.log)-1]
	//}
	args := &RequestVoteArgs{
		Term:         electionTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastIndex(),
		LastLogTerm:  rf.getLastTerm(),
	}
	id := GenerateId()
	DPrintf("[%v], in Term %v, issues an election and broadcasts RequestVoteArgs(%v): %+v", rf.me, electionTerm, id, *args)
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(index, id int) {
			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(index, args, reply)
			mu.Lock()
			defer mu.Unlock()
			if ok != true {
				DPrintf("[%v] send RequestVotes(%v) to [%v] fails, args: %+v", rf.me, id, index, args)
			} else {
				DPrintf("[%v] receives RequestVoteReply(%v) {%+v} from [%v] in Term %v", rf.me, id, *reply, index, electionTerm)
				if args.Term == rf.currentTerm && rf.status == CANDIDATE {
					if reply.Term > rf.currentTerm {
						DPrintf("[%v]'s RequestVote in Term %v finds a new leader [%v] with Term %v, step into follower", rf.me, args.Term, index, reply.Term)
						rf.currentTerm = reply.Term
						rf.ChangeStatus(FOLLOWER)
					} else if reply.VoteGranted == true {
						count++
					}
				} else {
					// rf.ChangeStatus(FOLLOWER)
					finished = len(rf.peers)
					cond.Broadcast()
					return
				}
			}
			finished++
			cond.Broadcast()
		}(i, id)
	}
	mu.Lock()
	for count <= len(rf.peers)/2 && finished != len(rf.peers) {
		cond.Wait()
	}
	DPrintf("[%v] election result: count:%v, finished:%v, len(rf.peers)/2 :%v, electionTerm: %v, rf.currentTerm:%v", rf.me, count, finished, len(rf.peers)/2, electionTerm, rf.currentTerm)
	if rf.status == CANDIDATE && count > len(rf.peers)/2 && electionTerm == rf.currentTerm {
		DPrintf("[%v] receives majority votes in Term %v and becomes leader", rf.me, electionTerm)
		rf.ChangeStatus(LEADER)
	}
	mu.Unlock()
}
