package raft

import "time"

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//rule 1 ------------
	if args.Term < rf.currentTerm {
		DPrintf("[ElectionReject++++++]Server %d reject %d, MYterm %d, candidate term %d", rf.me, args.CandidateId, rf.currentTerm, args.Term)
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	reply.Term = rf.currentTerm

	if args.Term > rf.currentTerm {
		DPrintf("[ElectionToFollower++++++]Server %d(term %d) into follower,candidate %d(term %d) ", rf.me, rf.currentTerm, args.CandidateId, args.Term)
		rf.currentTerm = args.Term
		rf.ChangeStatus(FOLLOWER)
		rf.persist()
	}

	//rule 2 ------------
	if !rf.UpToDate(args.LastLogIndex, args.LastLogTerm) {
		DPrintf("[ElectionReject+++++++]Server %d reject %d, UpToDate", rf.me, args.CandidateId)
		//rf.printLogsForDebug()
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	// arg.Term == rf.currentTerm
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId && args.Term == reply.Term {
		DPrintf("[ElectionReject+++++++]Server %d reject %d, Have voter for %d", rf.me, args.CandidateId, rf.votedFor)
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	} else {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		rf.timeoutTimer = time.NewTimer(RandomTimeBetween(TIMEOUT_LOWER_BOUND, TIMEOUT_UPPER_BOUND))
		rf.persist()
		DPrintf("[ElectionSUCCESS+++++++]Server %d voted for %d!", rf.me, args.CandidateId)
		return
	}

	return

}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term             int
	Success          bool
	ConflictingIndex int
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[GetHeartBeat]Sever %d, from Leader %d(term %d), mylastindex %d, leader.preindex %d, entries: %v", rf.me, args.LeaderId, args.Term, rf.getLastIndex(), args.PrevLogIndex, args.Entries)

	// rule 1
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.ConflictingIndex = -1
		return
	}

	rf.currentTerm = args.Term
	// rf.leaderId = args.LeaderId
	reply.Term = rf.currentTerm
	reply.Success = true
	reply.ConflictingIndex = -1

	if rf.status != FOLLOWER {
		rf.ChangeStatus(FOLLOWER)
	} else {
		rf.timeoutTimer = time.NewTimer(RandomTimeBetween(TIMEOUT_LOWER_BOUND, TIMEOUT_UPPER_BOUND))
		rf.persist()
	}

	//// confilict
	//// for my snapshot until 15,and len 3, you give me 10 - 20,I give you 16
	//if rf.lastSSPointIndex > args.PrevLogIndex {
	//	reply.Success = false
	//	reply.ConflictingIndex = rf.getLastIndex() + 1
	//	return
	//}

	if rf.getLastIndex() < args.PrevLogIndex {
		reply.Success = false
		reply.ConflictingIndex = rf.getLastIndex()
		DPrintf("[AppendEntries ERROR1]Sever %d ,prevLogIndex %d,Term %d, rf.getLastIndex %d, rf.getLastTerm %d, Confilicing %d", rf.me, args.PrevLogIndex, args.PrevLogTerm, rf.getLastIndex(), rf.getLastTerm(), reply.ConflictingIndex)
		return
	} else {
		if rf.getLogTermWithIndex(args.PrevLogIndex) != args.PrevLogTerm {
			reply.Success = false
			tempTerm := rf.getLogTermWithIndex(args.PrevLogIndex)
			for index := args.PrevLogIndex; index >= rf.lastSSPointIndex; index-- {
				if rf.getLogTermWithIndex(index) != tempTerm {
					reply.ConflictingIndex = index + 1
					DPrintf("[AppendEntries ERROR2]Sever %d ,prevLogIndex %d,Term %d, rf.getLastIndex %d, rf.getLastTerm %d, Confilicing %d", rf.me, args.PrevLogIndex, args.PrevLogTerm, rf.getLastIndex(), rf.getLastTerm(), reply.ConflictingIndex)
					break
				}
			}
			return
		}
	}

	//rule 3 & rule 4
	rf.log = append(rf.log[:args.PrevLogIndex+1-rf.lastSSPointIndex], args.Entries...)
	rf.persist()
	//if len(args.Entries) != 0{
	//	rf.printLogsForDebug()
	//}

	//rule 5
	if args.LeaderCommit > rf.commitIndex {
		rf.updateCommitIndex(FOLLOWER, args.LeaderCommit)
	}
	DPrintf("[FinishHeartBeat]Server %d, from leader %d(term %d), me.lastIndex %d", rf.me, args.LeaderId, args.Term, rf.getLastIndex())
	return
}
