package raft

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

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[%v] receives RequestVote from %v, args:%+v, reply:%+v", rf.me, args.CandidateId, args, reply)
	if rf.currentTerm < args.Term {
		// rf.currentTerm = args.Term
		rf.UpdateCurrentTerm(args.Term)
	}
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
	} else if args.LastLogIndex >= len(rf.log)-1 && (rf.votedFor == nil || rf.votedFor == rf.peers[args.CandidateId]) {
		reply.VoteGranted = true
		rf.votedFor = rf.peers[args.CandidateId]
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []ApplyMsg
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer func() {
		rf.timeoutTimer.Reset(RandomTimeBetween(TIMEOUT_LOWER_BOUND, TIMEOUT_UPPER_BOUND))
		rf.mu.Unlock()
	}()
	DPrintf("[%v] receives AppendEntriesArgs, args:%+v, reply:%+v", rf.me, args, reply)
	reply.Term = rf.currentTerm
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.UpdateCurrentTerm(args.Term)
	}

	if rf.currentTerm > args.Term {
		DPrintf("[%v] receives AppendEntries from [%v], but refused", rf.me, args.LeaderId)
		reply.Success = false
		// reply.Term = rf.currentTerm
		return
	} else {
		rf.status = FOLLOWER
		if !rf.containsPrevInfo(args.PrevLogTerm, args.PrevLogIndex) {
			reply.Success = false
		} else {
			if rf.conflictWithPrevInfo(args.PrevLogTerm, args.PrevLogIndex) {
				rf.deleteWithPrevInfo(args.PrevLogTerm, args.PrevLogIndex)
			}
			rf.AppendEntriesFromPrevPos(args.PrevLogIndex)
			reply.Success = true
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = Min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
		rf.CheckAndTryApply()
	}
	//DPrintf("++++++++++++++++++++++++++++++")
	//if !rf.time.Stop() && len(rf.heartbeatTimer.C) > 0 {
	//	<-rf.heartbeatTimer.C
	//}
	rf.timeoutTimer.Reset(RandomTimeBetween(TIMEOUT_LOWER_BOUND, TIMEOUT_UPPER_BOUND))
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	id := GenerateId()
	DPrintf("[%v] send AppendEntries (%v) to [%v], args: %+v", rf.me, id, server, *args)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	if ok != true {
		DPrintf("[%v] send AppendEntries (%v) to [%v] fails, args: %+v", rf.me, id, server, args)
	}
	DPrintf("[%v] get AppendEntriesReply (%v) from [%v], args: %+v, reply: %+v", rf.me, id, server, args, reply)
	if args.Term == rf.currentTerm && rf.status == LEADER {
		if reply.Term > rf.currentTerm {
			DPrintf("[%v] in term %v, find higher term of %v from [%v] than its current term %v, step down to follower", rf.me, args.Term, reply.Term, server, rf.currentTerm)
			rf.UpdateCurrentTerm(reply.Term)
		}
	}
	// other operations such as fix up
	rf.mu.Unlock()
	return ok
}

// tmp functions needed to check again
