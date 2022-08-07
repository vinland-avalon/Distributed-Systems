package raft

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	term         int
	candidateId  int
	lastLogIndex int
	lastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	term        int
	voteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[%v] receives RequestVote from %v, args:%+v, reply:%+v", rf.me, args.candidateId, args, reply)
	if rf.currentTerm < args.term {
		rf.currentTerm = args.term
		rf.UpdateCurrentTerm(args.term)
	}

	if args.term < rf.currentTerm {
		reply.voteGranted = false
	} else if args.lastLogIndex >= len(rf.log)-1 && (rf.votedFor == nil || rf.votedFor == rf.peers[args.candidateId]) {
		reply.voteGranted = true
		rf.currentTerm = args.term
		rf.votedFor = rf.peers[args.candidateId]
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
	term         int
	leaderId     int
	prevLogIndex int
	prevLogTerm  int
	entries      []ApplyMsg
	leaderCommit int
}

type AppendEntriesReply struct {
	term    int
	success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[%v] receives AppendEntries, args:%+v, reply:%+v", rf.me, args, reply)
	reply.term = rf.currentTerm
	if rf.currentTerm < args.term {
		rf.currentTerm = args.term
		rf.UpdateCurrentTerm(args.term)
	}

	if rf.currentTerm > args.term {
		reply.success = false
		return
	} else {
		if !rf.containsPrevInfo(args.prevLogTerm, args.prevLogIndex) {
			reply.success = false
		} else {
			if rf.conflictWithPrevInfo(args.prevLogTerm, args.prevLogIndex) {
				rf.deleteWithPrevInfo(args.prevLogTerm, args.prevLogIndex)
			}
			rf.AppendEntriesFromPrevPos(args.prevLogIndex)
			reply.success = true
		}
	}

	if args.leaderCommit > rf.commitIndex {
		rf.commitIndex = Min(args.leaderCommit, args.prevLogIndex+len(args.entries))
		rf.CheckAndTryApply()
	}

	rf.timeoutTimer.Reset(RandomTimeBetween(TIMEOUT_LOWER_BOUND, TIMEOUT_UPPER_BOUND))
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	DPrintf("[%v] sendAppendEntries to [%v], args: %+v", rf.me, server, *args)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	if args.term == rf.currentTerm && rf.status == LEADER {
		if reply.term > rf.currentTerm {
			DPrintf("[%v] in term %v, find higher term of %v from [%v] than its current term %v, step down to follower", rf.me, args.term, reply.term, server, rf.currentTerm)
			rf.UpdateCurrentTerm(reply.term)
		}
	}
	// other operations such as fix up
	rf.mu.Unlock()
	return ok
}

// tmp functions needed to check again
