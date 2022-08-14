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
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term               int
	Success            bool
	ConflictTerm       int
	ConflictFirstIndex int
}

//func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
//	// Your code here (2A, 2B).
//	rf.mu.Lock()
//
//	defer rf.persist()
//	defer rf.mu.Unlock()
//
//	DPrintf("[%v] as state of %v, before receiving AppendEntry with args {%+v}, its status is currentTerm %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v", rf.me, rf.status, args, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.GetFirstLog(), rf.GetLastLog())
//	reply.Term = rf.currentTerm
//	if rf.currentTerm > args.Term {
//		DPrintf("[%v] receives AppendEntries from [%v], but refused", rf.me, args.LeaderId)
//		reply.Success = false
//		// reply.Term = rf.currentTerm
//		return
//	}
//
//	if rf.currentTerm < args.Term {
//		rf.UpdateCurrentTerm(args.Term)
//	}
//
//	rf.ChangeStatus(FOLLOWER)
//	rf.timeoutTimer.Reset(RandomTimeBetween(TIMEOUT_LOWER_BOUND, TIMEOUT_UPPER_BOUND))
//
//	if !rf.containsPrevInfo(args.PrevLogTerm, args.PrevLogIndex) {
//		reply.Success = false
//	} else {
//		if rf.conflictWithPrevInfo(args.PrevLogTerm, args.PrevLogIndex) {
//			rf.deleteWithPrevInfo(args.PrevLogTerm, args.PrevLogIndex)
//		}
//		rf.AppendEntriesFromPrevPos(args.PrevLogIndex)
//		reply.Success = true
//	}
//
//	if args.LeaderCommit > rf.commitIndex {
//		rf.commitIndex = Min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
//		rf.CheckAndTryApply()
//	}
//	//DPrintf("++++++++++++++++++++++++++++++")
//	//if !rf.time.Stop() && len(rf.heartbeatTimer.C) > 0 {
//	//	<-rf.heartbeatTimer.C
//	//}
//	rf.timeoutTimer.Reset(RandomTimeBetween(TIMEOUT_LOWER_BOUND, TIMEOUT_UPPER_BOUND))
//}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Reply false if term < currentTerm, otherwise continue a "consistency check"
	if rf.currentTerm <= args.Term {

		// If RPC request or response contains term T > currentTerm:
		// set currentTerm = T, convert to follower
		if rf.currentTerm < args.Term {

			DPrintf("[AppendEntries]: Id %d Term %d State %v\t||\targs's term %d is newer\n",
				rf.me, rf.currentTerm, rf.status, args.Term)
			rf.UpdateCurrentTerm(args.Term)
		}

		// if the consistency check pass
		if len(rf.log) > args.PrevLogIndex &&
			rf.log[args.PrevLogIndex].TermOfEntry == args.PrevLogTerm {

			// 收到AppendEntries RPC(包括心跳)，说明存在leader，自己切换为follower状态
			rf.ChangeStatus(FOLLOWER)

			// **If** an existing entry conflicts with a new one(same index but
			// different terms), delete the existing entry and all that follow it.
			// 这里的If至关重要。如果follower拥有领导者的日志条目，则follower一定不能(MUST NOT)
			// 截断其日志。leader发送的条目之后的任何内容(any elements of following the entries
			// send by the leader)必须(MUST)保留。

			// 1. 判断follower中log是否已经拥有args.Entries的所有条目，全部有则匹配！
			isMatch := true
			nextIndex := args.PrevLogIndex + 1
			end := len(rf.log) - 1
			for i := 0; isMatch && i < len(args.Entries); i++ {
				// 如果args.Entries还有元素，而log已经达到结尾，则不匹配
				if end < nextIndex+i {
					isMatch = false
				} else if rf.log[nextIndex+i].TermOfEntry != args.Entries[i].TermOfEntry {
					isMatch = false
				}
			}

			// 2. 如果存在冲突的条目，再进行日志复制
			if isMatch == false {
				// 2.1. 进行日志复制，并更新commitIndex
				rf.log = append(rf.log[:nextIndex], args.Entries...) // [0, nextIndex) + entries
			}

			DPrintf("[AppendEntries]: Id %d Term %d State %v\t||\tcommitIndex %d while leaderCommit %d"+
				" for leader %d\n", rf.me, rf.currentTerm, rf.status, rf.commitIndex,
				args.LeaderCommit, args.LeaderId)

			// if leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
			if args.LeaderCommit > rf.commitIndex {
				rf.commitIndex = args.LeaderCommit
				if rf.commitIndex > len(rf.log)-1 {
					rf.commitIndex = len(rf.log) - 1
				}
			}

			index := nextIndex + len(args.Entries) - 1
			DPrintf("[AppendEntries]: Id %d Term %d State %v\t||\tconsistency check pass for index %d"+
				" with args's prevLogIndex %d args's prevLogTerm %d\n", rf.me, rf.currentTerm, rf.status,
				index, args.PrevLogIndex, args.PrevLogTerm)

			// Reset timeout when received leader's AppendEntries RPC
			rf.timeoutTimer.Reset(RandomTimeBetween(TIMEOUT_LOWER_BOUND, TIMEOUT_UPPER_BOUND))

			// 更新了commitIndex之后给applyCond条件变量发信号，以应用新提交的entries到状态机
			rf.applyCond.Broadcast()

			reply.Term = rf.currentTerm
			reply.Success = true
			return

		} else {

			nextIndex := args.PrevLogIndex + 1
			index := nextIndex + len(args.Entries) - 1

			DPrintf("[AppendEntries]: Id %d Term %d State %v\t||\tconsistency check failed for index %d"+
				" with args's prevLogIndex %d args's prevLogTerm %d\n",
				rf.me, rf.currentTerm, rf.status, index, args.PrevLogIndex, args.PrevLogTerm)

			//如果peer的日志长度小于leader的nextIndex
			if len(rf.log) < nextIndex {
				lastIndex := len(rf.log) - 1
				lastTerm := rf.log[lastIndex].TermOfEntry
				reply.ConflictTerm = lastTerm
				reply.ConflictFirstIndex = lastIndex

				DPrintf("[AppendEntries]: Id %d Term %d State %v\t||\tlog's len %d"+
					" is shorter than args's prevLogIndex %d\n",
					rf.me, rf.currentTerm, rf.status, len(rf.log), args.PrevLogIndex)
			} else {
				reply.ConflictTerm = rf.log[args.PrevLogIndex].TermOfEntry
				reply.ConflictFirstIndex = args.PrevLogIndex
				DPrintf("[AppendEntries]: Id %d Term %d State %v\t||\tconsistency check failed"+
					" with args's prevLogIndex %d args's prevLogTerm %d while it's prevLogTerm %d in"+
					" prevLogIndex %d\n", rf.me, rf.currentTerm, rf.status,
					args.PrevLogIndex, args.PrevLogTerm, rf.log[args.PrevLogIndex].TermOfEntry, args.PrevLogIndex)
			}
			// 递减reply.ConflictFirstIndex直到index为log中第一个term为reply.ConflictTerm的entry
			for i := reply.ConflictFirstIndex - 1; i >= 0; i-- {
				if rf.log[i].TermOfEntry != reply.ConflictTerm {
					break
				} else {
					reply.ConflictFirstIndex -= 1
				}
			}
			DPrintf("[AppendEntries]: Id %d Term %d State %v\t||\treply's conflictFirstIndex %d"+
				" and conflictTerm %d\n", rf.me, rf.currentTerm, rf.status,
				reply.ConflictFirstIndex, reply.ConflictTerm)
		}
	}

	reply.Term = rf.currentTerm
	reply.Success = false
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
