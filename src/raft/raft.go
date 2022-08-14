package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// wbh: persistent massages, which needed to be stored in files later
	currentTerm int
	votedFor    *labrpc.ClientEnd
	log         []Entry
	termOfLog   []int

	// wbh: all server's state
	commitIndex int
	lastApplied int

	// wbh: leader's unique state
	nextIndex  []int
	matchIndex []int

	status  int
	applyCh chan ApplyMsg

	heartbeatTimer *time.Timer
	timeoutTimer   *time.Timer

	applyCond sync.Cond
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.status == LEADER

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	if term, isLeader = rf.GetState(); isLeader {

		// 1. leader将客户端command作为新的entry追加到自己的本地log
		rf.mu.Lock()
		entry := Entry{Command: command, TermOfEntry: rf.currentTerm}
		rf.log = append(rf.log, entry)
		index = len(rf.log) - 1
		DPrintf("[Start]: Id %v Term %v State %v\t||\treplicate the command to Log index %v\n",
			rf.me, rf.currentTerm, rf.status, index)
		nReplica := 1

		// 接收到客户端命令，并写入log，保存下持久状态
		rf.persist()

		rf.mu.Unlock()

		// 2. 给其他peers并行发送AppendEntries RPC以复制该entry
		go func(nReplica *int, index int, commitIndex int, term int) {
			var wg sync.WaitGroup
			majority := len(rf.peers)/2 + 1
			agreement := false
			// isCommitted := false

			rf.mu.Lock()
			DPrintf("[Start]: Id %v Term %v State %v\t||\tcreate an goroutine for index %v"+
				" to issue parallel and wait\n", rf.me, rf.currentTerm, rf.status, index)
			rf.mu.Unlock()

			for i, _ := range rf.peers {

				// 避免进入了新任期，还发送过时的entries，因为leader原则上只能提交当前任期的entry
				rf.mu.Lock()
				if rf.currentTerm != term {
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()

				if i == rf.me {
					continue
				}
				wg.Add(1)

				// 给peer:i发送AppendEntries RPC
				go func(i int, rf *Raft, nReplica *int) {

					defer wg.Done()
					nextIndex := index + 1

					// 在AppendEntries RPC一致性检查失败后，递减nextIndex，重试
				retry:

					// 因为涉及到retry操作，避免过时的leader的retry操作继续下去
					_, isLeader = rf.GetState()
					if isLeader == false {
						return
					}

					// 避免进入了新任期，还发送过时的entries，因为leader原则上只能提交当前任期的entry
					rf.mu.Lock()
					if rf.currentTerm != term {
						rf.mu.Unlock()
						return
					}
					rf.mu.Unlock()

					rf.mu.Lock()
					// 封装AppendEntriesArgs参数
					prevLogIndex := nextIndex - 1
					if prevLogIndex < 0 {
						DPrintf("[Start]: Id %v Term %v State %v\t||\tinvalid prevLogIndex %v for index %v"+
							" peer %v\n", rf.me, rf.currentTerm, rf.status, prevLogIndex, index, i)
					}
					prevLogTerm := rf.log[prevLogIndex].TermOfEntry
					entries := make([]Entry, 0)
					if nextIndex <= index {
						entries = rf.log[nextIndex : index+1] // [nextIndex, index+1)
					}
					args := AppendEntriesArgs{Term: term, LeaderId: rf.me,
						PrevLogIndex: prevLogIndex, PrevLogTerm: prevLogTerm,
						Entries: entries, LeaderCommit: commitIndex}
					DPrintf("[Start]: Id %v Term %v State %v\t||\tissue AppendEntries RPC for index %v"+
						" to peer %v with nextIndex %v\n", rf.me, rf.currentTerm, rf.status, index, i, prevLogIndex+1)
					rf.mu.Unlock()
					var reply AppendEntriesReply

					ok := rf.sendAppendEntries(i, &args, &reply)

					// 发送AppendEntries RPC失败，表明无法和peer建立通信，直接放弃
					if ok == false {
						rf.mu.Lock()
						DPrintf("[Start]: Id %v Term %v State %v\t||\tissue AppendEntries RPC for index %v"+
							" to peer %v failed\n", rf.me, rf.currentTerm, rf.status, index, i)
						rf.mu.Unlock()
						// 发送AppendEntries失败，应该直接返回?
						return
					}

					// 图2通常不讨论当你收到旧的RPC回复(replies)时应该做什么。根据经验，
					// 我们发现到目前为止最简单的方法是首先记录该回复中的任期(the term
					// in the reply)(它可能高于你的当前任期)，然后将当前任期(current term)
					// 和你在原始RPC中发送的任期(the term you sent in your original RPC)
					// 比较。如果两者不同，请删除(drop)回复并返回。只有(only)当两个任期相同，
					// 你才应该继续处理该回复。通过一些巧妙的协议推理(protocol reasoning)，
					// 你可以在这里进一步的优化，但是这个方法似乎运行良好(work well)。并且
					// 不(not)这样做将导致一个充满鲜血、汗水、眼泪和失望的漫长而曲折的(winding)道路。
					rf.mu.Lock()
					if rf.currentTerm != args.Term {
						rf.mu.Unlock()
						return
					}
					rf.mu.Unlock()

					// AppendEntries被拒绝，原因可能是leader任期过时，或者一致性检查未通过
					if reply.Success == false {
						rf.mu.Lock()
						DPrintf("[Start]: Id %v Term %v State %v\t||\tAppendEntries RPC for index %v is rejected"+
							" by peer %v\n", rf.me, rf.currentTerm, rf.status, index, i)
						// 如果是leader任期过时，需要切换到follower并立即退出。这里应该使用
						// args.Term和reply.Term比较，因为一致性检查就是比较的这两者。而直接
						// 使用rf.currentTerm和reply.Term比较的话，任期过时的可能性就小了。
						// 因为rf.currentTerm在同步发送RPC的过程中可能已经发生改变！
						if args.Term < reply.Term {
							rf.currentTerm = reply.Term
							rf.votedFor = nil
							rf.ChangeStatus(FOLLOWER)
							//rf.resetElectionTimer()

							DPrintf("[Start]: Id %v Term %v State %v\t||\tAppendEntries PRC for index %v is rejected by"+
								" peer %v due to newer peer's term %v\n", rf.me, rf.currentTerm, rf.status,
								index, i, reply.Term)
							rf.persist()

							rf.mu.Unlock()
							return

						} else { // 如果是一致性检查失败，则递减nextIndex，重试

							// 这里递减nextIndex使用了论文中提到的优化策略：
							// If desired, the protocol can be optimized to reduce the number of rejected AppendEntries
							// RPCs. For example,  when rejecting an AppendEntries request, the follower can include the
							// term of the conflicting entry and the first index it stores for that term. With this
							// information, the leader can decrement nextIndx to bypass all of the conflicting entries
							// in that term; one AppendEntries RPC will be required for each term with conflicting entries,
							// rather than one RPC per entry.
							// 只存在reply.ConflictFirstIndex < nextIndex，由于一致性检查是从nextIndex-1(prevLogIndex)处
							// 查看的，所以不会出现reply.ConflictFirstIndex >= nextIndex。

							nextIndex--

							DPrintf("[Start]:[%v] Term %v Status %v, AppendEntries RPC for index %v is rejected by [%v] due to less pre index, so --", rf.me, rf.currentTerm, rf.status, index, i)

							rf.mu.Unlock()
							goto retry

						}
					} else { // AppendEntries RPC发送成功

						rf.mu.Lock()
						DPrintf("[Start]: Id %v Term %v State %v\t||\tsend AppendEntries PRC for index %v to peer %v success\n",
							rf.me, rf.currentTerm, rf.status, index, i)

						// 如果当前index更大，则更新该peer对应的nextIndex和matchIndex
						if rf.nextIndex[i] < index+1 {
							rf.nextIndex[i] = index + 1
							rf.matchIndex[i] = index
						}
						*nReplica += 1
						DPrintf("[Start]: Id %v Term %v State %v\t||\tnReplica %v for index %v\n",
							rf.me, rf.currentTerm, rf.status, *nReplica, index)

						// 如果已经将该entry复制到了大多数peers，接着检查index编号的这条entry的任期
						// 是否为当前任期，如果是则可以提交该条目
						if agreement == false && rf.status == LEADER && *nReplica >= majority {
							agreement = true
							DPrintf("[Start]: Id %v Term %v State %v\t||\thas replicated the entry with index %v"+
								" to the majority with nReplica %v\n", rf.me, rf.currentTerm, rf.status,
								index, *nReplica)

							// 如果index大于commitIndex，而且index编号的entry的任期等于当前任期，提交该entry
							if rf.commitIndex < index && rf.log[index].TermOfEntry == rf.currentTerm {
								DPrintf("[Start]: Id %v Term %v State %v\t||\tadvance the commitIndex to %v\n",
									rf.me, rf.currentTerm, rf.status, index)
								// isCommitted = true

								// 提升commitIndex
								rf.commitIndex = index

								// 当被提交的entries被复制到多数peers后，可以发送一次心跳通知其他peers更新commitIndex
								go rf.BroadcastHeartbeat()

								// 更新了commitIndex可以给applyCond条件变量发信号，
								// 以应用新提交的entries到状态机
								DPrintf("[Start]: Id %v Term %v State %v\t||\tapply updated commitIndex %v to applyCh\n",
									rf.me, rf.currentTerm, rf.status, rf.commitIndex)
								rf.applyCond.Broadcast()

								//// 已完成了多数者日志的复制，保存下持久状态
								//rf.persist()
							}

						}
						//// 当被提交的entries被复制到所有peers后，可以发送一次心跳通知其他peers更新commitIndex
						//if *nReplica == len(rf.peers) && isCommitted {
						//	// 同时发送给其他peers发送一次心跳，使它们更新commitIndex
						//	go rf.broadcastHeartbeat()
						//}

						rf.mu.Unlock()
					}

				}(i, rf, nReplica)
			}

			// 等待所有发送AppendEntries RPC的goroutine退出
			wg.Wait()

		}(&nReplica, index, rf.commitIndex, rf.currentTerm)

	}
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		select {
		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			if rf.status == LEADER {
				// send empty appendEntries RPC
				DPrintf("[%v], as status of %v, going to broadcast heartbeat", rf.me, rf.status)
				rf.mu.Unlock()
				rf.BroadcastHeartbeat()
			} else {
				rf.mu.Unlock()
			}
			rf.heartbeatTimer.Reset(RandomTimeBetween(HEARTBEAT_INTERVAL, HEARTBEAT_INTERVAL+1))
		case <-rf.timeoutTimer.C:
			rf.mu.Lock()
			if rf.status != LEADER {
				DPrintf("[%v], as status of %v, going to issue an election", rf.me, rf.status)
				rf.ChangeStatus(CANDIDATE)
				rf.currentTerm++
				// rf.timeoutTimer.Reset(RandomTimeBetween(TIMEOUT_LOWER_BOUND, TIMEOUT_UPPER_BOUND))
				rf.IssueElection(rf.currentTerm)
				// rf.heartbeatTimer.Reset(RandomTimeBetween(HEARTBEAT_INTERVAL, HEARTBEAT_INTERVAL+1))
			}
			rf.mu.Unlock()
			rf.timeoutTimer.Reset(RandomTimeBetween(TIMEOUT_LOWER_BOUND, TIMEOUT_UPPER_BOUND))
		}
	}
}

func (rf *Raft) IssueElection(electionTerm int) {
	// rf.currentTerm++
	rf.votedFor = rf.peers[rf.me]

	mu := sync.Mutex{}
	cond := sync.NewCond(&mu)
	count := 1
	finished := 1
	lastLogTerm := 0
	//if len(rf.log) != 0 {
	//	lastLogTerm = rf.termOfLog[len(rf.log)-1]
	//}
	args := &RequestVoteArgs{
		Term:         electionTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  lastLogTerm,
	}
	id := GenerateId()
	DPrintf("[%v], in term %v, issues an election and broadcasts RequestVoteArgs(%v): %+v", rf.me, electionTerm, id, *args)
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
				DPrintf("[%v] receives RequestVoteReply(%v) {%+v} from [%v] in term %v", rf.me, id, *reply, index, electionTerm)
				// DPrintf("$$$$$$$$$$$$$$$$$$$$$$$$$$\n%v,%v,%v", args.Term, rf.currentTerm, rf.status)
				if args.Term == rf.currentTerm && rf.status == CANDIDATE {
					if reply.Term > rf.currentTerm {
						DPrintf("[%v]'s RequestVote in term %v finds a new leader [%v] with term %v, step into follower", rf.me, args.Term, index, reply.Term)
						rf.UpdateCurrentTerm(reply.Term)
					} else if reply.VoteGranted == true {
						//DPrintf("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\nenter here?")
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
	if count > len(rf.peers)/2 && electionTerm == rf.currentTerm {
		DPrintf("[%v] receives majority votes in term %v and becomes leader", rf.me, electionTerm)
		rf.ChangeStatus(LEADER)
		rf.BroadcastHeartbeat()
		//DPrintf("((((((((((((((((((((((((((((((((((((")
		rf.persist()
	}
	mu.Unlock()
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.mu = sync.RWMutex{}

	rf.log = make([]Entry, 1)
	// rf.termOfLog = make([]int, 0)
	rf.votedFor = nil
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.dead = 0
	rf.currentTerm = 0
	rf.matchIndex = make([]int, len(peers))
	rf.nextIndex = make([]int, len(peers))
	for i, _ := range rf.matchIndex {
		rf.matchIndex[i] = -1
		rf.nextIndex[i] = 0
	}
	rf.peers = peers

	rf.applyCh = applyCh
	rf.status = FOLLOWER

	rf.heartbeatTimer = time.NewTimer(RandomTimeBetween(HEARTBEAT_INTERVAL, HEARTBEAT_INTERVAL+1))
	rf.timeoutTimer = time.NewTimer(RandomTimeBetween(TIMEOUT_LOWER_BOUND, TIMEOUT_UPPER_BOUND))

	DPrintf("[%v] me created, arguments: %+v", rf.me, rf)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

// Reset Status,VoteFor, currentTerm
func (rf *Raft) UpdateCurrentTerm(term int) {
	DPrintf("[%v] updateTerm from %v to %v, and update its status from %v to %v", rf.me, rf.currentTerm, term, rf.status, FOLLOWER)
	rf.currentTerm = term
	rf.status = FOLLOWER
	rf.votedFor = nil
}

func (rf *Raft) BroadcastHeartbeat() {
	term := rf.currentTerm
	prevLogIndex := len(rf.log) - 1
	prevLogTerm := 0
	//if prevLogIndex != -1 {
	//	prevLogTerm = rf.termOfLog[prevLogIndex]
	//}
	leaderCommit := rf.commitIndex
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(index int) {
			args := AppendEntriesArgs{
				Term:         term,
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      nil,
				LeaderCommit: leaderCommit,
			}
			rf.sendAppendEntries(index, &args, &AppendEntriesReply{})
		}(i)
	}
	//DPrintf("|||||||||||||||||||||||||||||||")
}

func (rf *Raft) ChangeStatus(status int) {
	DPrintf("[%v] switch status from %v to %v", rf.me, rf.status, status)
	rf.status = status
}

func (rf *Raft) GetFirstLog() *Entry {
	return &(rf.log[0])
}

func (rf *Raft) GetLastLog() *Entry {
	return &(rf.log[len(rf.log)-1])
}

// a dedicated applier goroutine to guarantee that each log will be push into applyCh exactly once, ensuring that service's applying entries and raft's committing entries can be parallel
func (rf *Raft) applier() {
	for rf.killed() == false {
		rf.mu.Lock()
		// if there is no need to apply entries, just release CPU and wait other goroutine's signal if they commit new entries
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}
		firstIndex, commitIndex, lastApplied := rf.GetFirstLog().Index, rf.commitIndex, rf.lastApplied
		entries := make([]Entry, commitIndex-lastApplied)

		// lastApplied 3,
		copy(entries, rf.log[lastApplied+1-firstIndex:commitIndex+1-firstIndex])
		rf.mu.Unlock()
		for _, entry := range entries {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.Index,
			}
		}
		rf.mu.Lock()
		DPrintf("{Node %v} applies entries %v-%v in term %v", rf.me, rf.lastApplied, commitIndex, rf.currentTerm)
		// use commitIndex rather than rf.commitIndex because rf.commitIndex may change during the Unlock() and Lock()
		// use Max(rf.lastApplied, commitIndex) rather than commitIndex directly to avoid concurrently InstallSnapshot rpc causing lastApplied to rollback
		rf.lastApplied = Max(rf.lastApplied, commitIndex)
		rf.mu.Unlock()
	}
}
