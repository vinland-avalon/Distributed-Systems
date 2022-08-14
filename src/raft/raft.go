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
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

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
	log         []ApplyMsg
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
	if len(rf.log) != 0 {
		lastLogTerm = rf.termOfLog[len(rf.log)-1]
	}
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

	rf.log = make([]ApplyMsg, 0)
	rf.termOfLog = make([]int, 0)
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
	if prevLogIndex != -1 {
		prevLogTerm = rf.termOfLog[prevLogIndex]
	}
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
