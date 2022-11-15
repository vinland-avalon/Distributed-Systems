package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
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

type Entry struct {
	Command interface{}
	Term    int
	Index   int
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

	currentTerm int
	votedFor    int
	// getVoteNum int
	log []Entry

	commitIndex int
	lastApplied int

	//leaderId int // reject kvserver and tell it who is the leader

	status int
	////election_timer *time.Timer
	////heartbeat_timer *time.Timer // also the append entries timer ,for 2A is heartbeat
	//lastResetElectionTime time.Time

	/*
		TODO: THIS is a instruction for my Index and len
		TODO: init log[] with a entry{Term:0,commmand:null} (shouldn't be applied or persist in snapshot)
		TODO: so "len(rf.log) - 1" is the index, and index is same with the arrayindex
		TODO: rf.lastSSpointIndex + arrayindex = e.GlobalIndex || e = rf.log[rf.GlobalIndex-rf.lastSSPointIndex]
		TODO: nextIndex is "NExt entry to send to that sever" positive "you have all leader's entries"
		TODO: matchIndex is "highest known to be replicated on server" pessmistive "you have nothing"
	*/
	nextIndex  []int
	matchIndex []int

	applyCh chan ApplyMsg
	// SnapShot Point use
	lastSSPointIndex int
	lastSSPointTerm  int

	heartbeatTimer *time.Timer
	timeoutTimer   *time.Timer
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
	rf.log = []Entry{}
	rf.log = append(rf.log, Entry{})
	// rf.termOfLog = make([]int, 0)
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.dead = 0
	rf.currentTerm = 0
	rf.lastSSPointIndex = 0
	rf.lastSSPointTerm = 0
	for i, _ := range rf.matchIndex {
		rf.matchIndex[i] = 0
		rf.nextIndex[i] = 0
	}

	rf.applyCh = applyCh
	rf.status = FOLLOWER

	DPrintf("[%v] me created, arguments: %+v", rf.me, rf)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	if rf.lastSSPointIndex > 0 {
		rf.lastApplied = rf.lastSSPointIndex
	}

	// start ticker goroutine to start elections
	rf.heartbeatTimer = time.NewTimer(RandomTimeBetween(HEARTBEAT_INTERVAL, HEARTBEAT_INTERVAL+1))
	rf.timeoutTimer = time.NewTimer(RandomTimeBetween(TIMEOUT_LOWER_BOUND, TIMEOUT_UPPER_BOUND))

	go rf.ticker()

	return rf
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
// Term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	if term, isLeader = rf.GetState(); isLeader && !rf.killed() {
		rf.mu.Lock()
		index = rf.getLastIndex() + 1
		entry := Entry{Command: command, Term: rf.currentTerm, Index: index}
		rf.log = append(rf.log, entry)
		DPrintf("[Start]: Id %v Term %v State %v\t||\treplicate the command to Log index %v\n", rf.me, rf.currentTerm, rf.status, index)
		rf.persist()
		rf.mu.Unlock()
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
		applyTimer := time.NewTimer(APPLIED_TIMEOUT * time.Millisecond)
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
			rf.heartbeatTimer = time.NewTimer(RandomTimeBetween(HEARTBEAT_INTERVAL, HEARTBEAT_INTERVAL+1))
		case <-rf.timeoutTimer.C:
			if rf.status != LEADER {
				DPrintf("[%v], as status of %v, going to issue an election", rf.me, rf.status)
				rf.ChangeStatus(CANDIDATE)
				// rf.heartbeatTimer.Reset(RandomTimeBetween(HEARTBEAT_INTERVAL, HEARTBEAT_INTERVAL+1))
			}
			rf.timeoutTimer = time.NewTimer(RandomTimeBetween(TIMEOUT_LOWER_BOUND, TIMEOUT_UPPER_BOUND))
		case <-applyTimer.C:
			rf.committedToApplied()
			applyTimer = time.NewTimer(APPLIED_TIMEOUT * time.Millisecond)
		}
	}
}

func (rf *Raft) committedToApplied() {
	// put the committed entry to apply on the state machine
	rf.mu.Lock()
	if rf.lastApplied >= rf.commitIndex {
		rf.mu.Unlock()
		return
	}

	Messages := make([]ApplyMsg, 0)
	// log.Printf("[!!!!!!--------!!!!!!!!-------]Restart, LastSSP: %d, LastApplied :%d, commitIndex %d",rf.lastSSPointIndex,rf.lastApplied,rf.commitIndex)
	//log.Printf("[ApplyEntry] LastApplied %d, commitIndex %d, lastSSPindex %d, len %d, lastIndex %d",rf.lastApplied,rf.commitIndex,rf.lastSSPointIndex, len(rf.log),rf.getLastIndex())
	for rf.lastApplied < rf.commitIndex && rf.lastApplied < rf.getLastIndex() {
		//for rf.lastApplied < rf.commitIndex {
		rf.lastApplied += 1
		//DPrintf("[ApplyEntry---] %d apply entry index %d, command %v, term %d, lastSSPindex %d",rf.me,rf.lastApplied,rf.getLogWithIndex(rf.lastApplied).Command,rf.getLogWithIndex(rf.lastApplied).Term,rf.lastSSPointIndex)
		Messages = append(Messages, ApplyMsg{
			CommandValid:  true,
			SnapshotValid: false,
			CommandIndex:  rf.lastApplied,
			Command:       rf.getLogWithIndex(rf.lastApplied).Command,
		})
	}
	rf.mu.Unlock()

	for _, messages := range Messages {
		rf.applyCh <- messages
	}

}

// change the raft server state and do something init
func (rf *Raft) ChangeStatus(howtochange int) {

	if howtochange == FOLLOWER {
		rf.status = FOLLOWER
		rf.votedFor = -1
		//rf.getVoteNum = 0
		rf.persist()
		rf.timeoutTimer = time.NewTimer(RandomTimeBetween(TIMEOUT_LOWER_BOUND, TIMEOUT_UPPER_BOUND))
		rf.heartbeatTimer = time.NewTimer(RandomTimeBetween(HEARTBEAT_INTERVAL, HEARTBEAT_INTERVAL+1))
	}

	if howtochange == CANDIDATE {
		rf.status = CANDIDATE
		rf.votedFor = rf.me
		rf.currentTerm += 1
		rf.persist()
		rf.IssueElection(rf.currentTerm)
		rf.timeoutTimer = time.NewTimer(RandomTimeBetween(TIMEOUT_LOWER_BOUND, TIMEOUT_UPPER_BOUND))
		rf.heartbeatTimer = time.NewTimer(RandomTimeBetween(HEARTBEAT_INTERVAL, HEARTBEAT_INTERVAL+1))
	}

	if howtochange == LEADER {
		rf.status = LEADER
		rf.votedFor = -1
		rf.persist()

		rf.nextIndex = make([]int, len(rf.peers))
		for i := 0; i < len(rf.peers); i++ {
			//rf.nextIndex[i] = len(rf.log)
			rf.nextIndex[i] = rf.getLastIndex() + 1
		}

		rf.matchIndex = make([]int, len(rf.peers))
		//rf.matchIndex[rf.me] = len(rf.log) - 1
		rf.matchIndex[rf.me] = rf.getLastIndex()
		rf.BroadcastHeartbeat()
		rf.timeoutTimer = time.NewTimer(RandomTimeBetween(TIMEOUT_LOWER_BOUND, TIMEOUT_UPPER_BOUND))
		rf.heartbeatTimer = time.NewTimer(RandomTimeBetween(HEARTBEAT_INTERVAL, HEARTBEAT_INTERVAL+1))
		//rf.leaderAppendEntries()
	}
}
