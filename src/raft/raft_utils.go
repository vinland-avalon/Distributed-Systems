package raft

func (rf *Raft) getLastIndex() int {
	return len(rf.log) - 1 + rf.lastSSPointIndex
}

func (rf *Raft) getLastTerm() int {
	if len(rf.log)-1 == 0 {
		return rf.lastSSPointTerm
	} else {
		return rf.log[len(rf.log)-1].Term
	}
}

func (rf *Raft) getLogTermWithIndex(globalIndex int) int {
	//log.Printf("[GetLogTermWithIndex] Sever %d,lastSSPindex %d ,len %d",rf.me,rf.lastSSPointIndex,len(rf.log))
	//if globalIndex - rf.lastSSPointIndex == 0{
	//	return rf.lastSSPointTerm
	//}
	return rf.log[globalIndex-rf.lastSSPointIndex].Term
}

func (rf *Raft) getPrevLogInfo(server int) (int, int) {
	newEntryBeginIndex := rf.nextIndex[server] - 1
	// TODO fix it in lab4
	lastIndex := rf.getLastIndex()
	if newEntryBeginIndex == lastIndex+1 {
		newEntryBeginIndex = lastIndex
	}
	return newEntryBeginIndex, rf.getLogTermWithIndex(newEntryBeginIndex)
}

func (rf *Raft) UpToDate(index int, term int) bool {
	//lastEntry := rf.log[len(rf.log)-1]
	lastIndex := rf.getLastIndex()
	lastTerm := rf.getLastTerm()
	return term > lastTerm || (term == lastTerm && index >= lastIndex)
}

func (rf *Raft) getLogWithIndex(globalIndex int) Entry {
	return rf.log[globalIndex-rf.lastSSPointIndex]
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

func (rf *Raft) GetFirstLog() *Entry {
	return &(rf.log[0])
}

func (rf *Raft) GetLastLog() *Entry {
	return &(rf.log[len(rf.log)-1])
}
