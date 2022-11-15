package raft

func (rf *Raft) BroadcastHeartbeat() {
	// send to every server to replicate logs to them
	for index := range rf.peers {
		if index == rf.me {
			continue
		}
		// parallel replicate logs to sever

		go func(server int) {
			rf.mu.Lock()
			if rf.status != LEADER {
				rf.mu.Unlock()
				return
			}

			aeArgs := AppendEntriesArgs{}

			if rf.getLastIndex() >= rf.nextIndex[server] {
				DPrintf("[LeaderAppendEntries]Leader %d (term %d) to server %d, index %d --- %d", rf.me, rf.currentTerm, server, rf.nextIndex[server], rf.getLastIndex())
				//rf.printLogsForDebug()
				entriesNeeded := make([]Entry, 0)
				entriesNeeded = append(entriesNeeded, rf.log[rf.nextIndex[server]-rf.lastSSPointIndex:]...)
				prevLogIndex, prevLogTerm := rf.getPrevLogInfo(server)
				aeArgs = AppendEntriesArgs{
					rf.currentTerm,
					rf.me,
					prevLogIndex,
					prevLogTerm,
					entriesNeeded,
					rf.commitIndex,
				}
			} else {
				prevLogIndex, prevLogTerm := rf.getPrevLogInfo(server)
				aeArgs = AppendEntriesArgs{
					rf.currentTerm,
					rf.me,
					prevLogIndex,
					prevLogTerm,
					[]Entry{},
					rf.commitIndex,
				}
				DPrintf("[LeaderSendHeartBeat]Leader %d (term %d) to server %d,nextIndex %d, matchIndex %d, lastIndex %d", rf.me, rf.currentTerm, server, rf.nextIndex[server], rf.matchIndex[server], rf.getLastIndex())
			}
			aeReply := AppendEntriesReply{}
			rf.mu.Unlock()

			re := rf.sendAppendEntries(server, &aeArgs, &aeReply)

			//if re == false{
			//	rf.mu.Lock()
			//	DPrintf("[HeartBeat ERROR]Leader %d (term %d) get no reply from server %d",rf.me,rf.currentTerm,server)
			//	rf.mu.Unlock()
			//}

			if re == true {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if rf.status != LEADER {
					return
				}

				if aeReply.Term > rf.currentTerm {
					rf.currentTerm = aeReply.Term
					rf.ChangeStatus(FOLLOWER)
					return
				}

				DPrintf("[HeartBeatGetReturn] Leader %d (term %d) ,from Server %d, prevLogIndex %d", rf.me, rf.currentTerm, server, aeArgs.PrevLogIndex)

				if aeReply.Success {
					DPrintf("[HeartBeat SUCCESS] Leader %d (term %d) ,from Server %d, prevLogIndex %d", rf.me, rf.currentTerm, server, aeArgs.PrevLogIndex)
					rf.matchIndex[server] = aeArgs.PrevLogIndex + len(aeArgs.Entries)
					rf.nextIndex[server] = rf.matchIndex[server] + 1
					rf.updateCommitIndex(LEADER, 0)
				}

				if !aeReply.Success {
					if aeReply.ConflictingIndex != -1 {
						DPrintf("[HeartBeat CONFLICT] Leader %d (term %d) ,from Server %d, prevLogIndex %d, Confilicting %d", rf.me, rf.currentTerm, server, aeArgs.PrevLogIndex, aeReply.ConflictingIndex)
						rf.nextIndex[server] = aeReply.ConflictingIndex
					}
				}
			}

		}(index)

	}
}

func (rf *Raft) updateCommitIndex(role int, leaderCommit int) {
	if role != LEADER {
		if leaderCommit > rf.commitIndex {
			lastNewIndex := rf.getLastIndex()
			if leaderCommit >= lastNewIndex {
				rf.commitIndex = lastNewIndex
			} else {
				rf.commitIndex = leaderCommit
			}
		}
		DPrintf("[CommitIndex] Fllower %d commitIndex %d, now log: %+v", rf.me, rf.commitIndex, rf.log)
		return
	}

	if role == LEADER {
		rf.commitIndex = rf.lastSSPointIndex
		//for index := rf.commitIndex+1;index < len(rf.log);index++ {
		//for index := rf.getLastIndex();index>=rf.commitIndex+1;index--{
		for index := rf.getLastIndex(); index >= rf.lastSSPointIndex+1; index-- {
			sum := 0
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					sum += 1
					continue
				}
				if rf.matchIndex[i] >= index {
					sum += 1
				}
			}

			//log.Printf("lastSSP:%d, index: %d, commitIndex: %d, lastIndex: %d",rf.lastSSPointIndex, index, rf.commitIndex, rf.getLastIndex())
			if sum >= len(rf.peers)/2+1 && rf.getLogTermWithIndex(index) == rf.currentTerm {
				rf.commitIndex = index
				break
			}

		}
		DPrintf("[CommitIndex] Leader %d(term%d) commitIndex %d", rf.me, rf.currentTerm, rf.commitIndex)
		return
	}

}
