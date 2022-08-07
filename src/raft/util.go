package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if !Debug {
		log.Printf(format, a...)
	}
	return
}

func Min(a, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

func RandomTimeBetween(lower, higher int) time.Duration {
	return time.Duration(rand.Intn(higher-lower)+lower) * time.Millisecond
}

func (rf *Raft) containsPrevInfo(term int, index int) bool {
	return true
}

func (rf *Raft) conflictWithPrevInfo(term int, index int) bool {
	return false
}

func (rf *Raft) AppendEntriesFromPrevPos(index int) {

}

func (rf *Raft) CheckAndTryApply() {

}

func (rf *Raft) deleteWithPrevInfo(term int, index int) {

}
