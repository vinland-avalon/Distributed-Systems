package raft

const (
	// timeOut time range: ms
	TIMEOUT_UPPER_BOUND = 750
	TIMEOUT_LOWER_BOUND = 500

	// status
	LEADER    = 1
	FOLLOWER  = 2
	CANDIDATE = 3

	// election result
	WIN_ELECTION  = 1
	LOSE_ELECTION = 2
)
