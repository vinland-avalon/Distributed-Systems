package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type CheckStatusReq struct {
}
type CheckStatusResp struct {
	Status int
}

type GetMapKVReq struct {
}
type GetMapKVResp struct {
	Index int
	Need  bool
	Key   string
}

type FinishMapReq struct {
	Index int
}
type FinishMapResp struct {
}

type GetReduceKVReq struct {
}
type GetReduceKVResp struct {
	Index  int
	Need   bool
	MapLen int
}

type FinishReduceReq struct {
	Index int
}
type FinishReduceResp struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
