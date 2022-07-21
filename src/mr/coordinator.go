package mr

import (
	"log"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	lk sync.RWMutex

	MapTaskNum       int
	ReduceTaskNum    int
	MapTaskStatus    []int
	ReduceTaskStatus []int
	Status           int
	TaskKeys         []string
	TaskValues       []string
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) CheckStatus(req *CheckStatusReq, resp *CheckStatusResp) error {
	defer func() {
		c.lk.RUnlock()
	}()
	c.lk.RLock()
	resp.Status = c.Status
	return nil
}

func (c *Coordinator) GetMapKV(req *GetMapKVReq, resp *GetMapKVResp) error {
	defer func() {
		c.lk.Unlock()
	}()
	log.Printf("[GetMapKV] req:%+v", req)
	c.lk.Lock()
	for i, taskStatus := range c.MapTaskStatus {
		if taskStatus == TASK_STATUS_UNSTARTED {
			resp.Index = i
			resp.Need = true
			resp.Key = c.TaskKeys[i]
			c.MapTaskStatus[i] = TASK_STATUS_PROCESS
			if c.RecruitAllMapper() && c.Status == STATUS_MAPPER_NEEDED {
				c.Status = STATUS_MAPPER_PROCESS
			}
			return nil
		}
	}
	resp.Need = false
	return nil
}

func (c *Coordinator) GetReduceKV(req *GetReduceKVReq, resp *GetReduceKVResp) error {
	defer func() {
		c.lk.Unlock()
	}()
	c.lk.Lock()
	for i, taskStatus := range c.ReduceTaskStatus {
		if taskStatus == TASK_STATUS_UNSTARTED {
			resp.Index = i
			resp.MapLen = c.MapTaskNum
			resp.Need = true
			c.ReduceTaskStatus[i] = TASK_STATUS_PROCESS
			if c.RecruitAllReducer() && c.Status == STATUS_REDUCER_NEEDED {
				c.Status = STATUS_REDUCER_PROCESS
			}
			return nil
		}
	}
	resp.Need = false
	return nil
}

func (c *Coordinator) FinishMap(req *FinishMapReq, resp *FinishMapResp) error {
	defer func() {
		c.lk.Unlock()
	}()
	c.lk.Lock()
	c.MapTaskStatus[req.Index] = TASK_STATUS_FINISHED
	if c.Status == STATUS_MAPPER_PROCESS && c.FinishAllMapTask() {
		c.Status = STATUS_REDUCER_NEEDED
	}
	return nil
}

func (c *Coordinator) FinishReduce(req FinishReduceReq, resp *FinishReduceResp) error {
	defer func() {
		c.lk.Unlock()
	}()
	c.lk.Lock()
	c.ReduceTaskStatus[req.Index] = TASK_STATUS_FINISHED
	if c.Status == STATUS_REDUCER_PROCESS && c.FinishAllMapTask() {
		c.Status = STATUS_FINISHED
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// Done
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	defer func() {
		c.lk.RUnlock()
	}()
	c.lk.RLock()
	if c.Status == STATUS_FINISHED {
		ret = true
	}

	return ret
}

// RecruitAllMapper to check if status can change (but can be improved using bit calculation)
func (c *Coordinator) RecruitAllMapper() bool {
	for _, mapTaskStatus := range c.MapTaskStatus {
		if mapTaskStatus == TASK_STATUS_UNSTARTED {
			return false
		}
	}
	return true
}

func (c *Coordinator) RecruitAllReduce() bool {
	for _, reduceTaskStatus := range c.ReduceTaskStatus {
		if reduceTaskStatus == TASK_STATUS_UNSTARTED {
			return false
		}
	}
	return true
}

func (c *Coordinator) FinishAllMapTask() bool {
	for _, mapTaskStatus := range c.MapTaskStatus {
		if mapTaskStatus != TASK_STATUS_FINISHED {
			return false
		}
	}
	return true
}

func (c *Coordinator) RecruitAllReducer() bool {
	for _, reduceTaskStatus := range c.ReduceTaskStatus {
		if reduceTaskStatus == TASK_STATUS_UNSTARTED {
			return false
		}
	}
	return true
}

// MakeCoordinator
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.ReduceTaskNum = nReduce
	c.MapTaskNum = len(files)
	c.MapTaskStatus = make([]int, c.MapTaskNum, c.MapTaskNum)
	c.ReduceTaskStatus = make([]int, c.ReduceTaskNum, c.ReduceTaskNum)
	c.Status = 0
	c.TaskKeys = files
	// c.TaskValues = make([]string, 0)

	//for _, filename := range files {
	//	file, err := os.Open(filename)
	//	if err != nil {
	//		log.Fatalf("cannot open %v", filename)
	//	}
	//	content, err := ioutil.ReadAll(file)
	//	if err != nil {
	//		log.Fatalf("cannot read %v", filename)
	//	}
	//	file.Close()
	//	c.TaskValues = append(c.TaskValues, string(content))
	//}

	c.server()
	return &c
}
