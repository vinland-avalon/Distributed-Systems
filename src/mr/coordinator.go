package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"

// task status:
// 0 -> unstarted, 1 -> process, 2 -> finished
// total status:
// 0 -> map need, 1 -> map process, 
// 2-> reduce need, 3 -> reduce process, 4 -> totally finished
type Coordinator struct {
	// Your definitions here.
	MapTaskNum int
	ReduceTaskNum int
	MapTaskStatus []int
	ReduceTaskStatus []int
	Status int
	TaskKeys []string
	TaskValues []string
	
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) CheckStatus(req *CheckStatusReq, resp *CheckStatusResp) error {
	resp.Status = c.Status
	return nil
}

func (c *Coordinator) GetMapKV(req *GetMapKVReq, resp *GetMapKVResp) error {
	for i, taskStatus := range c.MapTaskStatus {
		if taskStatus == 0 {
			resp.Index = i
			resp.Need = true
			c.MapTaskStatus[i] = 1
			if c.RecruitAllMapper() && c.Status == 0 {
				c.Status = 1
			}
			return nil
		}
	}
	resp.Need = false
	return nil
}

func (c *Coordinator) FinishMap(req *FinishMapReq, resp *FinishMapResp) error {
	c.MapTaskStatus[req.Index] = 2
	if c.Status == 1 && c.FinishAllMapTask() {
		c.Status = 2
	}
	return nil
}

func (c *Coordinator) GetReduceKV(req *GetReduceKVReq, resp *GetReduceKVResp) error {
	for i, taskStatus := range c.ReduceTaskStatus {
		if taskStatus == 0 {
			resp.Index = i
			resp.MapLen = c.MapTaskNum
			resp.Need = true
			c.ReduceTaskStatus[i] = 1
			if c.RecruitAllReducer() && c.Status == 2 {
				c.Status = 3
			}
			return nil
		}
	}
	resp.Need = false
	return nil
}

func (c *Coordinator) FinishReduce(req FinishReduceReq, resp *FinishReduceResp) error {
	c.ReduceTaskStatus[req.Index] = 2
	if c.Status == 3 && c.FinishAllMapTask() {
		c.Status = 4
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false
	
	// Your code here.
	if c.status == 4 {
		ret = true
	}

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.ReduceTaskNum = nReduce
	c.MapTaskNum = len(files)
	c.MapTaskStatus = make([]int, c.MapTaskNum, 0)
	c.ReduceTaskStatus = make([]int, c.ReduceTaskNum, 0)
	c.Status = 0
	c.TaskKeys = files
	c.TaskValues = make([]string)

	for _, filename := range files {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		c.TaskValues = append(c.TaskValues, string(content))
	}

	c.server()
	return &c
}
