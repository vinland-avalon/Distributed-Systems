package mr

import (
	"fmt"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	// //1. Send A message to check the status of coordinator
	// res := CheckStatusOfCoordinator()
	// if res == 0 {
	// 	return
	// }
	//2. Decide Works if keep alive, then its identification: mapper/reducer
	for {
		res := DecideRole()
		if res == 0 {
			return
		} else if res == 1 {
			//3. if mapper: 
			// rpc call, return corresponding task (key, also means filename); 
			getMapKVReq := GetMapKVReq{}
			getMapKVResp := GetMapKVResp{}
			ok := call("Coordinator.GetMapKV", &getMapKVReq, &getMapKVResp)
			if ok == false {
				return
			}
			if getMapKVResp.Need == false {
				time.Sleep(time.Second / 2)
				continue
			}
			mapperIndex := getMapKVResp.Index
			// mapf; 
			kva := mapf(getMapKVResp.key, getMapKVResp.value)
			// store the mapresult, using ihash to decide file-name
			for _, kv := range kva {
				fileName := ("mr-" + string(mapperIndex) + "-" + ihash(string(kv.Key)))
				err := kvAppendToFile(fileName, kv.Key, kv.Value)
				if err != nil {
					return
				}
			}
			// rpc return res
			finishMapReq := FinishMapReq{
				Index := mapperIndex
			}
			finishMapResp := FinishMapResp{}
			ok = call("Coordinator.FinishMap", &finishMapReq, &finishMapResp)
			if ok == false {
				return
			}
		} else if res == 2 {
			//3. if reducer:
			getReduceKVReq := GetReduceKVReq{}
			getReduceKVResp := GetReduceKVResp{}
			// rpc call, return corresponding task (key, one to ten); 
			ok := call("Coordinator.GetReduceKV", &getReduceKVReq, &getReduceKVResp)
			if ok == false {
				return
			} 
			if getReduceKVResp.Need == false {
				time.Sleep(time.Second / 2)
				continue
			}
			reducerIndex := getReduceKVResp.Index
			mapLen := getReduceKVResp.MapLen
			// read len(inputFiles) files and make values for each word;
			data := make(map[string][]string)
			for i := 0 ; i < mapLen ; i++{
				fileName := ("mr-" + string(i) + "-" + string(reducerIndex))
				addValue(data, fileName)
			}
			// each word and its values -> reducef 
			// store in one output file
			outputFileName := ("mr-out-" + string(reducerIndex))
			for key, values := range data {
				output := reducef(key, values)
				kvAppendToFile(outputFileName, key, output)
			}
			ok := call("Coordinator.FinishReduce", &finishReduceReq, &finishReduceResp)
			if ok == false {
				return
			}
			
		} else {
			time.Sleep(time.Second / 2)
		}
	}
}

func addValue(data map[string][]string, fileName string) error {
	if data == nil {
		return errors.New("data is nil")
	}
	// openning and close file
	filePath := os.Getwd() + fileName
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return errors.New(filePath + " file not exists")
	}
	file, err := os.OpenFile(filePath, os.RDONLY, 0666)
    if err != nil {
        fmt.Println("[addValue] fail to open file", err)
		return err
    }
	defer file.Close()

	// buff read
	buff := bufio.NewReader(file)
	for {
		line, _, eof := buff.ReadLine()
		if eof == io.EOF {
			break
		}
		// stringKV is of []string type, including Key, Value
		stringKV := strings.Split(string(line))
		word := stringKV[0]
		count := stringKV[1]
		if countArray, ok := data[word]; ok {
			countArray = append(countArray, count)
		} else {
			data[word] = make([]string, 1, count)
		}
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// 0 -> no need to keep alive, just quit
// 1 -> needed
func CheckStatusOfCoordinator() int {
	req := CheckStatusOfCoordinatorReq{}
	resp := CheckStatusOfCoordinatorResp{}
	ok := call("Coordinator.CheckStatus", &req, &resp)
	if ok == false {
		return 0
	}
	if resp.Status == 4 {
		return 0
	}
	return 1
}
// 0 -> no need to keep alive, just quit
// 1 -> mapper
// 2 -> reducer
// 3 -> waiting
func DecideRole() int {
	req := CheckStatusReq{}
	resp := CheckStatusResp{}
	ok := call("Coordinator.CheckStatus", &req, &resp)
	if ok == false {
		return 0
	}
	if resp.Status == 4{
		return 0
	}
	if resp.Status == 0 {
		return 1
	}
	if resp.Status == 2 {
		return 2
	}
	if resp.Status == 1 || resp.Status == 3 {
		return 3
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
// RPC maybe means: USE remote logic to change local variables
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func kvAppendToFile(fileName, key, value string) {
	file, err := os.OpenFile(os.Getwd() + fileName, os.O_APPEND|os.O_CREATE, 0666)
    if err != nil {
        fmt.Println("[mapAppendToFile] fail to open file", err)
    }
	defer file.Close()

    write := bufio.NewWriter(file)
    for i := 0; i < 5; i++ {
        write.WriteString(key + "\t" + value + "\n")
    }
    write.Flush()
}
