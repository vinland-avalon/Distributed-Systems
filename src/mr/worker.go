package mr

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"syscall"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// KeyValue
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

// Worker
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
		if res == ROLE_QUIT {
			return
		} else if res == ROLE_MAPPER {
			//3. if mapper:
			// rpc call, return corresponding task (key, also means filename);
			log.Println("Now as a mapper")
			getMapKVReq := GetMapKVReq{}
			getMapKVResp := GetMapKVResp{}
			ok := call("Coordinator.GetMapKV", &getMapKVReq, &getMapKVResp)
			log.Printf("[Worker] call GetMapKV rpc, req:%+v, resp:%+v", getMapKVReq, getMapKVResp)
			if ok == false {
				return
			}
			if getMapKVResp.Need == false {
				log.Println("[Worker] no more need to be mapper, just sleep 0.5s")
				time.Sleep(time.Second / 2)
				continue
			}
			mapperIndex := getMapKVResp.Index
			key := getMapKVResp.Key
			// read the file to get value (file contents)
			value, err := readMapValueByKeyFromFile(key)
			if err != nil {
				log.Printf(err.Error())
				return
			}
			// mapf;
			kva := mapf(key, value)
			// store the mapresult, using ihash to decide file-name
			for _, kv := range kva {
				fileName := fmt.Sprintf("mr-%v-%v", mapperIndex, (ihash(kv.Key))%10)
				//if i%200 == 0 {
				//	log.Printf("[Worker] after mapf, store kv:%v in file:%v", kv, fileName)
				//}
				err := KvAppendToFile(fileName, kv.Key, kv.Value)
				if err != nil {
					log.Println(err.Error())
					return
				}
			}
			// rpc return res
			finishMapReq := FinishMapReq{
				Index: mapperIndex,
			}
			finishMapResp := FinishMapResp{}
			log.Println("[Worker] call finishMap rpc")
			ok = call("Coordinator.FinishMap", &finishMapReq, &finishMapResp)
			if ok == false {
				return
			}
		} else if res == ROLE_REDUCER {
			//3. if reducer:
			log.Println("[Worker] now as a reducer")
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
			log.Printf("[Worker] call GetReduceKV rpc, resp: %v\n", getReduceKVResp)
			data := make(map[string][]string)
			for i := 0; i < mapLen; i++ {
				fileName := fmt.Sprintf("mr-%v-%v", i, reducerIndex)
				err := addValue(data, fileName)
				if err != nil {
					log.Println(err.Error())
					return
				}
			}
			// each word and its values -> reducef
			// store in one output file
			outputFileName := fmt.Sprintf("mr-out-%v", reducerIndex)
			log.Printf("[Worker] outputFile: %v\n", outputFileName)
			for key, values := range data {
				output := reducef(key, values)
				err := KvAppendToFile(outputFileName, key, output)
				if err != nil {
					log.Printf(err.Error())
					return
				}
			}
			finishReduceReq := FinishReduceReq{
				Index: reducerIndex,
			}
			finishReduceResp := FinishReduceResp{}
			ok = call("Coordinator.FinishReduce", &finishReduceReq, &finishReduceResp)
			if ok == false {
				return
			}
		} else {
			time.Sleep(time.Second / 2)
		}
	}
}

func readMapValueByKeyFromFile(filename string) (string, error) {
	file, err := os.Open(filename)
	if err != nil {
		// log.Fatalf("cannot open %v", filename)
		err := fmt.Errorf("[readMapValueByKeyFromFile] cannot open file : %v", filename)
		return "", err
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		// log.Fatalf("cannot read %v", filename)
		err := fmt.Errorf("[readMapValueByKeyFromFile] ioutil.ReadAll err : %v", filename)
		return "", err
	}
	file.Close()
	return string(content), nil
}

func addValue(data map[string][]string, fileName string) error {
	if data == nil {
		return errors.New("data is nil")
	}
	// openning and close file
	if _, err := os.Stat(fileName); os.IsNotExist(err) {
		return errors.New(fileName + " file not exists")
	}
	file, err := os.OpenFile(fileName, os.O_RDONLY, 0666)
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
		stringKV := strings.Split(string(line), "\t")
		word := stringKV[0]
		count := stringKV[1]
		if _, ok := data[word]; ok {
			data[word] = append(data[word], count)
			// log.Printf("[addValue] word: %v, countArray: %v", word, len(data[word]))
		} else {
			data[word] = make([]string, 0)
			data[word] = append(data[word], count)
		}
	}
	return nil
}

// CallExample
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
//func CheckStatusOfCoordinator() int {
//	req := CheckStatusOfCoordinatorReq{}
//	resp := CheckStatusOfCoordinatorResp{}
//	ok := call("Coordinator.CheckStatus", &req, &resp)
//	if ok == false {
//		return 0
//	}
//	if resp.Status == 4 {
//		return 0
//	}
//	return 1
//}

// DecideRole 0 -> no need to keep alive, just quit
// 1 -> mapper
// 2 -> reducer
// 3 -> waiting
func DecideRole() int {
	req := CheckStatusReq{}
	resp := CheckStatusResp{}
	ok := call("Coordinator.CheckStatus", &req, &resp)
	if ok == false {
		return ROLE_QUIT
	}
	if resp.Status == STATUS_FINISHED {
		return ROLE_QUIT
	}
	if resp.Status == STATUS_MAPPER_NEEDED {
		return ROLE_MAPPER
	}
	if resp.Status == STATUS_REDUCER_NEEDED {
		return ROLE_REDUCER
	}
	if resp.Status == STATUS_MAPPER_PROCESS || resp.Status == STATUS_REDUCER_PROCESS {
		return ROLE_WAITING
	}
	return ROLE_QUIT
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

func KvAppendToFile(fileName, key, value string) error {
	file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|syscall.O_WRONLY, 0666)
	if err != nil {
		fmt.Printf("[KvAppendToFile] fail to open file: %v\n", err)
		err = fmt.Errorf("[KvAppendToFile] fail to open file: %v\n", err)
		return err
	}
	defer file.Close()

	write := bufio.NewWriter(file)
	write.WriteString(key + "\t" + value + "\n")
	write.Flush()
	return nil
}
