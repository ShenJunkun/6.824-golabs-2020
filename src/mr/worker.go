package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"sync"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

var (
	mu           sync.Mutex
	workerStatus bool
)

var taskId int
var taskType int

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func doMapTask(task RequestTaskReply, mapf func(string, string) []KeyValue) {
	//start task
	mu.Lock()
	workerStatus = true
	mu.Unlock()

	filename := task.FileName[0]
	//read file
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	nreduce := task.NReduce
	res := make([][]KeyValue, nreduce)
	for i := 0; i < len(kva); i++ {
		idx := ihash(kva[i].Key) % nreduce
		res[idx] = append(res[idx], kva[i])
	}

	mapId := task.TaskId

	for idx, mapData := range res {
		outPutFile := fmt.Sprintf("mr-%d-%d", mapId, idx)
		file, _ := os.Create(outPutFile)
		enc := json.NewEncoder(file)

		for _, tmp := range mapData {
			err := enc.Encode(&tmp)
			if err != nil {
				fmt.Println("error")
			}
		}
		file.Close()
	}
	//finish task
	finishTask(task.TaskId, task.TaskType)
	mu.Lock()
	workerStatus = false
	mu.Unlock()

}

func doReduceTask(task RequestTaskReply, reducef func(string, []string) string) {
	//start task
	mu.Lock()
	workerStatus = true
	mu.Unlock()

	NMap := task.NMap
	var res []KeyValue
	for i := 0; i < NMap; i++ {
		inputFile := fmt.Sprintf("mr-%d-%d", i, task.TaskId)
		file, _ := os.Open(inputFile)
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			res = append(res, kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(res))

	oname := fmt.Sprintf("mr-out-%d", task.TaskId)
	ofile, _ := os.Create(oname)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(res) {
		j := i + 1
		for j < len(res) && res[j].Key == res[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, res[k].Value)
		}
		output := reducef(res[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", res[i].Key, output)

		i = j
	}

	ofile.Close()
	finishTask(task.TaskId, task.TaskType)
	//finish task

	mu.Lock()
	workerStatus = false
	mu.Unlock()

}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()

	for {
		reply := requestTask()

		if reply.TaskType == allFinish {
			break
		} else if reply.TaskType == mapTask {
			taskId = reply.TaskId
			taskType = reply.TaskType
			doMapTask(reply, mapf)
		} else if reply.TaskType == reduceTask {
			taskId = reply.TaskId
			taskType = reply.TaskType
			doReduceTask(reply, reducef)
		} else if reply.TaskType == isRunning {
			time.Sleep(time.Millisecond * 500)
		}

	}

}

func requestTask() RequestTaskReply {
	args := RequestTaskArgs{}
	mu.Lock()

	if workerStatus {
		args.RequstTask = false
		args.TaskId = taskId
		args.TaskType = taskType
	} else {
		args.RequstTask = true
		// args.TaskId = taskId
		// args.TaskType = taskType
	}
	mu.Unlock()
	reply := RequestTaskReply{}

	call("Master.RequestHandler", &args, &reply)

	return reply

}

func finishTask(taskId int, taskType int) {
	args := FinishTaskArgs{}
	args.TaskId = taskId
	args.TaskType = taskType

	reply := FinishTaskReply{}

	call("Master.FinishHandler", &args, &reply)

}

// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
