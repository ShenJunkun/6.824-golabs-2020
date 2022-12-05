package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Master struct {
	// Your definitions here.
	// 0：未分配 1：正在做 2：已完成
	mapTaskStatus    []int
	mapTaskTime      []time.Time
	reduceTaskStatus []int
	reduceTaskTime   []time.Time
	NMap             int
	NReduce          int
	finishAllMap     bool
	finishAllReduce  bool
	mu               sync.Mutex
	fileName         []string
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) RequestHandler(args *RequestTaskArgs, reply *RequestTaskReply) error {
	if !args.RequstTask {
		if args.TaskType == mapTask {
			m.mapTaskTime[args.TaskId] = time.Now()
		} else if args.TaskType == reduceTask {
			m.reduceTaskTime[args.TaskId] = time.Now()
		}
		reply.TaskType = isRunning
	} else {
		m.mu.Lock()
		findTask := false
		// 查找为分配的map task
		if !m.finishAllMap {
			for idx, stat := range m.mapTaskStatus {
				if stat == 0 {
					//修改task状态
					m.mapTaskStatus[idx] = 1
					m.mapTaskTime[idx] = time.Now()
					reply.TaskId = idx
					reply.TaskType = mapTask
					reply.NMap = m.NMap
					reply.NReduce = m.NReduce
					reply.FileName = append(reply.FileName, m.fileName[idx])
					findTask = true
					break
				}
			}
			if !findTask {
				reply.TaskType = isRunning
			}
		} else if !m.finishAllReduce {
			for idx, stat := range m.reduceTaskStatus {
				if stat == 0 {
					//修改task状态
					m.reduceTaskStatus[idx] = 1
					m.reduceTaskTime[idx] = time.Now()
					reply.TaskId = idx
					reply.TaskType = reduceTask
					reply.NMap = m.NMap
					reply.NReduce = m.NReduce
					findTask = true
					break
				}
			}
			if !findTask {
				reply.TaskType = isRunning
			}
		} else {
			reply.TaskType = allFinish
		}
		m.mu.Unlock()
	}
	return nil
}

func (m *Master) FinishHandler(args *FinishTaskArgs, reply *FinishTaskReply) error {
	if args.TaskType == mapTask {
		m.mapTaskStatus[args.TaskId] = 2
	} else if args.TaskType == reduceTask {
		m.reduceTaskStatus[args.TaskId] = 2
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.

func (m *Master) check() {
	for {
		m.mu.Lock()
		for idx, t := range m.mapTaskTime {
			dif := time.Since(t)
			if (dif > 10.0) && (m.mapTaskStatus[idx] == 1) {
				m.mapTaskStatus[idx] = 0
			}
		}

		allMapFin := true
		for _, stat := range m.mapTaskStatus {
			if stat != 2 {
				allMapFin = false
			}
		}

		if allMapFin {
			m.finishAllMap = true
		}

		for idx, t := range m.reduceTaskTime {
			dif := time.Since(t)
			if (dif > 10) && (m.reduceTaskStatus[idx] == 1) {
				m.reduceTaskStatus[idx] = 0
			}
		}

		allReduceFin := true
		for _, stat := range m.reduceTaskStatus {
			if stat != 2 {
				allReduceFin = false
			}
		}
		m.mu.Unlock()
		if allReduceFin {
			m.finishAllReduce = true
			break
		}
		time.Sleep(time.Millisecond * 100)
	}
}

func (m *Master) Done() bool {
	ret := false

	// Your code here.
	// m.mu.Lock()
	// for idx, t := range m.mapTaskTime {
	// 	dif := time.Since(t)
	// 	if (dif > 10) && (m.mapTaskStatus[idx] == 1) {
	// 		m.mapTaskStatus[idx] = 0
	// 	}
	// }

	// allMapFin := true
	// for _, stat := range m.mapTaskStatus {
	// 	if stat != 2 {
	// 		allMapFin = false
	// 	}
	// }

	// if allMapFin {
	// 	m.finishAllMap = true
	// }

	// for idx, t := range m.mapTaskTime {
	// 	dif := time.Since(t)
	// 	if (dif > 10) && (m.mapTaskStatus[idx] == 1) {
	// 		m.mapTaskStatus[idx] = 0
	// 	}
	// }

	// allReduceFin := true
	// for _, stat := range m.reduceTaskStatus {
	// 	if stat != 2 {
	// 		allReduceFin = false
	// 	}
	// }
	// m.mu.Unlock()
	// if allReduceFin {
	// 	m.finishAllReduce = true
	// 	ret = true
	// }

	mu.Lock()
	if m.finishAllReduce {
		ret = true
	}
	mu.Unlock()
	return ret
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.mapTaskStatus = make([]int, len(files))
	for idx, _ := range m.mapTaskStatus {
		m.mapTaskStatus[idx] = 0
	}

	m.reduceTaskStatus = make([]int, nReduce)
	for idx, _ := range m.reduceTaskStatus {
		m.reduceTaskStatus[idx] = 0
	}

	m.NMap = len(files)
	m.NReduce = nReduce
	m.finishAllMap = false
	m.finishAllReduce = false
	m.fileName = files
	m.mapTaskTime = make([]time.Time, len(files))
	m.reduceTaskTime = make([]time.Time, nReduce)

	m.server()
	go m.check()
	return &m
}
