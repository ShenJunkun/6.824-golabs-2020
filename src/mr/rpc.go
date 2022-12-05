package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

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

const (
	allFinish = iota
	mapTask
	reduceTask
	isRunning
	wait
)

type RequestTaskArgs struct {
	TaskId int
	//1 map 2 reduce
	TaskType   int
	RequstTask bool
}

type RequestTaskReply struct {
	TaskType int
	TaskId   int
	FileName []string
	NReduce  int
	NMap     int
}

type FinishTaskArgs struct {
	TaskType int
	TaskId   int
}

type FinishTaskReply struct {
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
