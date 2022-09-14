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

// Add your RPC definitions here.
// The task requested by the worker
type TaskArgs struct {
}

// The information the coordinator sends to the worker
type TaskReply struct {
	Task        string // the task of the worker
	FileName    string // the name of the file name for the map task
	MapCount    int    // the map task count
	ReduceCount int    // the reduce task count
	Reducer int // the reducer to apply to a map task 
}

// Tasks Completed
type MapTaskCompletedArgs struct {
	FileName string // name of the file that completed its map
}

type MapTaskCompletedReply struct {
}

type ReduceTaskCompletedArgs struct {
	Reducer int // name of the reducer that completed its reduce
}

type ReduceTaskCompletedReply struct {
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
