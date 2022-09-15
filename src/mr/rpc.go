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
type TaskArgs struct {
}

type TaskReply struct {
	Task         string // task assigned by coordinator
	FileName     string // file name for mapping
	Reducer      int // reducer number for reducing
	MapTaskID    int    // map task ID
	ReduceTaskID int    // reduce task ID
	NMap         int    // number of mappers
	NReduce      int    // number of reducers
}

type MapTaskCompletedArgs struct {
	MapTaskID int
}

type MapTaskCompletedReply struct {
}

type ReduceTaskCompletedArgs struct {
	ReduceTaskID int
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
