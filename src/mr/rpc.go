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
// The task requested by the worker
type TaskArgs struct {

}

// The information the coordinator sends to the worker
type TaskReply struct {
	Task string // the task of the worker
	FileName string // the name of the task requested by the worker
	MapCount int // the map task count 
}

// // Task Completed
// type CompletedArgs struct {

// }






// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
