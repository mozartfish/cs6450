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
type mrArgs struct {
	X int
}
type mrReply struct {
	FileName string 
}


// Constants for handling when program is done (Section 3.1)
// - Notify coordinator when finish map 
// - Notify coordinator when finish reduce 
// - Notify user program that map and reduce are done 
// const (
// 	MapDone = iota
// 	ReduceDone
// 	MapReduceDone
// )

// Three types of messages 
// 1. Coordinator Message 
// 2. Intermediate State Message 
// 3. Worker Message

// Coordinator Message 

// Intermediate Message
//  group together all intermediate values associated with the
//  same intermediate key I and pass to reduce function

// Worker Message 

// message needs to contain the following
// 1. File Name 
// 2. Task - map or reduce 
// 3. Status - Map, Reduce or done
// 4. 


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
