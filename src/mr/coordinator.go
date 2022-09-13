package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

// Section 3.2 state of workers
const (
	IdleState = iota
	InProgress
	Completed
)

type Coordinator struct {
	// Your definitions here.

	// Text files are input files - correspond to 1 piece in the map reduce paper
	// M files => M pieces => 8 map tasks, 10 reduce tasks () from the tests
	// files []string

	// Task variables
	TaskMsg string // the task the worker is supposed to perform

	// Map Task Variables
	MapFileStatus map[string]int // keep track of which map jobs done
	MapFinish     bool           // keep track of when all map tasks are finished

}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AssignTask(args *MRArgs, reply *MRReply) error {
	// print the arg values

	// Send the reply message
	reply.FileName = TaskManager(c) // send the file name for mapping
	reply.TaskMsg = c.TaskMsg       // send the task

	fmt.Printf("File name: %v\n", reply.FileName)
	fmt.Printf("Task: %v\n", reply.TaskMsg)
	return nil
}

func (c *Coordinator) UpdateTask(args *MRArgs, reply *MRReply) error {
	fmt.Printf("File name updated: %v\n", args.FileName)
	c.MapFileStatus[args.FileName] = Completed
	fmt.Printf("the mapped files %v\n", c.MapFileStatus)
	return nil
}

// Find a Task for the map
// returns the name of task that is incomplete
func TaskManager(c *Coordinator) string {
	// check if the all the map tasks have been finished
	mapDone := CheckMapStatus(c)
	fmt.Printf("map status %v\n", mapDone)

	// if not all the map tasks have been done then assign an incomplete file
	if !mapDone {
		// set the task value
		c.TaskMsg = "map"

		for key, value := range c.MapFileStatus {
			if value == IdleState {
				c.MapFileStatus[key] = InProgress
				return key
			}
		}
	}

	if c.MapFinish {
		c.TaskMsg = ""
		fmt.Println("Finish all the map tasks")
	}

	return ""
}

// Function that checks whether all map tasks have been completed
func CheckMapStatus(c *Coordinator) bool {
	// check if all the map status are complete
	for _, value := range c.MapFileStatus {
		if value != Completed {
			return false
		}
	}
	c.MapFinish = true
	return true
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.MapFileStatus = make(map[string]int)
	c.MapFinish = false
	c.TaskMsg = ""

	// populate files
	for _, value := range files {
		c.MapFileStatus[value] = IdleState
	}

	c.server()
	return &c
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}
