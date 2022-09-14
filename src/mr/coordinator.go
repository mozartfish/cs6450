package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"fmt"
)

const (
	IdleState = iota
	InProgress
	Completed
)

type Coordinator struct {
	// Your definitions here.

	// Map Variables
	nMap int // counter for the number of map tasks
	// mapFinish bool           // keep track of when map tasks are finished
	mapStatus map[string]int // keep track of the map task state

	// Reduce Variables
	nReduce int // counter for the number of reduce tasks
	// reduceFinish bool        // keep track of when reduce tasks are finished
	reduceStatus map[int]int // keep track of the state of reduce tasks

	// Other Variables
	task string // task sent from coordinator to worker (map or reduce)
}

// Your code here -- RPC handlers for the worker to call.

// RPC Handler to request task
func (c *Coordinator) AssignTask(args *TaskArgs, reply *TaskReply) error {
	// check if there are any map tasks that have yet to be completed
	mapComplete := false
	for !mapComplete {
		fmt.Println("Add a map task")
		fileName := GetMapTask(c)
		c.mapStatus[fileName] = InProgress
		reply.Task = "map"
		reply.FileName = fileName
		reply.MapCount = c.nMap
		mapComplete = CheckMapStatus(c)
	}
	// check if there are any reduce tasks that have yet to be completed
	return nil

}

// Get an Incomplete Map Task
func GetMapTask(c *Coordinator) string {
	for key, value := range c.mapStatus {
		if value == IdleState {
			return key
		}
	}
	return ""
}

// Check if all Map tasks are completed
func CheckMapStatus(c *Coordinator) bool {
	for _, value := range c.mapStatus {
		if value == IdleState {
			return false
		}
	}
	return true
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
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

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.nMap = 0 // initialize the map tasks since we don't know how many
	// c.mapFinish = false
	c.mapStatus = make(map[string]int)
	c.nReduce = nReduce
	// c.reduceFinish = false
	c.reduceStatus = make(map[int]int)
	c.task = ""

	// Initialize the state of the map tasks
	for i := 0; i < len(files); i++ {
		fileName := files[i]
		c.mapStatus[fileName] = IdleState
	}

	// Initialize the state of reduce tasks
	for j := 0; j < c.nReduce; j++ {
		c.reduceStatus[j] = IdleState
	}

	c.server()
	return &c
}
