package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

const (
	IdleState = iota
	InProgress
	Completed
)

type Coordinator struct {
	// Your definitions here.

	fileNames    []string // store all file names
	mapStatus    []int    // store all the map states
	reduceStatus []int    // store all the reduce states
	nReduce      int      // number of reducers
	nMap         int      // number of mappers
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// RPC Handler to assign tasks
func (c *Coordinator) AssignTask(args *TaskArgs, reply *TaskReply) error {
	// check if there is a map task to finish
	// c.mu.Lock()
	// defer c.mu.Unlock()
	fmt.Println("Assign a Task")
	fmt.Printf("file Names :%v\n", c.fileNames)
	fmt.Printf("map status :%v\n", c.mapStatus)
	fmt.Printf("num mappers :%v\n", c.nMap)
	fmt.Printf("num reducers :%v\n", c.nReduce)
	mapDone := CheckMapStatus(c)
	// reduceDone := CheckReduceStatus(c)
	if !mapDone {
		fmt.Println("Assign a map task")
		mapTaskID := GetMapTask(c)
		fmt.Printf("New map task ID: %v\n", mapTaskID)
		c.mapStatus[mapTaskID] = InProgress
		reply.Task = "map"
		reply.MapTaskID = mapTaskID
		reply.FileName = c.fileNames[mapTaskID]
		reply.NReduce = c.nReduce
		reply.NMap = c.nMap
		fmt.Printf("reply name :%v\n", reply.FileName)
		fmt.Printf("mapped status:%v\n", c.mapStatus)
		fmt.Printf("num mappers :%v\n", reply.NMap)
		fmt.Printf("num reducers :%v\n", reply.NReduce)
		fmt.Printf("reply map task ID: %v\n", reply.MapTaskID)
	} else {
		reply.Task = "mapreduce"
	}
	return nil
}

// RPC Handler to notify coordinator map tasks finished
func (c *Coordinator) UpdateMapStatus(args *MapTaskCompletedArgs, reply *MapTaskCompletedReply) error {
	// c.mu.Lock()
	// defer c.mu.Unlock()
	fmt.Println("Update the map status")
	fmt.Printf("Current Map Status: %v\n", c.mapStatus)
	mapTaskID := args.MapTaskID
	c.mapStatus[mapTaskID] = Completed
	fmt.Printf("New Map Status: %v\n", c.mapStatus)
	return nil
}

// Get a Map Task
func GetMapTask(c *Coordinator) int {
	fmt.Println("Get a Map Task")
	for index, value := range c.mapStatus {
		if value == IdleState {
			return index
		}
	}
	return -1
}

// Check Map Tasks Completed
func CheckMapStatus(c *Coordinator) bool {
	fmt.Println("Check Map Tasks")
	fmt.Printf("Current Map Status: %v\n", c.mapStatus)
	// c.mu.Lock()
	// defer c.mu.Unlock()
	for _, value := range c.mapStatus {
		if value != Completed {
			return false
		}
	}
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

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	fmt.Println("Create a Coordinator")
	// files represent the number of map tasks
	c.fileNames = make([]string, len(files))
	c.mapStatus = make([]int, len(files))
	c.reduceStatus = make([]int, nReduce)
	c.nReduce = nReduce
	c.nMap = len(files)

	// Update Map tasks and files
	for index, value := range files {
		c.mapStatus[index] = IdleState
		c.fileNames[index] = value
	}

	fmt.Printf("Coordinator files: %v\n", c.fileNames)
	fmt.Printf("Coordinator map status: %v\n", c.mapStatus)
	fmt.Printf("Coordinator reduce status: %v\n", c.reduceStatus)
	fmt.Printf("Coordinator nMap: %v\n", c.nMap)
	fmt.Printf("Coordinator nReduce: %v\n", c.nReduce)

	// Your code here.

	c.server()
	return &c
}
