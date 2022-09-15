package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	IdleState = iota
	InProgress
	Completed
)

type Coordinator struct {
	// Your definitions here.

	fileNames    []string    // store all file names
	mapStatus    []int       // store all the map states
	mapTime      []time.Time // store all map times
	reduceStatus []int       // store all the reduce states
	reduceTime   []time.Time // store all reduce times
	nReduce      int         // number of reducers
	nMap         int         // number of mappers
	mu           sync.Mutex
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
	c.mu.Lock()
	defer c.mu.Unlock()
	fmt.Println("Assign a Task")
	fmt.Printf("file Names :%v\n", c.fileNames)
	fmt.Printf("map status :%v\n", c.mapStatus)
	fmt.Printf("reduce status :%v\n", c.reduceStatus)
	fmt.Printf("num mappers :%v\n", c.nMap)
	fmt.Printf("num reducers :%v\n", c.nReduce)
	mapDone := CheckMapStatus(c)
	reduceDone := CheckReduceStatus(c)
	if !mapDone {
		fmt.Println("Assign a map task")
		mapTaskID := GetMapTask(c)
		fmt.Printf("New map task ID: %v\n", mapTaskID)
		c.mapStatus[mapTaskID] = InProgress
		c.mapTime[mapTaskID] = time.Now()
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
	}
	if mapDone && !reduceDone {
		fmt.Println("Assign a reduce task")
		reduceTaskID := GetReduceTask(c)
		fmt.Printf("New reduce task ID: %v\n", reduceTaskID)
		c.reduceStatus[reduceTaskID] = InProgress
		c.reduceTime[reduceTaskID] = time.Now()
		reply.Task = "reduce"
		reply.ReduceTaskID = reduceTaskID
		reply.Reducer = reduceTaskID
		reply.NReduce = c.nReduce
		reply.NMap = c.nMap
		fmt.Printf("reply reducer: %v\n", reply.Reducer)
		fmt.Printf("reducer status: %v\n", c.reduceStatus)
		fmt.Printf("num mappers: %v\n", reply.NMap)
		fmt.Printf("num reducers: %v\n", reply.NReduce)
		fmt.Printf("reply reduce task ID: %v\n", reply.ReduceTaskID)
	}
	// fmt.Println("Completed All Map and Reduce Tasks")
	// reply.Task = "mapreduce"
	return nil
}

// RPC Handler to notify coordinator map tasks finished
func (c *Coordinator) UpdateMapStatus(args *MapTaskCompletedArgs, reply *MapTaskCompletedReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	fmt.Println("Update the map status")
	fmt.Printf("Current Map Status: %v\n", c.mapStatus)
	mapTaskID := args.MapTaskID
	c.mapStatus[mapTaskID] = Completed
	fmt.Printf("New Map Status: %v\n", c.mapStatus)
	return nil
}

// RPC Handler to notify coordinator reduce tasks finished
func (c *Coordinator) UpdateReduceStatus(args *ReduceTaskCompletedArgs, reply *ReduceTaskCompletedReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	fmt.Println("Update the reduce status")
	fmt.Printf("Current Reduce Status: %v\n", c.reduceStatus)
	reduceTaskID := args.ReduceTaskID
	c.reduceStatus[reduceTaskID] = Completed
	fmt.Printf("New Reduce Status: %v\n", c.reduceStatus)
	return nil
}

// Get a Map Task
func GetMapTask(c *Coordinator) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	fmt.Println("Get a Map Task")
	for index, value := range c.mapStatus {
		if c.mapStatus[index] == InProgress && time.Since(c.mapTime[index]) > 10*time.Second {
			return index
		}
		if value == IdleState {
			return index
		}
	}
	return -1
}

// Get a Reduce Task
func GetReduceTask(c *Coordinator) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	fmt.Println("Get a Reduce Task")
	for index, value := range c.reduceStatus {
		if c.reduceStatus[index] == InProgress && time.Since(c.reduceTime[index]) > 10*time.Second {
			return index
		}
		if value == IdleState {
			return index
		}
	}
	return -1
}

// Check Map Tasks Completed
func CheckMapStatus(c *Coordinator) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	fmt.Println("Check Map Tasks")
	fmt.Printf("Current Map Status: %v\n", c.mapStatus)
	for _, value := range c.mapStatus {
		if value != Completed {
			return false
		}
	}
	return true
}

// Check Reduce Tasks Completed
func CheckReduceStatus(c *Coordinator) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	fmt.Println("Check Reduce Tasks")
	fmt.Printf("Current Reduce Status: %v\n", c.reduceStatus)
	for _, value := range c.reduceStatus {
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
	c.mu.Lock()
	defer c.mu.Unlock()
	ret := CheckReduceStatus(c)
	// ret := false

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
	for i := 0; i < nReduce; i++ {
		c.reduceStatus[i] = IdleState
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
