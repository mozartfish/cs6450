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

// Worker State as described in Map Reduce Paper
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
	mapCount     int         // keep track of all map tasks
	mapFinish    bool        // keep track of map done status
	reduceStatus []int       // store all the reduce states
	reduceTime   []time.Time // store all reduce times
	reduceCount  int         // keep track of all reduce
	reduceFinish bool        // keep track of reduce finish
	nReduce      int         // number of reducers
	nMap         int         // number of mappers
	mu           sync.Mutex // mutex for locking critical sections
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
	// fmt.Printf("file Names :%v\n", c.fileNames)
	fmt.Printf("map status :%v\n", c.mapStatus)
	fmt.Printf("reduce status :%v\n", c.reduceStatus)
	// fmt.Printf("num mappers :%v\n", c.nMap)
	// fmt.Printf("num reducers :%v\n", c.nReduce)
	if !c.mapFinish {
		fmt.Println("Assign a map task")
		fmt.Println("Get a Map Task")
		for i := 0; i < c.nMap; i++ {
			if c.mapStatus[i] == IdleState {
				c.mapStatus[i] = InProgress
				c.mapTime[i] = time.Now()
				reply.MapTaskID = i
				reply.Task = "map"
				reply.FileName = c.fileNames[i]
				reply.NReduce = c.nReduce
				reply.NMap = c.nMap
				reply.ReduceFinish = c.reduceFinish
				fmt.Printf("reply name :%v\n", reply.FileName)
				fmt.Printf("mapped status:%v\n", c.mapStatus)
				fmt.Printf("num mappers :%v\n", reply.NMap)
				fmt.Printf("num reducers :%v\n", reply.NReduce)
				fmt.Printf("reply map task ID: %v\n", reply.MapTaskID)
				return nil
			}
			if c.mapStatus[i] == InProgress && time.Since(c.mapTime[i]) > 10*time.Second {
				c.mapStatus[i] = InProgress
				c.mapTime[i] = time.Now()
				reply.MapTaskID = i
				reply.Task = "map"
				reply.FileName = c.fileNames[i]
				reply.NReduce = c.nReduce
				reply.NMap = c.nMap
				reply.ReduceFinish = c.reduceFinish
				fmt.Printf("reply name :%v\n", reply.FileName)
				fmt.Printf("mapped status:%v\n", c.mapStatus)
				fmt.Printf("num mappers :%v\n", reply.NMap)
				fmt.Printf("num reducers :%v\n", reply.NReduce)
				fmt.Printf("reply map task ID: %v\n", reply.MapTaskID)
				return nil
			}
		}
	} else if !c.reduceFinish {
		fmt.Println("Assign a reduce task")
		fmt.Println("Get a Reduce Task")
		for j := 0; j < c.nReduce; j++ {
			if c.reduceStatus[j] == IdleState {
				c.reduceStatus[j] = InProgress
				c.reduceTime[j] = time.Now()
				reply.Task = "reduce"
				reply.ReduceTaskID = j
				reply.Reducer = j
				reply.NReduce = c.nReduce
				reply.NMap = c.nMap
				reply.ReduceFinish = c.reduceFinish
				fmt.Printf("reply reducer: %v\n", reply.Reducer)
				fmt.Printf("reducer status: %v\n", c.reduceStatus)
				fmt.Printf("num mappers: %v\n", reply.NMap)
				fmt.Printf("num reducers: %v\n", reply.NReduce)
				fmt.Printf("reply reduce task ID: %v\n", reply.ReduceTaskID)
				return nil
			}
			if c.reduceStatus[j] == InProgress && time.Since(c.reduceTime[j]) > 10*time.Second {
				c.reduceStatus[j] = InProgress
				c.reduceTime[j] = time.Now()
				reply.Task = "reduce"
				reply.ReduceTaskID = j
				reply.Reducer = j
				reply.NReduce = c.nReduce
				reply.NMap = c.nMap
				reply.ReduceFinish = c.reduceFinish
				fmt.Printf("reply reducer: %v\n", reply.Reducer)
				fmt.Printf("reducer status: %v\n", c.reduceStatus)
				fmt.Printf("num mappers: %v\n", reply.NMap)
				fmt.Printf("num reducers: %v\n", reply.NReduce)
				fmt.Printf("reply reduce task ID: %v\n", reply.ReduceTaskID)
				return nil
			}
		}
	}
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
	c.mapCount++
	if c.mapCount == c.nMap {
		c.mapFinish = true
	}
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
	c.reduceCount++
	fmt.Printf("New Reduce Status: %v\n", c.reduceStatus)
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
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.reduceCount == c.nReduce {
		c.reduceFinish = true
	}
	ret := c.reduceFinish

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
	c.mapTime = make([]time.Time, len(files))
	c.reduceStatus = make([]int, nReduce)
	c.reduceTime = make([]time.Time, nReduce)
	c.nReduce = nReduce
	c.nMap = len(files)
	c.mapCount = 0
	c.reduceCount = 0

	// Update Map tasks and files
	for index, value := range files {
		c.mapStatus[index] = IdleState
		c.fileNames[index] = value
	}
	for i := 0; i < nReduce; i++ {
		c.reduceStatus[i] = IdleState
	}

	// fmt.Printf("Coordinator files: %v\n", c.fileNames)
	// fmt.Printf("Coordinator map status: %v\n", c.mapStatus)
	// fmt.Printf("Coordinator reduce status: %v\n", c.reduceStatus)
	// fmt.Printf("Coordinator nMap: %v\n", c.nMap)
	// fmt.Printf("Coordinator nReduce: %v\n", c.nReduce)

	// Your code here.

	c.server()
	return &c
}
