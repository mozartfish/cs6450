package mr

import (
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

	// Map Variables
	nMap      int            // counter for the number of map tasks
	mapFinish bool           // keep track of when map tasks are finished
	mapState  map[string]int // keep track of the map task state

	// Reduce Variables
	nReduce      int         // counter for the number of reduce tasks
	reduceFinish bool        // keep track of when reduce tasks are finished
	reduceState  map[int]int // keep track of the state of reduce tasks

	// Other Variables
	task string // task sent from coordinator to worker (map or reduce)
}

// Your code here -- RPC handlers for the worker to call.

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
	c.mapFinish = false
	c.mapState = make(map[string]int)
	c.nReduce = nReduce
	c.reduceFinish = false
	c.reduceState = make(map[int]int)
	c.task = ""

	// Initialize the state of the map tasks
	for i := 0; i < len(files); i++ {
		fileName := files[i]
		c.mapState[fileName] = IdleState
	}

	// Initialize the state of reduce tasks
	for j := 0; j < c.nReduce; j++ {
		c.reduceState[j] = IdleState
	}

	c.server()
	return &c
}
