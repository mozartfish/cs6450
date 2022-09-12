package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

// Section 3.2 state of workers
const (
	idleState = iota
	inProgressState
	completedState
)

type Coordinator struct {
	// Your definitions here.

	// Text files are input files - correspond to 1 piece in the map reduce paper
	// M files => M pieces => 8 map tasks, 10 reduce tasks () from the tests
	files []string

	// // file to task status
	// splits map[string]int

	// // Number of map tasks
	// nMap int

	// // Number of reduce tasks
	// nReduce int

	// // Coordinator lock
	// cMutex sync.Mutex

	// // Map Done
	// mapFinish bool

	// // Reduce Done
	// reduceFinish bool

}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) AssignTask(args *mrArgs, reply *mrReply) error {
	reply.FileName = c.files[0]
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
	c.files = files

	c.server()
	return &c
}
