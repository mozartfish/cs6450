package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strconv"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		// Your worker implementation here.
		taskReply := RequestTask()
		task := taskReply.Task // get the task specified by the coordinator
		switch {
		case task == "map":
			fmt.Println("Perform a Map Task")
			// fmt.Printf("FileName: %v\n", taskReply.FileName)
			fileName := taskReply.FileName
			// fmt.Printf("Map Count: %v\n", taskReply.MapCount)
			mapCount := taskReply.MapCount
			Map(fileName, mapCount, mapf)
			CompleteMapTask(fileName)
		case task == "reduce":
			fmt.Println("Perform a Reduce Task")
		default:
			log.Fatal("The Message was not received and failed!\n")
		}
	}
	// perform either a map or reduce task depending on the task sent by the coordinator

	// fmt.Printf("Task: %v\n", taskReply.Task)

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func Map(filename string, MapCount int, mapf func(string, string) []KeyValue) {
	file, err := os.Open(filename)
	defer file.Close()
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	kva := mapf(filename, string(content)) // apply map function to data

	// Write to Temp File
	mapfile := "mr-" + strconv.Itoa(MapCount)
	outfile, _ := os.Create(mapfile)
	defer outfile.Close()
	enc := json.NewEncoder(outfile)

	for i := 0; i < len(kva); i++ {
		err = enc.Encode(&kva[i])
	}
	outfile.Close()
}

func Reduce(reducef func(string, []string) string) {

}

// RPC Call to the coordinator requesting a task
func RequestTask() TaskReply {
	// Arguments
	args := TaskArgs{}

	// Reply
	reply := TaskReply{}

	// Send RPC Request, wait for reply
	ok := call("Coordinator.AssignTask", &args, &reply)
	if !ok {
		fmt.Printf("call failed!\n")
		return TaskReply{}
	} else {
		fmt.Printf("reply.name %v\n", reply.FileName)
		return reply
	}

}

// RPC Call to the coordinator notifying that map task has been finished
func CompleteMapTask(FileName string) {
	// fmt.Println("Complete Map Task RPC function")
	// Arguments
	args := MapTaskCompletedArgs{}
	args.FileName = FileName
	// // Reply
	reply := MapTaskCompletedReply{}
	ok := call("Coordinator.UpdateMap", &args, &reply)
	if !ok {
		fmt.Printf("Complete Task Call failed!\n")
	}

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
