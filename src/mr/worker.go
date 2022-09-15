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

	// Your worker implementation here.
	for {
		taskReply := RequestTask()
		// fmt.Printf("task reply struct %v\n", taskReply)
		task := taskReply.Task
		switch {
		case task == "map":
			fmt.Printf("Task: %v\n", taskReply.Task)
			fmt.Printf("File Name: %v\n", taskReply.FileName)
			filename := taskReply.FileName
			fmt.Printf("Map Task ID: %v\n", taskReply.MapTaskID)
			mapTaskID := taskReply.MapTaskID
			// fmt.Printf("Map Count: %v\n", taskReply.NMap)
			// nMap := taskReply.NMap
			fmt.Printf("Reduce Count %v\n", taskReply.NReduce)
			nReduce := taskReply.NReduce
			Map(filename, mapTaskID, nReduce, mapf)
			MapTaskCompleted(mapTaskID)
		case task == "mapReduce":
			fmt.Println("Finish all the map and reduce tasks")
			os.Exit(0)
		default:
			log.Fatalf("Task Request failed!\n")
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}
func Map(filename string, nMap, nReduce int, mapf func(string, string) []KeyValue) {
	fmt.Println("Map Function")
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("Cannot open file %v\n", file)
	}
	defer file.Close()
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	kva := mapf(filename, string(content)) // apply map function to data
	// fmt.Printf("KVA output: %v\n", kva)
	// call the partition function and write to temp file
	Partition(kva, nMap, nReduce)
}

// Function for mapping key values pairs to an index
func Partition(kva []KeyValue, nMap int, nReduce int) {
	fmt.Println("Partition Function")
	// create some buckets for the reduce
	iMap := make([][]KeyValue, nReduce)

	// fmt.Printf("length of kva: %v\n", len(kva))
	// fmt.Printf("reduce buckets: %v\n", iMap)

	for i := 0; i < len(kva); i++ {
		keyVal := kva[i]
		pIndex := ihash(keyVal.Key) % nReduce
		// fmt.Printf("reduce bucket: %v\n", pIndex)
		iMap[pIndex] = append(iMap[pIndex], keyVal)
		// fmt.Printf("values at current reduce bucktet: %v\n", iMap[pIndex])
	}

	// Create intermediary files for each reduce function
	for j := 0; j < nReduce; j++ {
		filename := "mr-" + strconv.Itoa(nMap) + "-" + strconv.Itoa(j)
		outfile, _ := os.Create(filename)
		defer outfile.Close()
		enc := json.NewEncoder(outfile)
		hkva := iMap[j] // the key values in a particular reduce bucket
		for k := 0; k < len(hkva); k++ {
			err := enc.Encode(&hkva[k])
			if err != nil {
				log.Fatalf("JSON encoding error: %v\n", err)
			}
		}
	}
}

// RPC Call to the coordinator requesting a task
func RequestTask() TaskReply {
	fmt.Println("Request a Task")
	// Arguments
	args := TaskArgs{}
	// Reply
	reply := TaskReply{}

	// Send RPC Request, wait for reply
	ok := call("Coordinator.AssignTask", &args, &reply)
	if !ok {
		log.Fatalf("Assign Task Call Failed!\n")
		return TaskReply{}
	} else {
		return reply
	}
}

// RPC Call to the coordinator notifying map task has completed
func MapTaskCompleted(MapTaskID int) {
	fmt.Println("Map Task Completed")
	fmt.Printf("Map Task ID: %v\n", MapTaskID)
	// Arguments
	args := MapTaskCompletedArgs{}
	args.MapTaskID = MapTaskID
	// Reply
	reply := MapTaskCompletedReply{}
	ok := call("Coordinator.UpdateMapStatus", &args, &reply)
	if !ok {
		log.Fatalf("Map Task Completed Reply Failed!\n")
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
