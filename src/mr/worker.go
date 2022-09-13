package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"

	// "sort"
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

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	//CallExample()

	for { // RPC Call and iordering
		cReply := MapReduceCall()
		task := cReply.TaskMsg // store the task message in a variable
		if cReply.FileName == "" {
			fmt.Println("Finish all the map tasks and exiting")
			os.Exit(0)
		}
		switch {
		case task == "map":
			// fmt.Printf("Filename: %v\n", cReply.FileName)
			// fmt.Printf("Task: %v\n", cReply.TaskMsg)
			fmt.Println("Call the map function and do mapping")
			MapTask(&cReply, mapf) // perform a map task

			// RPC Call set up
			// Set up the Arguments
			args := MRArgs{}
			args.FileName = cReply.FileName
			// Set up the Reply
			reply := MRReply{}
			ok := call("Coordinator.UpdateTask", &args, &reply)
			if !ok {
				log.Fatal("Fatal Coordinator Update")
			}

		case task == "reduce":
			// fmt.Printf("Filename: %v\n", cReply.FileName)
			// fmt.Printf("Task: %v\n", cReply.TaskMsg)
		default:
			log.Fatal("The task was undefined or a crash occurred")
		}
	}
}

// Function that does the mapping task
func MapTask(reply *MRReply, mapf func(string, string) []KeyValue) {
	fmt.Printf("Filename: %v\n", reply.FileName) // print the file name
	fmt.Printf("Task: %v\n", reply.TaskMsg)      // print the task

	// Do the map reduce work
	file, err := os.Open(reply.FileName)
	defer file.Close()
	if err != nil {
		log.Fatalf("cannot open %v", reply.FileName)
	}
	content, err := ioutil.ReadAll(file)
	kva := mapf(reply.FileName, string(content))

	// write to a temp file
	mapFile := "mr-intermediatory" + reply.FileName
	outfile, _ := os.Create(mapFile)
	enc := json.NewEncoder(outfile)

	for i := 0; i < len(kva); i++ {
		err = enc.Encode(&kva[i])
	}
	outfile.Close()
}

// 	call("Coordinator.UpdateTask", &args, &reply) // send file name

// mapReduce.

// map function
// map task count + 1
// return the new file name

// check if we have processed all the files for  the map task
// if fileName == "" {
// 	fmt.Println(("Finish all map tasks"))
// 	os.Exit(0) // 0 to indicate that all the map tasks work
// }

// mapTaskCount++
// _ := call()

// args.

// }

// file, err := os.Open(fileName)
// if err != nil {
// 	log.Fatalf("cannot open %v", fileName)
// }
// content, err := ioutil.ReadAll(file)
// if err != nil {
// 	log.Fatalf("cannot read %v", fileName)
// }
// file.Close()
// kva := mapf(fileName, string(content))

// // // write kva to a file to check contents
// mapFile := "mr-intermediatory-0"
// outfile, _ := os.Create(mapFile)
// enc := json.NewEncoder(outfile)

// for i := 0; i < len(kva); i++ {
// 	err = enc.Encode(&kva[i])
// }
// outfile.Close()

// // Perform the reduce
// openFile, err := os.Open(mapFile)

// dec := json.NewDecoder(openFile)
// intermediate := []KeyValue{}
// for {
//   var kv KeyValue
//   if err := dec.Decode(&kv); err != nil {
// 	break
//   }
//   intermediate = append(intermediate, kv)
// }

// sort.Sort(ByKey(intermediate))
// reduceFile := "mr-out-1"
// outfile2, _ := os.Create(reduceFile)
// i := 0
// for i < len(intermediate) {
// 	j := i + 1
// 	for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
// 		j++
// 	}
// 	values := []string{}
// 	for k := i; k < j; k++ {
// 		values = append(values, intermediate[k].Value)
// 	}
// 	output := reducef(intermediate[i].Key, values)

// 	// this is the correct format for each line of Reduce output.
// 	fmt.Fprintf(outfile2, "%v %v\n", intermediate[i].Key, output)

// 	i = j
// }

// outfile2.Close()

// fileOpen, err := os.Open(mapFile)
// if err != nil {
// 	log.Fatalf("cannot open %v", mapFile)
// }

// enc := json.NewEncoder(ofile)
// kv := kva[0]
// err = enc.Encode(&kv)
// fmt.Printf("test write to file")

// RPC Call to the Coordinator
func MapReduceCall() MRReply {
	// Set up the Arguments
	args := MRArgs{}
	// Set up the Reply
	reply := MRReply{}
	ok := call("Coordinator.AssignTask", &args, &reply)
	fmt.Printf("ok value: %v\n", ok)
	if ok {
		fmt.Printf("reply.name %v\n", reply.FileName)
		return reply
	} else {
		fmt.Printf("call failed!\n")
		return MRReply{}
	}
}

// Function that does the partition according to the map reduce paper
// func Partition(kva[]KeyValue, nReduce int)[] []KeyValue {

// }

// Function that writes to intermediate files
func WriteIntermediateFile(intermediate []KeyValue, mapTaskNum int, reduceTaskNum int) string {
	filename := "mr-" + strconv.Itoa(mapTaskNum) + "-" + strconv.Itoa(reduceTaskNum)
	ifile, _ := os.Create(filename)

	enc := json.NewEncoder(ifile)
	for _, value := range intermediate {
		err := enc.Encode(&value)
		if err != nil {
			log.Fatal("error: ", err)
		}
	}

	ifile.Close()
	return filename
}

// Function that writes to output files
// func WriteOutputFile()

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
