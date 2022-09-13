package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	// "decoding/json"
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

	fileName := mapReduce()

	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}
	file.Close()
	kva := mapf(fileName, string(content))

	// // write kva to a file to check contents
	mapFile := "mr-intermediatory-0"
	outfile, _ := os.Create(mapFile)
	enc := json.NewEncoder(outfile)

	for i := 0; i < len(kva); i++ {
		err = enc.Encode(&kva[i])
	}
	outfile.Close()

	// Perform the reduce
	openFile, err := os.Open(mapFile)

	dec := json.NewDecoder(openFile)
	intermediate := []KeyValue{}
	for {
	  var kv KeyValue
	  if err := dec.Decode(&kv); err != nil {
		break
	  }
	  intermediate = append(intermediate, kv)
	}

	sort.Sort(ByKey(intermediate))
	reduceFile := "mr-out-1"
	outfile2, _ := os.Create(reduceFile)
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(outfile2, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	outfile2.Close()


	// fileOpen, err := os.Open(mapFile)
	// if err != nil {
	// 	log.Fatalf("cannot open %v", mapFile)
	// }

	// enc := json.NewEncoder(ofile)
	// kv := kva[0]
	// err = enc.Encode(&kv)
	// fmt.Printf("test write to file")

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

func mapReduce() string {
	args := MRArgs{}
	args.X = 10
	reply := MRReply{}
	ok := call("Coordinator.AssignTask", &args, &reply)
	if ok {
		fmt.Printf("reply.name %v\n", reply.FileName)
		return reply.FileName
	} else {
		fmt.Printf("call failed!\n")
		return ""
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
