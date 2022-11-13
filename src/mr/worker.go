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
	var taskReply = TaskReply{}
	// Your worker implementation here.
	for !taskReply.ReduceFinish {
		taskReply = RequestTask()
		task := taskReply.Task
		switch {
		case task == "map":
			filename := taskReply.FileName
			mapTaskID := taskReply.MapTaskID
			nReduce := taskReply.NReduce
			Map(filename, mapTaskID, nReduce, mapf)
			MapTaskCompleted(mapTaskID)
		case task == "reduce":
			reducer := taskReply.Reducer
			reduceTaskID := taskReply.ReduceTaskID
			nMap := taskReply.NMap

			// combine all the files into an array corresponding to the reducer
			var reduceList []string
			for i := 0; i < nMap; i++ {
				if reducer >= 0 {
					filename := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reducer)
					reduceList = append(reduceList, filename)
				}
			}

			intermediate := []KeyValue{} // single intermediate value for sorting and reducing
			for j := 0; j < len(reduceList); j++ {
				filename := reduceList[j]
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %v", filename)
				}
				defer file.Close()
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}
			}
			sort.Sort(ByKey(intermediate)) // sort the intermediate values
			Reduce(reducer, intermediate, reducef)
			ReduceTaskCompleted(reduceTaskID)
		default:
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}

func Map(filename string, nMap, nReduce int, mapf func(string, string) []KeyValue) {
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
	// call the partition function and write to temp file
	Partition(kva, nMap, nReduce)
}

// Function for mapping key values pairs to an index and writing to temp files with atomic renaming
func Partition(kva []KeyValue, nMap int, nReduce int) {
	// create some buckets for the reduce
	iMap := make([][]KeyValue, nReduce)

	for i := 0; i < len(kva); i++ {
		keyVal := kva[i]
		pIndex := ihash(keyVal.Key) % nReduce
		iMap[pIndex] = append(iMap[pIndex], keyVal)
	}

	// Create intermediary files for each reduce function
	for j := 0; j < nReduce; j++ {
		filename := "mr-" + strconv.Itoa(nMap) + "-" + strconv.Itoa(j)
		outfile, err := ioutil.TempFile(".", "temp-"+filename)
		if err != nil {
			log.Fatalf("Error Occurred %v\n", err)
		}
		defer os.Remove(outfile.Name())
		enc := json.NewEncoder(outfile)
		hkva := iMap[j] // the key values in a particular reduce bucket
		for k := 0; k < len(hkva); k++ {
			err := enc.Encode(&hkva[k])
			if err != nil {
				log.Fatalf("JSON encoding error: %v\n", err)
			}
		}
		outfile.Close()

		// atomic rename the file
		err = os.Rename(outfile.Name(), filename)
		if err != nil {
			log.Fatalf("There was an error in renaming the encoding file %v\n", err)
		}

	}
}

func Reduce(Reducer int, Intermediate []KeyValue, reducef func(string, []string) string) {
	oname := "mr-out" + "-" + strconv.Itoa(Reducer)
	ofile, _ := ioutil.TempFile(".", "temp-"+oname)
	defer os.Remove(ofile.Name())
	i := 0
	for i < len(Intermediate) {
		j := i + 1
		for j < len(Intermediate) && Intermediate[j].Key == Intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, Intermediate[k].Value)
		}
		output := reducef(Intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", Intermediate[i].Key, output)

		i = j
	}
	ofile.Close()

	// atomic rename the file
	err := os.Rename(ofile.Name(), oname)
	if err != nil {
		log.Fatalf("There was an error in renaming the file %v\n", err)
	}

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
		log.Fatalf("Assign Task Call Failed!\n")
		return TaskReply{}
	} else {
		return reply
	}
}

// RPC Call to the coordinator notifying map task has completed
func MapTaskCompleted(MapTaskID int) {
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

// RPC Call to the coordinator notifying reduce task has completed
func ReduceTaskCompleted(ReduceTaskID int) {
	// Arguments
	args := ReduceTaskCompletedArgs{}
	args.ReduceTaskID = ReduceTaskID
	// Reply
	reply := ReduceTaskCompletedReply{}
	ok := call("Coordinator.UpdateReduceStatus", &args, &reply)
	if !ok {
		log.Fatalf("Reduce Task Completed Reply Failed!\n")
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
