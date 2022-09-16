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
		// fmt.Printf("task reply struct %v\n", taskReply)
		task := taskReply.Task
		// fmt.Printf("task reply struct %v\n", taskReply)
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
		case task == "reduce":
			fmt.Printf("Task: %v\n", taskReply.Task)
			fmt.Printf("Reducer: %v\n", taskReply.Reducer)
			reducer := taskReply.Reducer
			fmt.Printf("Reduce Task ID: %v\n", taskReply.ReduceTaskID)
			reduceTaskID := taskReply.ReduceTaskID
			fmt.Printf("Map Count %v\n", taskReply.NMap)
			nMap := taskReply.NMap
			// fmt.Printf("Reduce Count %v\n", taskReply.NReduce)
			// nReduce := taskReply.NReduce

			// combine all the files into an array corresponding to the reducer
			var reduceList []string
			for i := 0; i < nMap; i++ {
				if reducer >= 0 {
					filename := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reducer)
					reduceList = append(reduceList, filename)
				}
			}
			// fmt.Printf("Combined Reduced List: %v\n", reduceList)

			intermediate := []KeyValue{} // single intermediate value for sorting and reducing
			for j := 0; j < len(reduceList); j++ {
				filename := reduceList[j]
				fmt.Printf("Intermediate File Name: %v\n", filename)
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
			// fmt.Printf("Intermediate Reduce Input: %v\n", intermediate)
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

// Function for mapping key values pairs to an index and writing to temp files with atomic renaming
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
	fmt.Println("Reduce Function")
	oname := "mr-out" + "-" + strconv.Itoa(Reducer)
	fmt.Printf("Reduce Output Name: %v\n", oname)
	ofile, err := ioutil.TempFile(".", "temp-"+oname)
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
	err = os.Rename(ofile.Name(), oname)
	if err != nil {
		log.Fatalf("There was an error in renaming the file %v\n", err)
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

// RPC Call to the coordinator notifying reduce task has completed
func ReduceTaskCompleted(ReduceTaskID int) {
	fmt.Println("Reduce Task Completed")
	fmt.Printf("Reduce Task ID: %v\n", ReduceTaskID)
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
