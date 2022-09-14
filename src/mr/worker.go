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
			// fmt.Printf("Reduce Count: %v\n", taskReply.ReduceCount)
			reduceCount := taskReply.ReduceCount

			// Map
			Map(fileName, mapCount, reduceCount, mapf)

			// Notify coordinator with the update
			CompleteMapTask(fileName)
		case task == "reduce":
			fmt.Println("Perform a Reduce Task")
			// fmt.Printf("Reducer: %v\n", taskReply.Reducer)
			reducer := taskReply.Reducer
			// fmt.Printf("Map Count: %v\n", taskReply.MapCount)
			mapCount := taskReply.MapCount

			// combine all the files into an array corresponding to the reducer
			var reduceList []string
			for i := 0; i < mapCount; i++ {
				if reducer >= 0 {
					filename := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reducer)
					reduceList = append(reduceList, filename)
				}
			}

			// Produce a single intermediate value for sorting and reducing
			intermediate := []KeyValue{}
			for j := 0; j < len(reduceList); j++ {
				filename := reduceList[j]
				fmt.Printf("Filename: %v\n", filename)
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
			// sort the intermediate values
			sort.Sort(ByKey(intermediate))

			// Reduce
			Reduce(reducer, intermediate, reducef)

			// Notify coodinator that reduce task is complete
			CompleteReduceTask(reducer)

		default:
			log.Fatal("The Message was not received and failed!\n")
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func Map(filename string, MapCount int, ReduceCount int, mapf func(string, string) []KeyValue) {
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
	Partition(kva, MapCount, ReduceCount)
}

func Reduce(Reducer int, Intermediate []KeyValue, reducef func(string, []string) string) {
	oname := "mr-out" + "-" + strconv.Itoa(Reducer)
	ofile, _ := os.Create(oname)
	defer ofile.Close()
	// fmt.Printf("Intermediate: %v\n", Intermediate)
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

}

// Function for mapping key values pairs to an index
func Partition(kva []KeyValue, nMap int, nReduce int) {
	// create some buckets for the reduce
	iMap := make([][]KeyValue, nReduce)

	// Find length of key value map
	// fmt.Println("Enter the Partition Function")
	// fmt.Printf("len of kva: %v\n", len(kva))
	// fmt.Printf("iMap: %v\n", iMap)

	for i := 0; i < len(kva); i++ {
		keyVal := kva[i]
		pIndex := ihash(keyVal.Key) % nReduce
		// fmt.Printf("bucket: %v\n", pIndex)
		iMap[pIndex] = append(iMap[pIndex], keyVal)
		// fmt.Printf("current hash value: %v\n", iMap[pIndex])
	}

	// Print indexes of imap
	// for index,_ := range iMap {
	// 	// fmt.Printf("bucket: %v\n", index)
	// 	// fmt.Printf("number of elements in a bucket: %v\n", len(iMap[index]))
	// }

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
				log.Fatalf("error: %v\n", err)
			}
		}
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
	// Reply
	reply := MapTaskCompletedReply{}
	ok := call("Coordinator.UpdateMap", &args, &reply)
	if !ok {
		fmt.Printf("Complete Task Call failed!\n")
	}
}

// RPC Call to the coordinator notifying that reduce task has been finished
func CompleteReduceTask(Reducer int) {
	//Arguments
	args := ReduceTaskCompletedArgs{}
	args.Reducer = Reducer
	// Reply
	reply := ReduceTaskCompletedReply{}
	ok := call("Coordinator.UpdateReduce", &args, &reply)
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
