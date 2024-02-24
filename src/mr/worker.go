package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
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
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func finalizeReduceFile(tmpFile string, taskN int) {
	finalFile := fmt.Sprintf("mr-out-%d", taskN)
	os.Rename(tmpFile, finalFile)
}

func getIntermediateFile(mapTaskN int, redTaskN int) string {
	return fmt.Sprintf("mr-%d-%d", mapTaskN, redTaskN)
}

func finalizeIntermediateFile(tmpFile string, mapTaskN int, redTaskN int) {
	finalFile := getIntermediateFile(mapTaskN, redTaskN)
	os.Rename(tmpFile, finalFile)
}

func performMap(filename string, taskNum int, nReduceTasks int, mapf func(string, string) []KeyValue) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	kva := mapf(filename, string(content))

	tmpFiles := []*os.File{}
	tmpFilenames := []string{}
	encoders := []*json.Encoder{}
	for r := 0; r < nReduceTasks; r++ {
		tmpFile, err := os.CreateTemp("", "")
		if err != nil {
			log.Fatalf("cannot open tempfile")
		}
		tmpFiles = append(tmpFiles, tmpFile)
		tmpFilename := tmpFile.Name()
		tmpFilenames = append(tmpFilenames, tmpFilename)
		enc := json.NewEncoder(tmpFile)
		encoders = append(encoders, enc)
	}

	for _, kv := range kva {
		r := ihash(kv.Key) % nReduceTasks
		encoders[r].Encode(&kv)
	}
	for _, f := range tmpFiles {
		f.Close()
	}
	for r := 0; r < nReduceTasks; r++ {
		finalizeIntermediateFile(tmpFilenames[r], taskNum, r)
	}
}

func performReduce(taskNum int, nMapTasks int, reducef func(string, []string) string) {
	kva := []KeyValue{}
	for m := 0; m < nMapTasks; m++ {
		iFilename := getIntermediateFile(m, taskNum)
		file, err := os.Open(iFilename)
		if err != nil {
			log.Fatalf("cannot open %v", iFilename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(kva))

	tmpFile, err := os.CreateTemp("", "")
	if err != nil {
		log.Fatalf("cannot open tmpfile")
	}
	tmpFilename := tmpFile.Name()

	key_begin := 0
	for key_begin < len(kva) {
		key_end := key_begin + 1

		for key_end < len(kva) && kva[key_end].Key == kva[key_begin].Key {
			key_end++
		}
		values := []string{}
		for k := key_begin; k < key_end; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[key_begin].Key, values)

		fmt.Fprintf(tmpFile, "%v %v\n", kva[key_begin].Key, output)

		key_begin = key_end
	}
	finalizeReduceFile(tmpFilename, taskNum)
}

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		args := GetTaskArgs{}
		reply := GetTaskReply{}
		call("Coordinator.HandleGetTask", &args, &reply)

		switch reply.TaskType {
		case Map:
			performMap(reply.MapFile, reply.TaskNum, reply.NReduceTasks, mapf)
		case Reduce:
			performReduce(reply.TaskNum, reply.NMapTasks, reducef)
		case Done:
			os.Exit(0)
		default:
			fmt.Errorf("Bad task type? %s", reply.TaskType)
		}
		finargs := FinishedTaskArgs{
			TaskType: reply.TaskType,
			TaskNum:  reply.TaskNum,
		}
		finreply := FinishedTaskReply{}
		call("Coordinator.HandleFinishedTask", &finargs, &finreply)
	}
	// uncomment to send the Example RPC to the master.
	// CallExample()

}

// example function to show how to make an RPC call to the master.
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
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

// send an RPC request to the master, wait for the response.
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
