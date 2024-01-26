package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "io/ioutil"
import "encoding/json"
import "time"
import "sort"

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

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.SetOutput(ioutil.Discard)

	for {
		// First make an RPC to the coordinator to figure out what task needs to be completed.
		task := requestTask()

		switch task.Sort {
		case SORT_MAP:
			handleMapTask(&task, mapf)
		case SORT_REDUCE:
			handleReduceTask(&task, reducef)
		case SORT_IDLE:
			log.Println("sleeping for 100ms")
			time.Sleep(100 * time.Millisecond)
		case SORT_PLEASE_EXIT:
			log.Fatalln("now exiting worker")
		}
	}
}

func handleMapTask(task *RequestTaskReply, mapf func(string, string) []KeyValue) {
	// First read contents of file into memory
	file, err := os.Open(task.File)

	if err != nil {
		log.Fatalf("cannot open file %v\n", task.File)
	}

	content, err := ioutil.ReadAll(file)

	if err != nil {
		log.Fatalf("cannot read file %v\n", task.File)
	}

	file.Close()

	// Run "map" function on file contents
	kva := mapf(task.File, string(content))

	// Append every key value pair to file mr-X-Y
	//	X = task.Id
	//	Y = ihash(kv.Key)
	fileHandles := make([]*os.File, task.ReduceCount)

	for i := 0; i < len(fileHandles); i++ {
		file, err := ioutil.TempFile("", "mr-")

		if err != nil {
			log.Fatalln("could not create temporary file")
		}

		defer file.Close()
		fileHandles[i] = file
	}

	for _, kv := range kva {
		bucket := ihash(kv.Key) % task.ReduceCount
		file := fileHandles[bucket]
		enc := json.NewEncoder(file)

		log.Printf("writing key '%v' to reducer bucket %v", kv.Key, bucket)

		err := enc.Encode(&kv)

		if err != nil {
			log.Fatalln("could not write to file %v", file.Name())
		}
	}

	// Rename all files to be intermediate file locations
	for r, file := range fileHandles {
		err := os.Rename(file.Name(), fmt.Sprintf("mr-intermediate-%v-%v", task.Id, r))

		if err != nil {
			log.Fatalln("could not rename %v", file.Name())
		}
	}

	// Finally tell the coordinator that we have finished this task
	completeTask(task.Id, task.Sort)
}

func handleReduceTask(task *RequestTaskReply, reducef func(string, []string) string) {
	// First open all appropriate files and read their keys into memory
	M := task.MapCount
	intermediate := []KeyValue{}

	for i := 0; i < M; i++ {
		filename := fmt.Sprintf("mr-intermediate-%v-%v", i, task.Id)
		file, err := os.Open(filename)

		if err != nil {
			log.Fatalf("cannot open file %v\n", filename)
		}

		dec := json.NewDecoder(file)

		for {
			var kv KeyValue

			if err := dec.Decode(&kv); err != nil {
				break
			}

			intermediate = append(intermediate, kv)
		}
	}

	// Sort all the intermediate keys before passing to reducer.
	sort.Sort(ByKey(intermediate))

	// Create output file
	oname := fmt.Sprintf("mr-out-%v", task.Id)
	ofile, _ := os.Create(oname)

	for i := 0; i < len(intermediate); {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}

		values := make([]string, 0, 10)

		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}

		output := reducef(intermediate[i].Key, values)

		// Print out based on format
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()

	// Finally tell the coordinator that we have finished this task
	completeTask(task.Id, task.Sort)
}

func requestTask() RequestTaskReply {
	args := RequestTaskArgs{}
	reply := RequestTaskReply{}

	log.Println("asking coordinator for task")

	ok := call("Coordinator.RequestTask", &args, &reply)

	if !ok {
		log.Fatalln("could not reach coordinator; exiting")
	}

	log.Printf("starting task %v", reply)

	return reply
}

func completeTask(id int, sort TaskSort) {
	args := CompleteTaskArgs{Sort: sort, Id: id}
	reply := CompleteTaskReply{}

	log.Println("telling coordinator about task completion")

	ok := call("Coordinator.CompleteTask", &args, &reply)

	if !ok {
		log.Fatalln("could not reach coordinator; exiting")
	}

	log.Printf("completing task %v(%v)", sort, id)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
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
