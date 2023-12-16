package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
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
type KeyValueArray []KeyValue

func (m KeyValueArray) Len() int           { return len(m) }
func (m KeyValueArray) Swap(i, j int)      { m[i], m[j] = m[j], m[i] }
func (m KeyValueArray) Less(i, j int) bool { return m[i].Key < m[j].Key }

type worker struct {
	ID   int
	task Task
}

func workerTrigger(workerID int, mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	worker := worker{workerID, Task{}}
	worker.task.mapf = mapf
	worker.task.reducef = reducef
	ok := call("Coordinator.WorkerRegister", &worker, nil)
	if ok {
		for {
			call("Coordinator.GetTask", &worker, nil)
			switch worker.task.tasktype {
			//This is not a task
			case 0:

			//This is a map task
			case 1:
				var kvResult []KeyValue
				for _, filename := range worker.task.filename {
					file, err := os.Open(filename)
					if err != nil {
						log.Fatalf("can't open %v", worker.task.filename)
					}
					conslice, err := ioutil.ReadAll(file)
					if err != nil {
						log.Fatalf("can't read %v", worker.task.filename)
					}
					file.Close()
					resultSlice := worker.task.mapf(filename, string(conslice))
					kvResult = append(kvResult, resultSlice...)
				}
				var nReduce int
				ok := call("Coordinate.GetnReduce", nil, &nReduce)
				if !ok {
					return
				}
				worker.emit(&kvResult, nReduce)
			//This is a reduce task
			case 2:
				var content []KeyValue
				var key, value string
				for _, filename := range worker.task.filename {
					file, err := os.Open(filename)
					for err == nil {
						_, err := fmt.Fscanf(file, "%v %v\n", &key, &value)
						if err != nil {
							break
						}
						content = append(content, KeyValue{key, value})
					}
					file.Close()
				}
				var reduceResult []KeyValue
				sort.Sort(KeyValueArray(content))
				i := 0
				for i < len(content) {
					j := i + 1
					for j < len(content) && content[j].Key == content[i].Key {
						j++
					}
					values := []string{}
					for k := i; k < j; k++ {
						values = append(values, content[k].Value)
					}
					output := worker.task.reducef(content[i].Value, values)
					reduceResult = append(reduceResult, KeyValue{content[i].Key, output})
				}
				worker.emit(&reduceResult, -1)
			//This indicates a end
			case 3:
				return
			}
		}
	}
}

func (w *worker) emit(Result *[]KeyValue, nReduce int) {
	switch w.task.tasktype {
	//produce output file for map
	case 1:
		sort.Sort(KeyValueArray(*Result))
		mOutFile := make([]*os.File, nReduce)
		for i := 0; i < nReduce; i++ {
			file, _ := os.Create(fmt.Sprintf("m%d-out-%d", w.ID, i))
			mOutFile[i] = file
		}
		for _, kv := range *Result {
			fmt.Fprintf(mOutFile[ihash(kv.Key)%nReduce], "%v %v\n", kv.Key, kv.Value)
		}
	case 2:
		rOutFile, _ := os.Create(fmt.Sprintf("mr-out-%d", w.ID))
		for _, kv := range *Result {
			fmt.Fprintf(rOutFile, "%v %v\n", kv.Key, kv.Value)
		}
	}
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

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	var workernum int
	ok := call("Coordinator.GetnReduce", nil, &workernum)
	if ok {
		for i := 1; i < workernum; i++ {
			go workerTrigger(i, mapf, reducef)
		}
	} else {
		log.Fatal("get nReduce RPC failed")
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
