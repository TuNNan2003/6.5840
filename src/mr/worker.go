package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"sync"
	"time"
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

var funclock sync.Mutex

func workerTrigger(workerID int, mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	worker := MRworker{workerID, Task{}}
	worker.Task.mapf = mapf
	worker.Task.reducef = reducef
	var tempInt int
	err := call("Coordinator.Register", &worker, &tempInt)
	if err {
		for {
			nwork := MRworker{Task: Task{}}
			err := call("Coordinator.GetTask", &worker, &nwork)
			worker = nwork
			worker.Task.mapf = mapf
			worker.Task.reducef = reducef
			if err {
				switch worker.Task.TaskType {
				//This is not a task
				case 0:
					time.Sleep(3 * time.Microsecond)
				//This is a map task
				case 1:
					var kvResult []KeyValue
					for _, filename := range worker.Task.Filename {
						file, err := os.Open(filename)
						if err != nil {
							log.Fatalf("can't open %v", worker.Task.Filename)
						}
						conslice, err := ioutil.ReadAll(file)
						if err != nil {
							log.Fatalf("can't read %v", worker.Task.Filename)
						}
						file.Close()
						funclock.Lock()
						var alive int
						call("Coordinator.IfDeleted", &worker, &alive)
						if alive != 1 {
							funclock.Unlock()
							continue
						}
						resultSlice := worker.Task.mapf(filename, string(conslice))
						call("Coordinator.IfDeleted", &worker, &alive)
						if alive != 1 {
							funclock.Unlock()
							continue
						}
						funclock.Unlock()
						kvResult = append(kvResult, resultSlice...)

					}
					var nReduce int
					temp := MRworker{0, Task{}}
					ok := call("Coordinator.GetnReduce", &temp, &nReduce)
					if !ok {
						return
					}
					worker.emit(&kvResult, nReduce)
					call("Coordinator.TaskFinish", &worker, &tempInt)
				//This is a reduce task
				case 2:
					var content []KeyValue
					var key, value string
					for _, filename := range worker.Task.Filename {
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
						funclock.Lock()
						var alive int
						call("Coordinator.IfDeleted", &worker, &alive)
						if alive != 1 {
							funclock.Unlock()
							continue
						}
						output := worker.Task.reducef(content[i].Value, values)
						call("Coordinator.IfDeleted", &worker, &alive)
						if alive != 1 {
							funclock.Unlock()
							continue
						}
						funclock.Unlock()
						reduceResult = append(reduceResult, KeyValue{content[i].Key, output})
						i = j
					}

					worker.emit(&reduceResult, -1)
					call("Coordinator.TaskFinish", &worker, &tempInt)
				//This indicates a end
				case 3:
					return
				}
			} else {
				fmt.Println("Error when getting task")
			}
		}
	}
}

func (w *MRworker) emit(Result *[]KeyValue, nReduce int) {
	switch w.Task.TaskType {
	//produce output file for map
	case 1:
		sort.Sort(KeyValueArray(*Result))
		var mOutFile []*os.File
		var OutFilePath []string
		for i := 0; i < nReduce; i++ {
			filename := fmt.Sprintf("m%d-out-%d", w.ID, i)
			file, err := os.Create(filename)
			if err == nil {
				mOutFile = append(mOutFile, file)
				OutFilePath = append(OutFilePath, filename)
			} else {
				fmt.Println(err)
			}
		}
		w.Task.Filename = OutFilePath
		for _, kv := range *Result {
			fmt.Fprintf(mOutFile[ihash(kv.Key)%nReduce], "%v %v\n", kv.Key, kv.Value)
		}
		for _, file := range mOutFile {
			file.Close()
		}
		return
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
	temp := MRworker{0, Task{}}
	temp.Task.mapf = mapf
	temp.Task.reducef = reducef
	temp.Task.Filename = make([]string, 0)
	ok := call("Coordinator.GetnReduce", temp, &workernum)
	pid := os.Getpid()
	if ok {
		for i := 0; i < workernum; i++ {
			go workerTrigger(pid*100+i, mapf, reducef)
		}
	} else {
		log.Fatal("get nReduce RPC failed")
	}
	var finish bool
	for {
		call("Coordinator.WorkerQuit", &temp, &finish)
		if finish {
			return
		}
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

	fmt.Println("RPC error:", rpcname, err)
	return false
}
