package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	// Your definitions here.
	files             []string
	intermadiateFiles [][]string
	nReduce           int

	JobMux          sync.Mutex
	MapFinish       bool //whether map jobs has all finished
	MapFinishNum    int
	ReduceFinish    bool //whether reduce jobs has all finished
	ReduceFinishNum int
	AllFinish       bool //decide the map-reduce job finish

	WorkerMux sync.Mutex  //mutex for the worker queue
	workers   []*MRworker //the worker queue

	TaskMux sync.Mutex             //mutex for the task queue
	tasks   map[*MRworker]TaskDesc //the task queue

	FileOccupy map[string]bool
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) WorkerQuit(args *MRworker, reply *bool) error {
	*reply = c.AllFinish
	return nil
}

func (c *Coordinator) GetnReduce(args *MRworker, reply *int) error {
	*reply = c.nReduce
	return nil
}

func (c *Coordinator) Register(w *MRworker, reply *int) error {
	c.WorkerMux.Lock()
	c.workers = append(c.workers, w)
	c.WorkerMux.Unlock()
	*reply = 0
	return nil
}

func (c *Coordinator) GetTask(worker *MRworker, reply *MRworker) error {
	c.JobMux.Lock()
	mapfinish := c.MapFinish
	c.JobMux.Unlock()
	fmt.Println("Map condition", mapfinish)
	if !mapfinish {
		c.TaskMux.Lock()
		for _, file := range c.files {
			if !c.FileOccupy[file] {
				c.FileOccupy[file] = true
				fname := []string{file}
				c.tasks[worker] = TaskDesc{Filename: fname, TaskType: 1, Time: 0}
				reply.ID = worker.ID
				reply.Task.Filename = fname
				reply.Task.TaskType = c.tasks[worker].TaskType
				//log.Fatalf("generate task %v -> %v\n", file, reply)
				fmt.Println("assign task 1", worker, reply)
				c.TaskMux.Unlock()
				return nil
			}
		}
		reply.ID = worker.ID
		reply.Task.TaskType = 0
		worker.Task.TaskType = 0
		fmt.Println("assign task 0", worker, reply)
		c.TaskMux.Unlock()
		return nil
	} else {
		c.TaskMux.Lock()
		fmt.Println("task 2 or 3", len(c.tasks), c.nReduce)
		if len(c.tasks) < c.nReduce {
			mFileGroup := c.intermadiateFiles[len(c.tasks)]
			c.tasks[worker] = TaskDesc{Filename: mFileGroup, TaskType: 2, Time: 0}
			reply.Task.TaskType = 2
			reply.Task.Filename = mFileGroup
			//log.Fatalf("generate task %v -> %v\n", mFileGroup, worker)
		} else {
			reply.Task.TaskType = 3
			//log.Fatalf("ask one worker to quit %v", worker)
		}
		c.TaskMux.Unlock()
		return nil
	}
}

func (c *Coordinator) TaskFinish(worker *MRworker, reply *int) error {
	switch worker.Task.TaskType {
	case 1:
		fmt.Println("task finish", worker)
		c.JobMux.Lock()
		c.MapFinishNum++
		fmt.Println("finished & all ", c.MapFinishNum, len(c.files))
		if c.MapFinishNum == len(c.files) {
			c.MapFinish = true
		}
		for i, filename := range worker.Task.Filename {
			c.intermadiateFiles[i] = append(c.intermadiateFiles[i], filename)
		}
		c.JobMux.Unlock()
		c.TaskMux.Lock()
		delete(c.tasks, worker)
		c.TaskMux.Unlock()

	case 2:
		c.JobMux.Lock()
		c.ReduceFinishNum++
		if c.ReduceFinishNum == c.nReduce {
			c.ReduceFinish = true
		}
		if c.ReduceFinish && c.MapFinish {
			c.AllFinish = true
		}
		c.JobMux.Unlock()
	}
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)

	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// Your code here.

	return c.AllFinish
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{files: files, nReduce: nReduce, AllFinish: false,
		MapFinish: false, MapFinishNum: 0,
		ReduceFinish: false, ReduceFinishNum: 0,
	}
	c.FileOccupy = make(map[string]bool, len(files))
	for _, file := range files {
		c.FileOccupy[file] = false
	}
	c.intermadiateFiles = make([][]string, nReduce)
	c.workers = make([]*MRworker, 0)
	c.tasks = make(map[*MRworker]TaskDesc)
	// Your code here.

	c.server()
	return &c
}
