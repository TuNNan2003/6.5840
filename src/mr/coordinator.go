package mr

import (
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

func (c *Coordinator) GetTask(worker *MRworker, reply *int) error {
	c.JobMux.Lock()
	mapfinish := c.MapFinish
	c.JobMux.Unlock()
	if !mapfinish {
		c.TaskMux.Lock()
		for _, file := range c.files {
			if !c.FileOccupy[file] {
				c.FileOccupy[file] = true
				fname := []string{file}
				c.tasks[worker] = TaskDesc{filename: fname, tasktype: 1, time: 0}
				worker.task.filename = fname
				worker.task.tasktype = c.tasks[worker].tasktype
				log.Fatalf("generate task %v -> %v\n", file, worker)
				return nil
			}
		}
		worker.task.tasktype = 0
		c.TaskMux.Unlock()
	} else {
		c.TaskMux.Lock()
		if len(c.tasks) < c.nReduce {
			mFileGroup := c.intermadiateFiles[len(c.tasks)]
			c.tasks[worker] = TaskDesc{filename: mFileGroup, tasktype: 2, time: 0}
			worker.task.filename = mFileGroup
			worker.task.tasktype = 2
			log.Fatalf("generate task %v -> %v\n", mFileGroup, worker)
		} else {
			worker.task.tasktype = 3
			log.Fatalf("ask one worker to quit %v", worker)
		}
		c.TaskMux.Unlock()
		return nil
	}
	return nil
}

func (c *Coordinator) TaskFinish(worker *MRworker, reply *int) error {
	switch worker.task.tasktype {
	case 1:
		c.JobMux.Lock()
		c.MapFinishNum++
		if c.MapFinishNum == len(c.files) {
			c.MapFinish = true
		}
		for i, filename := range worker.task.filename {
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
	c.workers = make([]*MRworker, 0)
	c.tasks = make(map[*MRworker]TaskDesc)
	// Your code here.

	c.server()
	return &c
}
