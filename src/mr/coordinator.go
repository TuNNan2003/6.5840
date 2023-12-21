package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type interFiles struct {
	filePaths []string
	occupy    bool
}

type Coordinator struct {
	// Your definitions here.
	files   []string
	nReduce int

	JobMux          sync.Mutex
	MapFinish       bool //whether map jobs has all finished
	MapFinishNum    int
	ReduceFinish    bool //whether reduce jobs has all finished
	ReduceFinishNum int
	AllFinish       bool //decide the map-reduce job finish

	WorkerMux sync.Mutex  //mutex for the worker queue
	workers   []*MRworker //the worker queue

	TaskMux    sync.Mutex        //mutex for the task queue
	tasks      map[int]*TaskDesc //the task queue
	FileOccupy map[string]bool
	InterFiles []interFiles
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
	allfinish := c.AllFinish
	c.JobMux.Unlock()
	if !mapfinish {
		c.TaskMux.Lock()
		if len(c.tasks) < 8 {
			for num, file := range c.files {
				if !c.FileOccupy[file] {
					c.FileOccupy[file] = true
					fname := []string{file}
					c.tasks[worker.ID] = &TaskDesc{Filename: fname, TaskType: 1, Time: 0, seqnum: num}
					reply.ID = worker.ID
					reply.Task.Filename = fname
					reply.Task.TaskType = c.tasks[worker.ID].TaskType
					log.Print("assign task 1", reply)
					c.TaskMux.Unlock()
					return nil
				}
			}
		}
		reply.ID = worker.ID
		reply.Task.TaskType = 0
		worker.Task.TaskType = 0
		c.TaskMux.Unlock()
		return nil
	} else if !allfinish {
		c.TaskMux.Lock()
		for num, interfile := range c.InterFiles {
			if !interfile.occupy {
				_, find := c.tasks[worker.ID]
				if find {
					reply.ID = worker.ID
					reply.Task.TaskType = 0
					c.TaskMux.Unlock()
					return nil
				}
				c.InterFiles[num].occupy = true
				mFileGroup := interfile.filePaths
				c.tasks[worker.ID] = &TaskDesc{Filename: mFileGroup, TaskType: 2, Time: 0, seqnum: num}
				reply.ID = worker.ID
				reply.Task.TaskType = 2
				reply.Task.Filename = mFileGroup
				log.Print("assign task 2", reply)
				c.TaskMux.Unlock()
				return nil
			}
		}
		reply.ID = worker.ID
		reply.Task.TaskType = 0
		//log.Print("assign task 0 to not quit", reply)
		//log.Fatalf("ask one worker to quit %v", worker)
		c.TaskMux.Unlock()
		return nil
	} else {
		c.TaskMux.Lock()
		reply.ID = worker.ID
		reply.Task.TaskType = 3
		log.Print("assign task 3 to quit", reply)
		c.TaskMux.Unlock()
		return nil
	}
}

func (c *Coordinator) IfDeleted(worker *MRworker, reply *int) error {
	c.TaskMux.Lock()
	_, find := c.tasks[worker.ID]
	if !find {
		*reply = -1
		c.TaskMux.Unlock()
		return nil
	}
	*reply = 1
	c.tasks[worker.ID].Time = 0
	c.TaskMux.Unlock()
	return nil
}

func (c *Coordinator) TaskFinish(worker *MRworker, reply *int) error {
	switch worker.Task.TaskType {
	case 1:
		c.JobMux.Lock()
		c.TaskMux.Lock()
		_, find := c.tasks[worker.ID]
		c.TaskMux.Unlock()
		if find {
			*reply = 1
			c.MapFinishNum++
		} else {
			*reply = -1
		}
		if c.MapFinishNum == len(c.files) {
			c.MapFinish = true
		}
		log.Print("task 1 finish", worker, "Mapfinish: ", c.MapFinishNum)
		c.JobMux.Unlock()
		c.TaskMux.Lock()
		delete(c.tasks, worker.ID)
		for i, filename := range worker.Task.Filename {
			c.InterFiles[i].filePaths = append(c.InterFiles[i].filePaths, filename)
		}
		c.TaskMux.Unlock()

	case 2:
		c.JobMux.Lock()
		c.TaskMux.Lock()
		_, find := c.tasks[worker.ID]
		delete(c.tasks, worker.ID)
		c.TaskMux.Unlock()
		if find {
			c.ReduceFinishNum++
			if c.ReduceFinishNum == c.nReduce {
				c.ReduceFinish = true
			}
			if c.ReduceFinish && c.MapFinish {
				c.AllFinish = true
			}
			log.Print("task 2 finish", worker, "reduce finished: ", c.ReduceFinishNum)
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
	time.Sleep(time.Second * time.Duration(4))
	c.TaskMux.Lock()
	log.Print("Enter check")
	for id, desc := range c.tasks {
		c.tasks[id].Time = c.tasks[id].Time + 5
		log.Println(id, desc)
		if desc.Time >= 10 {
			switch desc.TaskType {
			case 1:
				log.Print("task 1 find one crash ", id, desc)
				c.FileOccupy[desc.Filename[0]] = false
				delete(c.tasks, id)
			case 2:
				log.Print("task 2 find one crash ", id, desc)
				c.InterFiles[desc.seqnum].occupy = false
				delete(c.tasks, id)
			}

		}
	}

	c.TaskMux.Unlock()
	return c.AllFinish
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	logfile, err := os.OpenFile("log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		log.Fatal(err)
	}
	log.SetOutput(logfile)
	log.Print("New Entrance")
	c := Coordinator{files: files, nReduce: nReduce, AllFinish: false,
		MapFinish: false, MapFinishNum: 0,
		ReduceFinish: false, ReduceFinishNum: 0,
	}
	c.FileOccupy = make(map[string]bool, len(files))
	for _, file := range files {
		c.FileOccupy[file] = false
	}
	c.InterFiles = make([]interFiles, nReduce)
	for _, interfile := range c.InterFiles {
		interfile.filePaths = make([]string, 0)
		interfile.occupy = false
	}
	c.workers = make([]*MRworker, 0)
	c.tasks = make(map[int]*TaskDesc)
	// Your code here.

	c.server()
	return &c
}
