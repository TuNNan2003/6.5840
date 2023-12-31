package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type TaskDesc struct {
	Filename []string
	TaskType int
	Time     int
	seqnum   int
}

type Task struct {
	Filename []string
	mapf     func(string, string) []KeyValue
	reducef  func(string, []string) string
	TaskType int
}

type MRworker struct {
	ID   int
	Task Task
}

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
