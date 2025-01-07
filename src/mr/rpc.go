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

type TaskType int
type TaskCompletedStatus int

const (
	MapTask TaskType = iota
	ReduceTask
	WaitTask
	ExitTask
)

const (
	MapTaskCompleted TaskCompletedStatus = iota
	ReduceTaskCompleted
	MapTaskFailed
	ReduceTaskFailed
)

// MessageSend is the struct of message send
type MessageSend struct {
	TaskID              int
	TaskCompletedStatus TaskCompletedStatus
}

// MessageReply is the struct of message reply
type MessageReply struct {
	NMap     int      // map number, indicate the number of map tasks
	NReduce  int      // reduce number, indicate the number of reduce tasks
	TaskID   int      // task id
	TaskFile string   // task file name
	TaskType TaskType // task type, map or reduce or wait or exit
}

//
// example to show how to declare the arguments
// and reply for an RPC.
//

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
