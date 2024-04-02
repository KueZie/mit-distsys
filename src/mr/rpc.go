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
// Add your RPC definitions here.

type StageType string // "Map", "Reduce", "Wait", "Exit"

type RequestTaskArgs struct {
	WorkerId int
}
type RequestTaskReply struct {
	Stage          StageType
	Filename       string
	SequenceId     int
	NumReduceTasks int
}

type ReportTaskCompletionArgs struct {
	Stage    string // "MapCompleted", "ReduceCompleted"
	Filename string // the filename of the map or reduce task
}
type ReportTaskCompletionReply struct {}

type RegisterWorkerArgs struct {}
type RegisterWorkerReply struct {
	WorkerId int
}

type HeartbeatArgs struct {
	WorkerId int
}
type HeartbeatReply struct {}


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
