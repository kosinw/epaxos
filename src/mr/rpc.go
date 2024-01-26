package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

type TaskSort int

const (
	SORT_MAP TaskSort = iota
	SORT_REDUCE
	SORT_PLEASE_EXIT
	SORT_IDLE
)

func (s TaskSort) String() string {
	return []string{"map", "reduce", "exit", "idle"}[s]
}

// Add your RPC definitions here.
type RequestTaskArgs struct {
}

type RequestTaskReply struct {
	Sort        TaskSort
	Id          int
	File        string
	ReduceCount int
	MapCount    int
}

type CompleteTaskArgs struct {
	Sort TaskSort
	Id   int
}

type CompleteTaskReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
