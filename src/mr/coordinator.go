package mr

import (
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskStatus int

const (
	STATUS_IDLE TaskStatus = iota
	STATUS_IN_PROGRESS
	STATUS_COMPLETED
)

func (s TaskStatus) String() string {
	return []string{"idle", "in_progress", "completed"}[s]
}

type MapTask struct {
	status TaskStatus
	id     int
	file   string
}

type ReduceTask struct {
	status TaskStatus
	id     int
}

type Coordinator struct {
	// Your definitions here.
	M, R                    int
	nMapTasks, nReduceTasks int
	lock                    *sync.Mutex
	mapTasks                []MapTask
	reduceTasks             []ReduceTask
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) CompleteTask(args *CompleteTaskArgs, reply *CompleteTaskReply) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	// Check if task is already complete, then do nothing
	switch args.Sort {
	case SORT_MAP:
		if c.mapTasks[args.Id].status == STATUS_IN_PROGRESS {
			log.Printf("finished map task %v\n", args.Id)
			c.mapTasks[args.Id].status = STATUS_COMPLETED
			c.nMapTasks--
		}
	case SORT_REDUCE:
		if c.reduceTasks[args.Id].status == STATUS_IN_PROGRESS {
			log.Printf("finished reduce task %v\n", args.Id)
			c.reduceTasks[args.Id].status = STATUS_COMPLETED
			c.nReduceTasks--
		}
	default:
		log.Fatalln("encountered unknown task type; exiting")
	}

	return nil
}

func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	reply.ReduceCount = c.R
	reply.MapCount = c.M

	// First try to assign any map tasks if they are available...
	for i := 0; i < c.M; i++ {
		task := &c.mapTasks[i]

		if task.status == STATUS_IDLE {
			reply.Sort = SORT_MAP
			reply.Id = i
			reply.File = task.file

			task.status = STATUS_IN_PROGRESS

			log.Printf("assigned map task %v\n", i)

			go c.monitorTask(reply.Sort, reply.Id)

			return nil
		}
	}

	if c.nMapTasks > 0 {
		reply.Sort = SORT_IDLE
		log.Printf("assigned idle task")
		return nil
	}

	// Next try to assign reduce tasks...
	for i := 0; i < c.R; i++ {
		task := &c.reduceTasks[i]

		if task.status == STATUS_IDLE {
			reply.Sort = SORT_REDUCE
			reply.Id = i

			task.status = STATUS_IN_PROGRESS

			log.Printf("assigned reduce task %v\n", i)

			go c.monitorTask(reply.Sort, reply.Id)

			return nil
		}
	}

	// Otherwise tell this task to "please exit" if no reducers are needed
	if c.nReduceTasks == 0 {
		reply.Sort = SORT_PLEASE_EXIT
		log.Printf("assigned please exit task")
	} else {
		reply.Sort = SORT_IDLE
		log.Printf("assigned idle task")
	}

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
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.nReduceTasks == 0
}

func (c *Coordinator) monitorTask(sort TaskSort, id int) {
	// Wait 10 seconds and if task is not complete by then,
	// Mark task as available to be scheduled again
	time.Sleep(10 * time.Second)

	c.lock.Lock()
	defer c.lock.Unlock()

	switch sort {
	case SORT_MAP:
		if c.mapTasks[id].status == STATUS_IN_PROGRESS {
			c.mapTasks[id].status = STATUS_IDLE
			log.Printf("killed incomplete map task %v \n", id)
		}
	case SORT_REDUCE:
		if c.reduceTasks[id].status == STATUS_IN_PROGRESS {
			c.reduceTasks[id].status = STATUS_IDLE
			log.Printf("killed incomplete reduce task %v \n", id)
		}
	default:
		log.Fatalln("unknown task type; exiting")
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.SetOutput(ioutil.Discard)

	c.M = len(files)
	c.R = nReduce

	c.nMapTasks = c.M
	c.nReduceTasks = c.R
	c.lock = &sync.Mutex{}

	c.mapTasks = make([]MapTask, c.M)

	for i := 0; i < c.M; i++ {
		c.mapTasks[i] = MapTask{STATUS_IDLE, i, files[i]}
	}

	c.reduceTasks = make([]ReduceTask, c.R)

	for i := 0; i < c.R; i++ {
		c.reduceTasks[i] = ReduceTask{STATUS_IDLE, i}
	}

	c.server()
	return &c
}
