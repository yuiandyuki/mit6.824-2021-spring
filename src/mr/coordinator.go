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

// Master 结构体用于表示 MapReduce 框架中的主节点。
type Coordinator struct {
	mu   sync.Mutex // 用于保护 Master 结构体的并发访问
	cond *sync.Cond

	mapFiles     []string // 存储所有映射任务的输入文件名
	nMapTasks    int      // 映射任务的总数
	nReduceTasks int      // 归约任务的总数

	mapTasksFinished []bool      // 记录每个映射任务的完成状态
	mapTasksIssued   []time.Time // 记录每个映射任务的发放时间

	reduceTasksFinished []bool      // 记录每个归约任务的完成状态
	reduceTasksIssued   []time.Time // 记录每个归约任务的发放时间

	isDone bool // 标识整个任务是否完成
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) HandleGetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	reply.NReduceTasks = c.nReduceTasks
	reply.NMapTasks = c.nMapTasks

	for {
		mapDone := true
		for m, done := range c.mapTasksFinished {
			if !done {
				if c.mapTasksIssued[m].IsZero() ||
					time.Since(c.mapTasksIssued[m]).Seconds() > 10 {
					reply.TaskType = Map
					reply.TaskNum = m
					reply.MapFile = c.mapFiles[m]
					c.mapTasksIssued[m] = time.Now()
					return nil
				} else {
					mapDone = false
				}
			}
		}

		if !mapDone {
			c.cond.Wait()
		} else {
			break
		}
	}

	for {
		redDone := true
		for r, done := range c.reduceTasksFinished {
			if !done {
				if c.reduceTasksIssued[r].IsZero() ||
					time.Since(c.reduceTasksIssued[r]).Seconds() > 10 {
					reply.TaskType = Reduce
					reply.TaskNum = r
					c.reduceTasksIssued[r] = time.Now()
					return nil
				} else {
					redDone = false
				}
			}
		}
		if !redDone {
			c.cond.Wait()
		} else {
			break
		}
	}
	reply.TaskType = Done
	c.isDone = true

	return nil
}

func (c *Coordinator) HandleFinishedTask(args *FinishedTaskArgs, reply *FinishedTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch args.TaskType {
	case Map:
		c.mapTasksFinished[args.TaskNum] = true
	case Reduce:
		c.reduceTasksFinished[args.TaskNum] = true
	default:
		log.Fatalf("Bad finished task? %s", args.TaskType)
	}
	c.cond.Broadcast()
	return nil
}

//
// an example RPC handler.

// the RPC argument and reply types are defined in rpc.go.
func (m *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (m *Coordinator) server() {
	rpc.Register(m)
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

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Coordinator) Done() bool {
	// ret := false

	// Your code here.

	return m.isDone
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.cond = sync.NewCond(&c.mu)

	c.mapFiles = files
	c.nMapTasks = len(files)
	c.mapTasksFinished = make([]bool, len(files))
	c.mapTasksIssued = make([]time.Time, nReduce)

	c.nReduceTasks = nReduce
	c.reduceTasksFinished = make([]bool, nReduce)
	c.reduceTasksIssued = make([]time.Time, nReduce)

	go func() {
		for {
			c.mu.Lock()
			c.cond.Broadcast()
			c.mu.Unlock()
			time.Sleep(time.Second)
		}
	}()

	c.server()
	return &c
}
