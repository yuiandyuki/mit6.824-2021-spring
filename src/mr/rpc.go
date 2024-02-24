package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

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

// TaskType 是一个自定义的整数类型，用于表示不同类型的任务。
type TaskType int

// 下面是任务类型的常量定义：
// - Map 表示映射任务，用于MapReduce框架中的映射阶段。
// - Reduce 表示归约任务，用于MapReduce框架中的归约阶段。
// - Done 表示任务已完成，用于标识任务执行完毕。
const (
	Map    TaskType = 1
	Reduce TaskType = 2
	Done   TaskType = 3
)

// GetTaskArgs 表示获取任务的请求参数结构。
type GetTaskArgs struct{}

// GetTaskReply 包含从服务器获取的任务的相关信息。
type GetTaskReply struct {
	TaskType     TaskType // 任务类型：Map、Reduce 或 Done
	TaskNum      int      // 任务编号
	NReduceTasks int      // Reduce任务的总数
	MapFile      string   // map任务对应的输入文件名
	NMapTasks    int      // map任务的总数
}

// FinishedTaskArgs 表示完成任务的请求参数结构。
type FinishedTaskArgs struct {
	TaskType TaskType // 完成的任务类型
	TaskNum  int      // 完成的任务编号
}

// FinishedTaskReply 表示完成任务的回复结构，通常为空，因为成功与否可以通过返回状态码判断。
type FinishedTaskReply struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
