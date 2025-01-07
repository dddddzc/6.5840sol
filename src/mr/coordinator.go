package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskStatus int

// 任务有四种状态:未开始,进行中,已完成,失败
const (
	Idle TaskStatus = iota
	Assigned
	Completed
	Failed
)

// 每个任务的信息:状态,文件,已运行的时间
type TaskInfo struct {
	FileName  string
	Status    TaskStatus
	TimeStamp time.Time
}

type Coordinator struct {
	// Your definitions here.
	NMap        int
	NReduce     int
	MapDone     bool
	ReduceDone  bool
	MapTasks    []TaskInfo
	ReduceTasks []TaskInfo
	Mu          sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
// 1.首先选择 未分配的Map任务 或 执行失败的Map任务 或 超时的Map任务
// 2.如果以上Map任务均不存在,但Map又没有全部执行完成,则执行WaitTask
// 3.MapDone && !ReduceDone, 按照1和2的逻辑进行Reduce任务的分配
// 4.MapDone && ReduceDone, 告知worker执行ExitTask
func (c *Coordinator) GetTask(args *MessageSend, reply *MessageReply) error {
	c.Mu.Lock()
	defer c.Mu.Unlock()

	// 先处理MapTasks
	if !c.MapDone && !c.ReduceDone {
		MapTaskCompleted := 0
		for i, MapTaskInfo := range c.MapTasks {
			if MapTaskInfo.Status == Idle || MapTaskInfo.Status == Failed ||
				(MapTaskInfo.Status == Assigned && time.Since(MapTaskInfo.TimeStamp) > 10*time.Second) {
				// 开始分配任务
				reply.TaskID = i
				reply.TaskFile = MapTaskInfo.FileName
				reply.TaskType = MapTask
				reply.NMap = c.NMap
				reply.NReduce = c.NReduce // 用于worker在shuffle阶段的划分
				c.MapTasks[i].Status = Assigned
				c.MapTasks[i].TimeStamp = time.Now()
				fmt.Printf("分配Map任务:MapTaskID:%d-TaskFile:%s\n", reply.TaskID, reply.TaskFile)
				return nil
			} else if MapTaskInfo.Status == Completed {
				MapTaskCompleted++
			}
		}

		if MapTaskCompleted == c.NMap {
			c.MapDone = true
			fmt.Println("Coordinator:Map任务已全部完成!")
		} else {
			reply.TaskType = WaitTask
			return nil
		}
	}

	// If all map tasks are done, handle reduce tasks
	if c.MapDone && !c.ReduceDone {
		ReduceTaskCompleted := 0
		for i, ReduceTaskInfo := range c.ReduceTasks {
			if ReduceTaskInfo.Status == Idle || ReduceTaskInfo.Status == Failed ||
				(ReduceTaskInfo.Status == Assigned && time.Since(ReduceTaskInfo.TimeStamp) > 10*time.Second) {
				// 开始分配任务
				reply.TaskID = i
				reply.TaskFile = ReduceTaskInfo.FileName
				reply.TaskType = ReduceTask
				reply.NMap = c.NMap
				reply.NReduce = c.NReduce
				c.ReduceTasks[i].Status = Assigned
				c.ReduceTasks[i].TimeStamp = time.Now()
				fmt.Printf("分配Reduce任务:ReduceTaskID:%d\n", reply.TaskID)
				return nil
			} else if ReduceTaskInfo.Status == Completed {
				ReduceTaskCompleted++
			}
		}

		if ReduceTaskCompleted == c.NReduce {
			c.ReduceDone = true
			fmt.Println("Coordinator:Reduce任务已全部完成!")
		} else {
			reply.TaskType = WaitTask
			return nil
		}
	}

	// If both map and reduce tasks are done, exit
	reply.TaskType = ExitTask
	return nil
}

func (c *Coordinator) ReportTask(args *MessageSend, reply *MessageReply) error {
	c.Mu.Lock()
	defer c.Mu.Unlock()

	if args.TaskCompletedStatus == MapTaskCompleted {
		c.MapTasks[args.TaskID].Status = Completed
	} else if args.TaskCompletedStatus == MapTaskFailed {
		c.MapTasks[args.TaskID].Status = Failed
	} else if args.TaskCompletedStatus == ReduceTaskCompleted {
		c.ReduceTasks[args.TaskID].Status = Completed
	} else if args.TaskCompletedStatus == ReduceTaskFailed {
		c.ReduceTasks[args.TaskID].Status = Failed
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
	c.Mu.Lock()
	defer c.Mu.Unlock()
	return c.MapDone && c.ReduceDone
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// 初始化Coordinator的参数
	c := Coordinator{
		NMap:        len(files),
		NReduce:     nReduce,
		MapTasks:    make([]TaskInfo, len(files)),
		ReduceTasks: make([]TaskInfo, nReduce),
	}

	// 初始化MapTasks和ReduceTasks的TaskInfo
	c.InitTask(files)
	// 调用server函数,启动对RPC任务的监听
	c.server()
	return &c
}

func (c *Coordinator) InitTask(file []string) {
	for i, file := range file {
		c.MapTasks[i] = TaskInfo{
			FileName:  file,
			Status:    Idle,
			TimeStamp: time.Now(),
		}
	}
	for i := range c.ReduceTasks {
		c.ReduceTasks[i] = TaskInfo{
			Status: Idle,
		}
	}
}
