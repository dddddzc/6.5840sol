package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
// 实现了sort.Interface接口的三个方法
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func HandleMapTask(reply *MessageReply, mapf func(string, string) []KeyValue) {
	fmt.Printf("接收Map任务:TaskID:%d-TaskFile:%s\n", reply.TaskID, reply.TaskFile)
	// 读取文件内容
	file, err := os.Open(reply.TaskFile)
	if err != nil {
		log.Fatalf("cannot open %v", reply.TaskFile)
		return
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.TaskFile)
	}
	file.Close()

	// 调用Map函数
	kva := mapf(reply.TaskFile, string(content))
	// 将Map函数的输出结果shuffle保存成NReduce份
	intermediate := make([][]KeyValue, reply.NReduce)
	for _, kv := range kva {
		reduceIdx := ihash(kv.Key) % reply.NReduce
		intermediate[reduceIdx] = append(intermediate[reduceIdx], kv)
	}
	// 将intermediate写入多个文件
	for reduceIdx, kva := range intermediate {
		oname := fmt.Sprintf("mr-%v-%v", reply.TaskID, reduceIdx)
		ofile, err := os.CreateTemp("", oname)
		if err != nil {
			log.Fatalf("cannot create temp file %v", oname)
		}
		enc := json.NewEncoder(ofile)
		for _, kv := range kva {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("cannot encode %v", kv)
			}
		}
		ofile.Close()
		os.Rename(ofile.Name(), oname)
	}
	args := MessageSend{TaskID: reply.TaskID, TaskCompletedStatus: MapTaskCompleted}
	call("Coordinator.ReportTask", &args, &MessageReply{})
}

func HandleReduceTask(reply *MessageReply, reducef func(string, []string) string) {
	fmt.Printf("接收Reduce任务:TaskID:%d\n", reply.TaskID)
	// 存储mr-N-M聚合key-value对
	var intermediateKV []KeyValue
	// 获取要处理的文件
	var intermediateFiles []string
	for MapID := 0; MapID < reply.NMap; MapID++ {
		intermediateFiles = append(intermediateFiles, fmt.Sprintf("mr-%v-%v", MapID, reply.TaskID))
	}
	for _, filename := range intermediateFiles {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
			return
		}
		// decode the intermediate file
		dec := json.NewDecoder(file)
		for {
			kv := KeyValue{}
			if err := dec.Decode(&kv); err == io.EOF {
				break
			}
			intermediateKV = append(intermediateKV, kv)
		}
		file.Close()
	}

	// sort the intermediate key-value pairs by key
	sort.Sort(ByKey(intermediateKV))
	// write the key-value pairs to the output file
	oname := fmt.Sprintf("mr-out-%v", reply.TaskID)
	ofile, err := os.Create(oname)
	if err != nil {
		log.Fatalf("cannot create %v", oname)
		return
	}
	for i := 0; i < len(intermediateKV); {
		j := i + 1
		for j < len(intermediateKV) && intermediateKV[j].Key == intermediateKV[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediateKV[k].Value)
		}

		// call the reduce function to get the output
		output := reducef(intermediateKV[i].Key, values)

		// write the key-value pairs to the output file
		fmt.Fprintf(ofile, "%v %v\n", intermediateKV[i].Key, output)
		i = j
	}
	ofile.Close()
	// rename the output file to the final output file
	os.Rename(ofile.Name(), oname)

	// send the task completion message to the coordinator
	args := MessageSend{
		TaskID:              reply.TaskID,
		TaskCompletedStatus: ReduceTaskCompleted,
	}
	call("Coordinator.ReportTask", &args, &MessageReply{})
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		args := MessageSend{}
		reply := MessageReply{}
		ok := call("Coordinator.GetTask", &args, &reply)
		if ok {
			switch reply.TaskType {
			case MapTask:
				HandleMapTask(&reply, mapf)
			case ReduceTask:
				HandleReduceTask(&reply, reducef)
			case WaitTask:
				time.Sleep(time.Second)
			case ExitTask:
				os.Exit(0)
			default:
				fmt.Println("TaskType Error!")
			}
		} else {
			fmt.Printf("GetTask failed!\n")
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
