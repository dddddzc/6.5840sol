package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type ClientState struct {
	lastRequestID int64  // 客户端上一次请求的序列号
	lastReply     string // 客户端上一次请求的回复
}

type KVServer struct {
	mu sync.Mutex
	// Your definitions here.
	store   map[string]string
	clients map[int64]ClientState // 用map存储每个客户端的状态
}

// 返回当前key对应的value，如果key不存在则返回空字符串
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// 检查是否是通知删除的请求
	if args.Success {
		delete(kv.clients, args.ClientID)
		return
	}
	DPrintf("Get request: %v", args)

	// 如果是重复请求,直接返回之前的回复
	cs, exists := kv.clients[args.ClientID]
	if exists && args.RequestID == cs.lastRequestID {
		reply.Value = cs.lastReply
		return
	}

	// 正常执行Get操作
	value, ok := kv.store[args.Key]
	if ok {
		reply.Value = value
	} else {
		reply.Value = ""
	}

	// 更新客户端状态
	cs.lastRequestID = args.RequestID
	cs.lastReply = reply.Value
	kv.clients[args.ClientID] = cs
}

// 如果key存在,则更新kv; 否则创建新的kv
func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// 检查是否是通知删除的请求
	if args.Success {
		delete(kv.clients, args.ClientID)
		return
	}
	DPrintf("Put request: %v", args)

	// 如果是重复请求,直接返回之前的回复
	cs, exists := kv.clients[args.ClientID]
	if exists && args.RequestID == cs.lastRequestID {
		reply.Value = cs.lastReply
		return
	}

	// 正常执行Put操作
	kv.store[args.Key] = args.Value
	reply.Value = args.Value

	// 更新客户端状态
	cs.lastRequestID = args.RequestID
	cs.lastReply = reply.Value
	kv.clients[args.ClientID] = cs
}

// 返回值:args.key对应的old value
// 更改:将args.key对应的old value更改为old value + args.value
func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// 检查是否是通知删除的请求
	if args.Success {
		delete(kv.clients, args.ClientID)
		return
	}
	DPrintf("Append request: %v", args)

	// 如果是重复请求,直接返回之前的回复
	cs, exists := kv.clients[args.ClientID]
	if exists && args.RequestID == cs.lastRequestID {
		reply.Value = cs.lastReply
		return
	}

	// 正常执行Append请求
	reply.Value = kv.store[args.Key]
	kv.store[args.Key] += args.Value

	// 更新客户端状态
	cs.lastRequestID = args.RequestID
	cs.lastReply = reply.Value
	kv.clients[args.ClientID] = cs
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.store = make(map[string]string)
	kv.clients = make(map[int64]ClientState)
	return kv
}
