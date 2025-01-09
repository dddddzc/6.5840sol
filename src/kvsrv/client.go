package kvsrv

import (
	"crypto/rand"
	"math/big"

	"6.5840/labrpc"
)

type Clerk struct {
	server *labrpc.ClientEnd
	// You will have to modify this struct.
	clientID  int64
	requestID int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
	// You'll have to add code here.
	ck.clientID = nrand() // 为每个客户端生成唯一的客户端ID
	ck.requestID = 0      // 初始化该客户端的请求序列号,从0开始
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	args := GetArgs{
		Key:       key,
		ClientID:  ck.clientID,
		RequestID: ck.requestID,
	}
	reply := GetReply{}
	// 失败后重试请求
	for {
		ok := ck.server.Call("KVServer.Get", &args, &reply)
		if ok {
			// 成功后再将ck.requestID加1
			// 并且告知server,可以删除ClientState
			ck.requestID++
			deleteArgs := GetArgs{
				ClientID: ck.clientID,
				Success:  true,
			}
			deleteReply := GetReply{}
			ck.server.Call("KVServer.Get", &deleteArgs, &deleteReply)
			return reply.Value
		}
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) string {
	// You will have to modify this function.
	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		ClientID:  ck.clientID,
		RequestID: ck.requestID,
	}
	reply := PutAppendReply{}

	for {
		ok := ck.server.Call("KVServer."+op, &args, &reply)
		if ok {
			ck.requestID++
			// 统一用Get方法发送删除请求,反正不影响
			deleteArgs := GetArgs{
				ClientID: ck.clientID,
				Success:  true,
			}
			deleteReply := GetReply{}
			ck.server.Call("KVServer.Get", &deleteArgs, &deleteReply)
			return reply.Value
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}
