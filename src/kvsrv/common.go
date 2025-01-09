package kvsrv

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op        string // operation type: "Put", "Append"
	ClientID  int64
	RequestID int64
	Success   bool // if request OK, inform server to delete ClientState
}

type PutAppendReply struct {
	Value string
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientID  int64
	RequestID int64
	Success   bool
}

type GetReply struct {
	Value string
}
