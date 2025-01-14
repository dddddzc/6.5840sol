package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// 状态
type State int

const (
	FOLLOWER State = iota
	CANDIDATE
	LEADER
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// 日志条目
type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int // 候选人的当前任期号
	CandidateId  int // 候选人的ID
	LastLogIndex int // 候选人最后一个日志的索引
	LastLogTerm  int // 候选人最后一个日志的任期号
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
// 注意 Index 和 Term 都从1开始
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int  // 投票节点的当前任期号
	VoteGranted bool // 是否投票给该candidate
}

type AppendEntriesArgs struct {
	// Your data here (3A, 3B).
	Term         int        // leader的当前任期号
	LeaderId     int        // leader的ID,让follower重定位给clients
	PrevLogIndex int        // 最新日志条目之前的日志条目的索引
	PrevLogTerm  int        // 最新日志条目之前的日志条目的任期号
	Entries      []LogEntry // 要追加的日志条目,空则为心跳
	LeaderCommit int        // leader已知的已提交日志的最大索引
}

type AppendEntriesReply struct {
	// Your data here (3A).
	Term    int  // follower的当前任期号,用于leader更新自己的任期号,初始为1
	Success bool // 如果follower的条目匹配PrevLogIndex和PrevLogTerm,返回true
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// 与状态相关
	state             State         // 该节点的状态:leader, candidate, follower
	lastHeartbeatTime time.Time     // 上次收到心跳的时间,便于ticker确定是否需要开始选举
	electionTimeout   time.Duration // 选举超时时间,150ms~300ms

	// 需要持久化存储
	currentTerm int        // 该节点已知的最新任期号,初始化0
	votedFor    int        // 在当前任期内被该节点投票的candidateID,初始化-1
	log         []LogEntry // 日志集合,Index初始化为1,Index=0为dummyHead

	// 不需要持久化存储
	commitIndex int // 已知的已提交的日志条目的最大索引,初始化0
	lastApplied int // 已知的已应用于状态机的日志条目的最大索引,初始化0

	// 作为leader时需要,每次选举后重新初始化
	nextIndex  []int // 对于每个节点，需要发送给该节点的下一个日志条目的索引,初始化为 leader.LastLogIndex+1
	matchIndex []int // 对于每个节点，已被复制到该节点的日志条目的最大索引,初始化为0
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == LEADER
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("Term %d : node %d received RequestVote from node %d", args.Term, rf.me, args.CandidateId)

	// 初始化回复
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// 如果候选者的任期号小于当前任期号, 拒绝请求
	if args.Term < rf.currentTerm {
		return
	}

	// 如果候选者的任期号大于当前任期号, 更新当前任期号并转换为 Follower
	if args.Term > rf.currentTerm {
		rf.backToFollower(args.Term)
	}

	// 满足条件,投票
	if rf.votedFor == -1 && rf.isCandidateLogUpToDate(args) {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
	}
}

// example RequestVote RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("Term %d : node %d received AppendEntries from node %d", args.Term, rf.me, args.LeaderId)

	// 初始化回复
	reply.Term = rf.currentTerm
	reply.Success = false

	// 1.如果领导者的任期号小于当前任期号，拒绝请求
	if args.Term < rf.currentTerm {
		return
	}

	// candidate收到leader的心跳 或者 follower收到leader的心跳
	if args.Term >= rf.currentTerm {
		rf.backToFollower(reply.Term)
	}

	// 2.如果日志匹配PrevLogIndex和PrevLogTerm，同意请求
	if len(rf.log) > args.PrevLogIndex && rf.log[args.PrevLogIndex].Term == args.PrevLogTerm {
		reply.Success = true
		rf.resetTimer()
		return
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		if rf.state == LEADER {
			ms := 100 + (rand.Int63() % 50)
			time.Sleep(time.Duration(ms) * time.Millisecond)
			rf.broadcastHeartbeat()
		} else if time.Since(rf.lastHeartbeatTime) > rf.electionTimeout {
			// 无论是follower还是candidate,只要超过一段时间没收到心跳,则开始下一次选举
			// 任期加1->转换为candidate->投票给自己->并行地给发送 RequestVote RPCs 给其他所有节点
			rf.currentTerm++
			rf.state = CANDIDATE
			rf.votedFor = rf.me
			rf.broadcastRequestVote()
		}
		rf.mu.Unlock()
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.state = FOLLOWER
	rf.lastHeartbeatTime = time.Now()
	rf.electionTimeout = time.Duration(150+rand.Int63()%100) * time.Millisecond
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 1) // log的第一个元素不使用,作为dummyHead
	rf.log = append(rf.log, LogEntry{Index: 0, Term: 0, Command: nil})
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func (rf *Raft) broadcastRequestVote() {
	DPrintf("Term %d : node %d broadcastRequestVote", rf.currentTerm, rf.me)
	if rf.state != CANDIDATE {
		panic("Invalid state for broadcastRequestVote")
	}

	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm:  rf.getLastLogTerm(),
	}

	receivedVotes := 1 // vote for itself
	for i := range rf.peers {
		if rf.state != CANDIDATE {
			break
		}
		if i == rf.me {
			continue
		}
		go func(peer int) {
			reply := &RequestVoteReply{}
			rf.sendRequestVote(peer, args, reply)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			// follower的Term比当前candidate的Term大,退回follower状态
			if reply.Term > rf.currentTerm {
				rf.backToFollower(reply.Term)
			}
			if reply.VoteGranted {
				DPrintf("Term %d : node %d received vote from node %d", rf.currentTerm, rf.me, peer)
				receivedVotes++
				if receivedVotes > len(rf.peers)/2 && rf.state != LEADER {
					rf.state = LEADER
					DPrintf("Term %d : node %d become leader", rf.currentTerm, rf.me)
					rf.broadcastHeartbeat()
				}
			}
		}(i)
	}
}

// leader给除自己以外的节点广播心跳
func (rf *Raft) broadcastHeartbeat() {
	DPrintf("Term %d : node %d broadcastHeartbeat", rf.currentTerm, rf.me)
	if rf.state != LEADER {
		panic("Invalid state for broadcastHeartbeat")
	}

	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: rf.getLastLogIndex(),
		PrevLogTerm:  rf.getLastLogTerm(),
		Entries:      nil,
		LeaderCommit: rf.commitIndex,
	}

	for i := range rf.peers {
		if rf.state != LEADER {
			break
		}
		if i == rf.me {
			continue
		}
		go func(peer int) {
			reply := &AppendEntriesReply{}
			rf.sendAppendEntries(peer, args, reply)
			if reply.Success {
				DPrintf("Term %d : node %d received match from node %d", rf.currentTerm, rf.me, peer)
			}
			if reply.Term > rf.currentTerm {
				rf.backToFollower(reply.Term)
			}
		}(i)
	}
}

// 调用者应持有锁
func (rf *Raft) getLastLogIndex() int {
	return rf.log[len(rf.log)-1].Index
}
func (rf *Raft) getLastLogTerm() int {
	return rf.log[len(rf.log)-1].Term
}

// 重置心跳定时器
func (rf *Raft) resetTimer() {
	rf.electionTimeout = time.Duration(2000+rand.Int63()%1000) * time.Millisecond
	rf.lastHeartbeatTime = time.Now()
}

// 检查检查候选者的日志是否至少和接收者的日志一样新
func (rf *Raft) isCandidateLogUpToDate(args *RequestVoteArgs) bool {
	lastLogIndex := rf.getLastLogIndex()
	lastLogTerm := rf.getLastLogTerm()
	return args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)
}

func (rf *Raft) backToFollower(newTerm int) {
	rf.currentTerm = newTerm
	rf.state = FOLLOWER
	rf.votedFor = -1
	rf.resetTimer()
}
