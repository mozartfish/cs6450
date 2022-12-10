package kvraft

import (
	// "fmt"
	"log"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClerkRequestName string
	Key              string
	Value            string
	Operation        string // op - Get, Put, Append
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	store       map[string]string // key-value store data structure
	lastApplied map[string]bool   // data structure for handling multiple requests for the same command
	lastTerm    int               // keep track of the previous term
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{}
	op.ClerkRequestName = strconv.Itoa(args.ClerkID) + "-" + strconv.Itoa(args.RequestID)
	op.Key = args.Key
	op.Value = ""
	op.Operation = "Get"

	kv.mu.Lock()
	currentTerm := kv.lastTerm
	value, ok := kv.lastApplied[op.ClerkRequestName]
	isLeader := false
	if !ok {
		_, term, isLeader := kv.rf.Start(op)
		if isLeader {
			kv.lastApplied[op.ClerkRequestName] = false
			value = false
			kv.lastTerm = term
		}
	}
	kv.mu.Unlock()

	if value {
		kv.mu.Lock()
		reply.Err = OK
		reply.Value = kv.store[args.Key]
		kv.mu.Unlock()
		return
	}

	term, isLeader := kv.rf.GetState()
	if !isLeader || currentTerm != term {
		if ok {
			kv.mu.Lock()
			delete(kv.lastApplied, op.ClerkRequestName)
			kv.mu.Unlock()
		}
		reply.Err = ErrWrongLeader
		return
	}

	startTime := time.Now()
	for !kv.killed() {

		kv.mu.Lock()
		if kv.lastApplied[op.ClerkRequestName] {
			reply.Err = OK
			reply.Value = kv.store[args.Key]
			kv.mu.Unlock()
			return
		}

		if kv.lastApplied[op.ClerkRequestName] {
			term, isLeader := kv.rf.GetState()
			if !isLeader || currentTerm != term {
				reply.Err = ErrWrongLeader
				delete(kv.lastApplied, op.ClerkRequestName)
				kv.mu.Unlock()
				return
			}
		}

		if time.Since(startTime) >= (500 * time.Millisecond) {
			reply.Err = ErrNoKey
			kv.mu.Unlock()
			return
		}
		kv.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{}
	op.ClerkRequestName = strconv.Itoa(args.ClerkID) + "-" + strconv.Itoa(args.RequestID)
	op.Key = args.Key
	op.Value = args.Value
	op.Operation = args.Op

	kv.mu.Lock()
	currentTerm := kv.lastTerm
	value, ok := kv.lastApplied[op.ClerkRequestName]
	isLeader := false
	if !ok {
		_, term, isLeader := kv.rf.Start(op)
		if isLeader {
			kv.lastApplied[op.ClerkRequestName] = false
			kv.lastTerm = term
			value = false
		}
	}
	kv.mu.Unlock()

	if value {
		reply.Err = OK
		return
	}

	term, isLeader := kv.rf.GetState()
	if !isLeader || currentTerm != term {
		if ok {
			kv.mu.Lock()
			delete(kv.lastApplied, op.ClerkRequestName) // remove lastApplied entry if the term changes and ok
			kv.mu.Unlock()
		}
		reply.Err = ErrWrongLeader
		return
	}


	startTime := time.Now()
	for !kv.killed() {
		kv.mu.Lock()
		if kv.lastApplied[op.ClerkRequestName] {
			reply.Err = OK
			kv.mu.Unlock()
			return
		}

		if kv.lastApplied[op.ClerkRequestName] {
			term, isLeader = kv.rf.GetState()
			if !isLeader || currentTerm != term {
				reply.Err = ErrWrongLeader
				delete(kv.lastApplied, op.ClerkRequestName)
				kv.mu.Unlock()
				return
			}
		}

		if time.Since(startTime) >= (500 * time.Millisecond) {
			reply.Err = ErrNoKey
			kv.mu.Unlock()
			return
		}
		kv.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

// The ReadApplyMsg go routine starts a background go routine to apply commands and operations from commands
// that have been replicated and applied on all the servers
func (kv *KVServer) ReadApplyMsg() {
	for !kv.killed() {
		msg := <-kv.applyCh
		// fmt.Printf("GOT VALUE ON APPLY CHANNEL, %v\n", msg)
		op := msg.Command.(Op)
		kv.mu.Lock()
		if !kv.lastApplied[op.ClerkRequestName] {
			switch op.Operation {
			case "Put":
				kv.store[op.Key] = op.Value
			case "Append":
				kv.store[op.Key] += op.Value
			}
			kv.lastApplied[op.ClerkRequestName] = true
		}
		kv.mu.Unlock()
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.store = make(map[string]string)
	kv.lastApplied = make(map[string]bool)
	go kv.ReadApplyMsg()

	// You may need initialization code here.

	return kv
}
