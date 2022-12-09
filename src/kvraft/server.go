package kvraft

import (
	"fmt"
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
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{}
	op.ClerkRequestName = strconv.Itoa(args.ClerkID) + "-" + strconv.Itoa(args.RequestID)
	op.Key = args.Key
	op.Value = ""
	op.Operation = "Get"

	_, _, isLeader := kv.rf.Start(op)

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	fmt.Printf("START RAFT GET AGREEMENT\n")

	kv.mu.Lock()
	kv.lastApplied[op.ClerkRequestName] = false
	kv.mu.Unlock()

	startTime := time.Now()
	for !kv.killed() {
		_, isLeader = kv.rf.GetState()
		kv.mu.Lock()
		if kv.lastApplied[op.ClerkRequestName] && isLeader {
			reply.Err = OK
			reply.Value = kv.store[args.Key]
			fmt.Printf("GET SUCCESS\n")
			fmt.Printf("KV STORE: %v\n", kv.store)
			kv.mu.Unlock()
			return
		}

		if !isLeader {
			reply.Err = ErrWrongLeader
			fmt.Printf("LEADER CHANGED GET, KV STORE: %v\n", kv.store)
			kv.mu.Unlock()
			return
		}

		if time.Since(startTime) >= (300 * time.Millisecond) {
			reply.Err = ErrNoKey
			fmt.Printf("GET TIME OUT\n")
			kv.mu.Unlock()
			return
		}
		kv.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}

	// reply.Err = OK

	// value := <-kv.applyCh
	// cmd := value.Command.(Op)
	// reply.Value = kv.store[cmd.Key]
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{}
	op.ClerkRequestName = strconv.Itoa(args.ClerkID) + "-" + strconv.Itoa(args.RequestID)
	op.Key = args.Key
	op.Value = args.Value
	op.Operation = args.Op
	// if args.Op == "Put" {
	// 	op.Operation = "Put"
	// }

	// if op.Operation == "Append" {
	// 	op.Operation = "Append"
	// }
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	fmt.Printf("START RAFT PUTAPPEND AGREEMENT\n")
	kv.mu.Lock()
	kv.lastApplied[op.ClerkRequestName] = false
	kv.mu.Unlock()

	startTime := time.Now()
	for !kv.killed() {
		_, isLeader = kv.rf.GetState()
		kv.mu.Lock()
		if kv.lastApplied[op.ClerkRequestName] && isLeader {
			reply.Err = OK
			fmt.Printf("PUTAPPEND SUCCESS\n")
			fmt.Printf("KV STORE: %v\n", kv.store)
			kv.mu.Unlock()
			return
		}

		if !isLeader {
			reply.Err = ErrWrongLeader
			fmt.Printf("LEADER CHANGED PUTAPPEND, KV STORE: %v\n", kv.store)
			kv.mu.Unlock()
			return
		}

		if time.Since(startTime) >= (300 * time.Millisecond) {
			reply.Err = ErrNoKey
			fmt.Printf("PUTAPPEND TIME OUT\n")
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
	for {
		msg := <-kv.applyCh
		fmt.Printf("GOT VALUE ON APPLY CHANNEL, %v\n", msg)
		op := msg.Command.(Op)
		kv.mu.Lock()
		if !kv.lastApplied[op.ClerkRequestName] {
			switch op.Operation {
			case "Get":
				// op.Value = kv.store[op.Key]
			case "Put":
				kv.store[op.Key] = op.Value
			case "Append":
				kv.store[op.Key] += op.Value
			}
			kv.lastApplied[op.ClerkRequestName] = true
		}
		kv.mu.Unlock()
		// time.Sleep(time.Duration(1) * time.Millisecond)
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
