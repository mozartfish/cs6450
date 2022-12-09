package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderServer int // server ID which turned out to be leader. Raft Peers are mapped to kv servers one to one
	clientID     int // unique clerk that sends a request to key value servers
	requestID    int // unique request associated with a unique clerk
	mu           sync.Mutex
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.leaderServer = 0
	ck.clientID = int(nrand())
	ck.requestID = 0
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	var currentValue = ""
	ck.mu.Lock()
	clerkID := ck.clientID
	requestID := ck.requestID
	requestID += 1
	ck.mu.Unlock()
	i := ck.leaderServer
	for {
		// Get Args
		args := GetArgs{}
		args.Key = key
		args.ClerkID = clerkID
		args.RequestID = requestID
		// Get Reply
		reply := GetReply{}
		ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
		if ok && reply.Err == OK {
			ck.mu.Lock()
			ck.leaderServer = i
			ck.mu.Unlock()
			currentValue = reply.Value
			break
		}
		i = (i + 1) % len(ck.servers)
	}
	return currentValue
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.mu.Lock()
	clerkID := ck.clientID
	requestID := ck.requestID
	requestID += 1
	ck.mu.Unlock()
	i := ck.leaderServer
	for {
		// PutAppend Args
		args := PutAppendArgs{}
		if op == "Put" {
			args.Op = op
		} else if op == "Append" {
			args.Op = op
		}
		args.Key = key
		args.Value = value
		args.ClerkID = clerkID
		args.RequestID = requestID

		// PutAppend Reply
		reply := PutAppendReply{}

		ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)

		if ok && reply.Err == OK {
			ck.mu.Lock()
			ck.leaderServer = i
			ck.mu.Unlock()
			return
		}
		i = (i + 1) % len(ck.servers)
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
