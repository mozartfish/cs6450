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
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int // term when entry was received by leader

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// states for the raft servers
const (
	Follower = iota
	Candidate
	Leader
)

// Log Entry definition
type LogEntry struct {
	LogIndex int
	Term     int
	Command  interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
    dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent State on all servers
	serverState   int        // follower, candidate or leader
	lastHeartBeat time.Time  // keep track of the last time at which a peer heard from the leader
	voteCount     int        // keep track of how many total votes we have
	currentTerm   int        // latest term server has seen
	votedFor      int        // candidateID that received vote in current term
	log           []LogEntry // log entries

	// Volatile state on all servers
	commitIndex int // index of highest log entry known to be committed
	lastApplied int // index of highest log entry applied to state machine

	// Volatile state on leaders (applies to only leaders)
	nextIndex  []int // index of the next long entry to send to that server
	matchIndex []int // for each server, index of highest log entry known to be replicated on server

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.serverState == Leader
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
// Arguments passed by candidate for receiving a vote from a server
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	// 2A
	Term         int // candidate curent term
	CandidateID  int // candidate requesting vote
	LastLogIndex int // index of candidates last log entry
	LastLogTerm  int // term of candidate's last log entry

}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
// Arguments from the server regarding the vote result
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // current term for candidate to update itself
	VoteGranted bool // true means received vote
}

// Function to process requests from candidates to servers
func (rf * Raft) processRequests(server int, args RequestVoteArgs, reply RequestVoteReply) {
	// send request to server
	ok := rf.sendRequestVote(server, &args, &reply)
	if !ok {
		log.Fatalf("Candidate vote request to server failed!\n")
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
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
// A false return ca	// reply.Term = rf.currentTerm
// reply.VoteGranted = lement your own timeouts around Call().
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

// RPC request to request vote information from server that receives a message from candidate
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	fmt.Printf("Candidate has requested my vote! =>")
	fmt.Printf("The candidate information!\n")
	fmt.Printf("Candidate term: %v\n", args.Term)
	fmt.Printf("Candidate ID: %v\n", args.CandidateID)
	fmt.Printf("Candidate lastLogIndex: %v\n", args.LastLogIndex)
	fmt.Printf("Candidate lastLogTerm: %v\n", args.LastLogTerm)

	// rf refers to the current server
	// Figure 2 Request Vote RPC Receiver Implementation
	if (args.Term < rf.currentTerm) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false 
	}

	if (rf.votedFor == - 1 || rf.votedFor == args.CandidateID) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
	}
	
	return nil
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

	// Your code here (2B).

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

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	electionTimeOut := time.Duration(rand.Intn(300-150)+150) * time.Millisecond

	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mu.Lock()
		// create a new election time out
		if (rf.lastHeartBeat.Add(electionTimeOut)).Before(time.Now()) {
			// become candidate logic (can put it into a function)
			// increment current term
			rf.currentTerm = rf.currentTerm + 1
			// vote for self
			rf.votedFor = rf.me
			// reset timer
			rf.lastHeartBeat = time.Now()
			electionTimeOut = time.Duration(rand.Intn(300-150)+150) * time.Millisecond
			rf.voteCount = 1
			args := RequestVoteArgs{}
			args.CandidateID = rf.me
			args.Term = rf.currentTerm
			args.LastLogIndex = 0
			args.LastLogTerm = 0
			reply := RequestVoteReply{}
			// send request vote rpc to each peer
			for i := 0; i < len(rf.peers); i++ {
				// need a go routine to ping the other servers for their vote
				go processRequests(i, args, reply)
			}
		}
		rf.mu.Unlock()
		time.Sleep(10)

		// if (time.Now().Sub(rf.orderTime))

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

	// Your initialization code here (2A, 2B, 2C).

	// Lab 2A Initialization
	rf.serverState = Follower
	rf.lastHeartBeat = time.Now()
	rf.voteCount = 0

	// Persistent state on all servers
	rf.currentTerm = 0
	rf.votedFor = -1             // -1 represents null from the RAFT paper definition
	rf.log = make([]LogEntry, 1) // first index is 1 for the log so need a placeholder for 0

	// Volatile state on all servers
	rf.commitIndex = 0
	rf.lastApplied = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	// send off heart beat routine to check for leader

	return rf
}
