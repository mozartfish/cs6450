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
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// Server State Definitions
const (
	Follower = iota
	Candidate
	Leader
)

// Log Entry Definition
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
	lastHeartBeat time.Time  // keep track of the last time which peer heard from the leader
	voteCount     int        // keep track of how many total votes we have
	currentTerm   int        // latest term server has seen
	votedFor      int        // candidateID that received vote in current term
	log           []LogEntry // the log shared among all peers

	// Volatile State on all servers
	commitIndex int // index of highest log entry known to be committed - initialized to 0 and increases monotonically
	lastApplied  int // index of highest log entry applied to state machine - initialized to O and increases monotonically

	// Volatile State on all laders (applies to leaders only)
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
	term = rf.currentTerm
	isleader = (rf.serverState == Leader)
	// Your code here (2A).
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
type RequestVoteArgs struct {
	// Your data here (2A, 2B).

	// 2A
	Term        int // candidate term
	CandidateID int // candidate requesting vote

	// 2B
	LastLogIndex int // Index of candidate's last log entry
	LastLogTerm  int // Term of candidate's last loge entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// AppendEntries Args
type AppendEntriesArgs struct {
	Term         int        // leader's term
	LeaderID     int        // so follower can redirect clients
	PrevLogIndex int        // Index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store
	LeaderCommit int        // Leader commit index
}

// AppendEntries Reply
type AppendEntriesReply struct {
	Term    int  // current term for leader to update itself
	Success bool // true if follower contained entry matching previous log index and previous log term
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	// 2A
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// rf refers to the current server
	// Figure 2 Request Vote RPC Receiver Implementation

	// // candidate term is behind follower term
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// candidate term is ahead of follower term
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.serverState = Follower
		rf.voteCount = 0
	}

	// voted for is null or candidate id
	if rf.votedFor == -1 || rf.votedFor == args.CandidateID {
		rf.lastHeartBeat = time.Now() // reset timer
		rf.votedFor = args.CandidateID
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		return
	}
}

// // RPC Call to Append Entries function
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// leader term is behind follower term
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// if rf.log[args.PrevLogIndex].Term < args.PrevLogTerm {
	// 	reply.Term = rf.currentTerm
	// 	reply.Success = false
	// 	return 
	// }
	
	// leader is ahead of the follower
	if args.Term >= rf.currentTerm {
		rf.currentTerm = args.Term
		rf.serverState = Follower
		rf.lastHeartBeat = time.Now()
		reply.Term = rf.currentTerm
		reply.Success = true
		return
	}
}

// Intermediate function to handle race conditions and overriding for request vote
func (rf *Raft) ProcessRequestVote(server int, args RequestVoteArgs, reply RequestVoteReply) {
	// send request to server
	ok := rf.sendRequestVote(server, &args, &reply)
	if !ok {
		fmt.Println("Process Request Vote RPC failed!")
	} else {
		rf.mu.Lock()
		// if rpc response contains term T > currentTerm: convert candidate to follower
		if reply.Term > rf.currentTerm {
			rf.serverState = Follower
			rf.currentTerm = reply.Term
			rf.voteCount = 0
		}
		if rf.serverState == Candidate && rf.currentTerm == args.Term {
			if reply.VoteGranted {
				rf.voteCount++
				if rf.voteCount >= len(rf.peers)/2+1 {
					rf.serverState = Leader
				}
			}
		}

		rf.mu.Unlock()
	}
}

// Intermediate function to handle race conditions and overriding for append entries
func (rf *Raft) ProcessAppendEntries(server int, args AppendEntriesArgs, reply AppendEntriesReply) {
	ok := rf.sendAppendEntries(server, &args, &reply)
	if !ok {
		fmt.Printf("ProcessAppendEntries RPC Failed!\n")
	}
	rf.mu.Lock()
	if reply.Term > rf.currentTerm {
		rf.serverState = Follower
		rf.currentTerm = reply.Term
	}
	rf.mu.Unlock()
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
	// fmt.Printf("Server: %v\n", server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {
		fmt.Print("SendRequestVote RPC Failed!\n")
	}
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if !ok {
		fmt.Printf("SendAppendEntries Failed!\n")
	}
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

	// Your code here (2B).
	if rf.serverState != Leader {
		isLeader = false
	}

	index = rf.commitIndex
	term = rf.currentTerm


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
	electionTimeOut := time.Duration(rand.Intn(300-150)+150) * time.Millisecond // election time out window
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.serverState != Leader {
			// Check to see if we need to start a new election
			if (rf.lastHeartBeat.Add(electionTimeOut)).Before(time.Now()) {
				rf.currentTerm = rf.currentTerm + 1 // increment current term
				rf.serverState = Candidate          // change to candidate
				rf.votedFor = rf.me                 // vote for self
				rf.lastHeartBeat = time.Now()       // reset timer
				electionTimeOut = time.Duration(rand.Intn(300-150)+150) * time.Millisecond // reset election time out 
				rf.voteCount = 1

				// Request Vote Args
				args := RequestVoteArgs{}
				args.CandidateID = rf.me
				args.Term = rf.currentTerm
				args.LastLogIndex = 0
				args.LastLogTerm = 0

				// Request Vote Reply
				reply := RequestVoteReply{}

				// Issue vote requests in parallel to all other servers
				for i := 0; i < len(rf.peers); i++ {
					// peer cannot vote again for itself
					if i == rf.me {
						continue
					}
					go rf.ProcessRequestVote(i, args, reply)
				}
			}
		}
		// heart beat + election timeout < current time
		if rf.serverState == Leader {
			// Volatile State on leaders 
			rf.nextIndex = make([]int, len(rf.log))
			rf.matchIndex = make([]int, 0)
			args := AppendEntriesArgs{}
			args.Term = rf.currentTerm
			args.LeaderID = rf.me
			reply := AppendEntriesReply{}
			for i := 0; i < len(rf.peers); i++ {
				// peer cannot append stuff to itself
				if i == rf.me {
					continue
				}
				go rf.ProcessAppendEntries(i, args, reply)
			}
		}

		rf.mu.Unlock()
		time.Sleep(time.Duration(10) * time.Millisecond)
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

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

	// 2A
	// Persistent State on all servers
	rf.serverState = Follower
	rf.lastHeartBeat = time.Now()
	rf.voteCount = 0
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 1)

	// Volatile State on all servers
	rf.commitIndex = 0
	rf.lastApplied = 0

	// Volatile State on leaders
	rf.nextIndex = make([]int, len(rf.log))
	rf.matchIndex = make([]int, 0)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
