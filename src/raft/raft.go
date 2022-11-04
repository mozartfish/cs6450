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

	// "fmt"
	"math"
	"math/rand"
	"sort"
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

// Server State
const (
	Follower = iota
	Candidate
	Leader
)

// Log Entry
type LogEntry struct {
	Term    int         // term when entry was first received by leader
	Command interface{} // command for state machine
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
	// 2A
	// Persistent State on All Servers (updated on stable storage before responding to RPCs)
	serverState   int       // Follower, Candidate, or Leader
	lastHeartBeat time.Time // last time which peer heard from the leader
	voteCount     int       // number of votes (elections)
	currentTerm   int       // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor      int       // candidateID that received vote in current term (or null if none)

	// 2B
	log []LogEntry // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)

	// Volatile State on All Servers
	commitIndex int // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int // index of highest log entry applied to stadte machine (initialized to 0, increases monotonically)

	// Volatile State on Leaders:
	// (Reinitialized After Election)
	nextIndex  []int // for each server, index of the next log entry to send to that server (initialized to leader log + 1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
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
	isleader = (rf.serverState == Leader)
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
	Term        int // candidate's term
	CandidateID int // candidate requesting vote

	// 2B
	LastLogIndex int // index of candidate's last log entry (Section 5.4)
	LastLogTerm  int // term of candidate's last log entry (Section 5.4)
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	// 2A
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// AppendEntries RPC arguments structure
type AppendEntriesArgs struct {
	Term         int        // leader's term
	LeaderID     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of PrevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        // Leader's commit index
}

// AppendEntries RPC reply structure
type AppendEntriesReply struct {
	Term          int  // currentTerm, for leader to update itself
	Success       bool // true if follower contained entry matching prevLogIndex and prevLogTerm
	ConflictIndex int  // keep track of conflicting indexes
	ConflictTerm  int  // term for the conflicting index
	// NextIndex int // keep track of nextIndex
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// currentTerm - latest term server has seen
	// term - candidate's term
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 2A
	// 1. Reply false if term < currentTerm (Section 5.1)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// 2a. term > currentTerm
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.serverState = Follower
		rf.voteCount = 0
	}

	reply.Term = rf.currentTerm
	// 2. If votedFor is null or candidateID and candidate's log is at least up-to-date as receiver's
	// log then grant vote (Section 5.2, 5.4)
	if rf.votedFor == -1 || rf.votedFor == args.CandidateID {
		// Check if logs are more up to date
		lastLogIndex := len(rf.log) - 1
		lastLogTerm := rf.log[lastLogIndex].Term

		if lastLogTerm > args.LastLogTerm {
			reply.VoteGranted = false
			return
		}

		if lastLogTerm == args.LastLogTerm && args.LastLogIndex < lastLogIndex {
			reply.VoteGranted = false
			return
		}

		rf.votedFor = args.CandidateID
		rf.lastHeartBeat = time.Now() // reset peer heartbeat time for election
		reply.VoteGranted = true
	}

}

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 1. Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.serverState = Follower
		rf.voteCount = 0
	}

	reply.Term = rf.currentTerm
	rf.lastHeartBeat = time.Now()

	if args.PrevLogIndex > len(rf.log)-1 {
		// fmt.Printf("Ser: %v, PrevLogIndex: %v > Len(Log): %v\n", rf.me, args.PrevLogIndex, len(rf.log))
		reply.Term = rf.currentTerm
		reply.ConflictIndex = len(rf.log)
		reply.ConflictTerm = -1
		reply.Success = false
		return
	}

	// // 2. Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
		for i := 0; i < len(rf.log); i++ {
			if rf.log[i].Term == rf.log[args.PrevLogIndex].Term {
				reply.ConflictIndex = i
				break
			}
		}
		reply.Term = rf.currentTerm
		reply.Success = false
		rf.log = rf.log[:args.PrevLogIndex]
		// fmt.Printf("Server: %v, Conflict Term: %v, Conflict Index: %v\n", rf.me, reply.ConflictTerm, reply.ConflictIndex)
		return
	}

	// Check for duplicate log entries and conflicting terms
	var a int = args.PrevLogIndex + 1
	var b int = 0

	for ; a < len(rf.log) && b < len(args.Entries); a, b = a+1, b+1 {
		if rf.log[a].Term != args.Entries[b].Term {
			rf.log = rf.log[:a] // at this point, all entries up to a are the same and do not conflic
			break
		}
	}

	args.Entries = args.Entries[b:] // start append entries from index b

	if len(args.Entries) > 0 {
		// fmt.Printf("%v adding %v to log %v\n", rf.me, args.Entries, rf.log)
		rf.log = append(rf.log, args.Entries...)
	}

	reply.Success = true

	// 5. If leaderCommit > commitIndex set commitINdex = min(leaderCommit, index of last new Entry)
	if args.LeaderCommit > rf.commitIndex {
		// fmt.Printf("%v commiting %v in log %v\n", rf.me, args.LeaderCommit, rf.log)
		rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(len(rf.log)-1)))
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

// wrapper function for sendRequestVote to handle race conditions and overriding votes
func (rf *Raft) processSendRequestVote(server int, args RequestVoteArgs, reply RequestVoteReply) {
	// send request to server
	ok := rf.sendRequestVote(server, &args, &reply)
	// fmt.Printf("Server: %v reply in rv from Server: %v, ok: %v, reply: %v\n", rf.me, server, ok, reply)

	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		// If the leader's term (included in its rpc) is at least as large as the candidate's current term
		// candidate recognizes leader as legitimate and returns to follower state
		if reply.Term > rf.currentTerm {
			rf.serverState = Follower
			rf.currentTerm = reply.Term
			rf.voteCount = 0
			rf.votedFor = -1
			return
		}

		// Handle Stale RPCS
		if args.Term != rf.currentTerm {
			return
		}

		// fmt.Printf("Server: %v VoteCount: %v, currentTerm: %v, serverState: %v\n", rf.me, rf.currentTerm, rf.voteCount, rf.serverState)
		if rf.serverState == Candidate {
			if reply.VoteGranted {
				rf.voteCount++
				// fmt.Printf("Server: %v vote count is %v", rf.me, rf.voteCount)
				if rf.voteCount >= len(rf.peers)/2+1 {
					rf.serverState = Leader
					rf.voteCount = 0
					for i := 0; i < len(rf.peers); i++ {
						rf.nextIndex[i] = len(rf.log)
						rf.matchIndex[i] = 0
					}
				}
			}
		}
	}
}

// AppendEntries RPC to a server
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
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// wrapper function for sendAppendEnries to handle race conditions and overriding append entries
func (rf *Raft) processSendAppendEntries(server int, args AppendEntriesArgs, reply AppendEntriesReply) {
	ok := rf.sendAppendEntries(server, &args, &reply)
	// fmt.Printf("Server: %v reply in AppendEntries from Server: %v, ok: %v\n", rf.me, server, ok)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if reply.Term > rf.currentTerm {
			rf.serverState = Follower
			rf.currentTerm = reply.Term
			rf.voteCount = 0
			return
		}

		// Handle Stale RPCS
		if args.Term != rf.currentTerm {
			return
		}

		if rf.serverState == Leader {
			if reply.Success {
				rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
				rf.nextIndex[server] = rf.matchIndex[server] + 1

				// Professor Stutsman trick: Sort match index and select median
				matchIndexCopy := make([]int, len(rf.matchIndex))
				copy(matchIndexCopy, rf.matchIndex) // make a copy of data to sort and find majority
				sort.Ints(matchIndexCopy) // sort and pick median for majority
				majorityIndex := matchIndexCopy[len(matchIndexCopy)/2]
				// fmt.Printf("Server: %v, matchIndex: %v, matchIndexCopy: %v, nextIndex: %v, log: %v, currentTerm: %v\n", rf.me, rf.matchIndex, matchIndexCopy, rf.nextIndex, rf.log, rf.currentTerm)
				if majorityIndex > rf.commitIndex && rf.log[majorityIndex].Term == rf.currentTerm {
					// fmt.Printf("Leader Server: %v, Time to commit Index: %v\n", rf.me, majorityIndex)
					// fmt.Printf("Server: %v, matchIndex: %v, matchIndexCopy: %v, nextIndex: %v, log: %v, currentTerm: %v, majorityIndex : %v\n", rf.me, rf.matchIndex, matchIndexCopy, rf.nextIndex, rf.log, rf.currentTerm, majorityIndex)
					rf.commitIndex = majorityIndex
				}
			} else {
				// implement Raft Student Guide Optimization
				index := 0
				for j := len(rf.log) - 1; j > 0; j-- {
					if rf.log[j].Term == reply.ConflictTerm {
						index = j
						break
					}
				}
				if index > 0 {
					rf.nextIndex[server] = index + 1
				} else {
					rf.nextIndex[server] = reply.ConflictIndex
				}
			}
		}
	}
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	// check if server is leader or if raft instance has been killed
	if rf.serverState != Leader || rf.killed() {
		isLeader = false
		return index, term, isLeader
	}

	// Append command to leader log as a new entry 
	rf.log = append(rf.log, LogEntry{rf.currentTerm, command})
	term = rf.currentTerm
	index = len(rf.log) - 1
	rf.matchIndex[rf.me] = len(rf.log) - 1
	// fmt.Printf("Server: %v Starting command is %v at Index %v\n", rf.me, command, index)
	// Send out AppendEntries RPCs after command from client
	rf.AppendEntriesRPC()

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
	electionTimeOut := time.Duration(rand.Intn(150)+150) * time.Millisecond
	for !rf.killed() {
		rf.mu.Lock()
		serverState := rf.serverState
		lastHeartBeat := rf.lastHeartBeat
		rf.mu.Unlock()

		if serverState != Leader {
			if lastHeartBeat.Add(electionTimeOut).Before(time.Now()) {
				rf.mu.Lock()
				// fmt.Printf("Server %v Starting New Election\n", rf.me)
				rf.currentTerm = rf.currentTerm + 1
				rf.serverState = Candidate
				rf.votedFor = rf.me
				rf.voteCount = 1
				rf.lastHeartBeat = time.Now()

				electionTimeOut = time.Duration(rand.Intn(150)+150) * time.Millisecond

				// Request Vote Args
				args := RequestVoteArgs{}
				args.CandidateID = rf.me
				args.Term = rf.currentTerm
				args.LastLogIndex = len(rf.log) - 1
				args.LastLogTerm = rf.log[args.LastLogIndex].Term
				// Request Vote Reply
				reply := RequestVoteReply{}

				// Issue Request Vote RPCs in parallel to each of the other servers in a cluster
				for i := 0; i < len(rf.peers); i++ {
					// send RequestVote RPCS to all servers except for self
					if i == rf.me {
						continue
					}
					// fmt.Printf("Server: %v, RequestVote From: %v, Log: %v\n", rf.me, i, rf.log)
					go rf.processSendRequestVote(i, args, reply)
				}
				rf.mu.Unlock()
			}
		}
		if serverState == Leader {
			rf.mu.Lock()
			rf.AppendEntriesRPC()
			rf.mu.Unlock()
			// time.Sleep(time.Duration(10) * time.Millisecond)
			time.Sleep(time.Duration(100) * time.Millisecond) // Change sleep time to 100 for RAFT optimization
		}
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
	}
}

// AppendEntries RPC logic
func (rf *Raft) AppendEntriesRPC() {
	// AppendEntries Args
	args := AppendEntriesArgs{}
	args.Term = rf.currentTerm
	args.LeaderID = rf.me
	args.LeaderCommit = rf.commitIndex
	// AppendEntries Reply
	reply := AppendEntriesReply{}
	for j := 0; j < len(rf.peers); j++ {
		// send AppendEntries RPCS to all servers except for self
		if j == rf.me {
			continue
		}
		args.PrevLogIndex = rf.nextIndex[j] - 1
		args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
		// fmt.Printf("Server: %v, PreviousLogIndex arg: %v, PrevLogTerm arg: %v\n", rf.me, args.PrevLogIndex, args.PrevLogTerm)
		entries := rf.log[rf.nextIndex[j]:]
		// fmt.Printf("Server: %v, Entries:%v, Next Log Entry Index: %v\n", rf.me, entries, rf.nextIndex[j])
		args.Entries = make([]LogEntry, len(entries))
		copy(args.Entries, entries) // make a copy of entries to avoid mutating data
		// fmt.Printf("Server: %v, Sending Heartbeat Args: %v, Log State: %v\n", rf.me, args, rf.log)
		go rf.processSendAppendEntries(j, args, reply)
	}
}

// The applyToStateMachine go routine starts a background routine to apply an entry to its local state
// machine (in log order) once it has been notified a log entry has been committed by the leader
func (rf *Raft) applyToStateMachine(applyCh chan ApplyMsg) {
	for !rf.killed() {
		applymsg := ApplyMsg{}
		rf.mu.Lock()
		// fmt.Printf("Server: %v, Time to apply Index: %v, Highest Log Entry Applied to State Machines: %v\n", rf.me, rf.commitIndex, rf.lastApplied)
		if rf.commitIndex > rf.lastApplied {
			rf.lastApplied = rf.lastApplied + 1
			applymsg.CommandValid = true
			applymsg.Command = rf.log[rf.lastApplied].Command
			applymsg.CommandIndex = rf.lastApplied
			// fmt.Printf("Applied Server: %v, Command: %v, CommandIndex: %v\n", rf.me, applymsg.Command, applymsg.CommandIndex)
			rf.mu.Unlock()
			applyCh <- applymsg
		} else {
			rf.mu.Unlock()
		}
		time.Sleep(time.Duration(10) * time.Millisecond)
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
	// Persistent State on All Servers
	rf.serverState = Follower
	rf.lastHeartBeat = time.Now()
	rf.voteCount = 0
	rf.currentTerm = 0
	rf.votedFor = -1

	// 2B
	rf.log = make([]LogEntry, 1)

	// Volatile State on All Servers
	rf.commitIndex = 0
	rf.lastApplied = 0

	// Volatile State on Leaders (reinitialized after election)
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	// start applyToStateMachine to apply and replicate commands to state machine
	// one a leader commits an entry
	go rf.applyToStateMachine(applyCh)

	return rf
}
