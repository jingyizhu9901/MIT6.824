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
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
)

const (
	FOLLOWER  = "Follower"
	CANDIDATE = "Candidate"
	LEADER    = "Leader"
	// election timeout will be selected randomly in the range of 250 to 400
	// milliseconds
	HEARTBEAT_TIMEOUT = 100
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// log entry
// each entry contains command for state machine,
// and term when entry was received by leader (first index is 1)
type logEntry struct {
	Command interface{}
	Term    int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent state on all servers
	currentTerm int
	votedFor    int
	log         []logEntry

	// volatile state on all servers
	state       string
	commitIndex int
	lastApplied int

	// volatile state on leaders
	nextIndex  []int // nextIndex[i] = the idx of next log entry to send to server i (init == leader's lastLogIndex+1)
	matchIndex []int // matchIndex[i] = the idx of the highest log entry known to be replicated on server (init = 0)

	// 2A
	lastHeardFromLeader time.Time
	electionTimeout     int
	voteCount           int

	// 2B
	applyCh chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).

	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state == LEADER
	rf.mu.Unlock()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.log)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	data := w.Bytes()
	DPrintf("** 2C: Persist() -> len(log) = %v; currentTerm = %v; votedFor = %v\n", len(rf.log), rf.currentTerm, rf.votedFor)
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	DPrintf("** 2C: readPersist() \n")
	if data == nil || len(data) < 1 { // bootstrap without any state?
		DPrintf("** 2C: readPersist() no data to read \n")
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var log []logEntry
	var currentTerm int
	var votedFor int
	if d.Decode(&log) != nil || d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil {
		//error...
		DPrintf("Error when reading persist\n")
	} else {
		rf.log = log
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		DPrintf("** 2C: ReadPersist() got data -> len(log) = %v; currentTerm = %v; votedFor = %v\n", len(rf.log), rf.currentTerm, rf.votedFor)
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	//DPrintf("Raft %v receives request vote from raft %v\n", rf.me, args.CandidateID)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	// candidate's term is smaller than my term
	if args.Term < rf.currentTerm || (args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateID) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// candidate's term larger than me, I must not be a candidate/leader
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.changeState(FOLLOWER)
	}

	// voter denied vote if its own log is more up-to-date
	voterLastEntryIndex := len(rf.log) // index start from 1
	voterLastEntryTerm := -1           // empty
	if len(rf.log) > 0 {
		voterLastEntryTerm = rf.log[voterLastEntryIndex-1].Term
	}
	if voterLastEntryTerm > args.LastLogTerm || (voterLastEntryTerm == args.LastLogTerm && voterLastEntryIndex > args.LastLogIndex) {
		reply.VoteGranted = false
		return
	}

	reply.VoteGranted = false
	reply.Term = rf.currentTerm

	if rf.votedFor == -1 || rf.votedFor == args.CandidateID {
		rf.votedFor = args.CandidateID
		reply.VoteGranted = true
		DPrintf("Raft %v vote for raft %v\n", rf.me, args.CandidateID)
	}
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !ok {
		return ok
	}
	if rf.state != CANDIDATE || args.Term != rf.currentTerm {
		return ok
	}
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.changeState(FOLLOWER)
		rf.persist()
		return ok
	}
	if reply.VoteGranted {
		rf.voteCount++
		if rf.voteCount > len(rf.peers)/2 {
			rf.changeState(LEADER)
		}
	}

	return ok
}

// AppendEntries RPC
type AppendEntriesArgs struct {
	Term         int // leader's term
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []logEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int  // currTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
	// faster roll-back
	XTerm  int
	Xindex int
	XLen   int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.Success = true
	reply.XTerm = -1
	reply.Xindex = -1
	reply.XLen = len(rf.log)

	// reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// leader's term larger than me, I must not be a candidate
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.changeState(FOLLOWER)
	}

	// reset timer
	rf.lastHeardFromLeader = time.Now()
	rf.electionTimeout = 0

	// LOG CONSISTENCY CHECK
	// reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
	lastLogIndex := len(rf.log)
	prevLogTerm := -1
	if len(rf.log) > 0 && args.PrevLogIndex-1 >= 0 && args.PrevLogIndex-1 < len(rf.log) {
		prevLogTerm = rf.log[args.PrevLogIndex-1].Term
	}
	if lastLogIndex < args.PrevLogIndex || prevLogTerm != args.PrevLogTerm { // my log is shorter than i should || my log is inconsistent
		DPrintf("Raft %v log is inconsistent with leader %v\n", rf.me, args.LeaderID)
		reply.Success = false
		if prevLogTerm != -1 {
			reply.XTerm = prevLogTerm
			reply.Xindex = args.PrevLogIndex - 1
			// find the index of the first entry with XTerm
			for reply.Xindex >= 0 && rf.log[reply.Xindex].Term >= reply.XTerm {
				reply.Xindex--
			}
			reply.Xindex++
			if rf.log[reply.Xindex].Term != reply.XTerm {
				fmt.Println("Error: cannot find Xindex")
				reply.Xindex = -1
			}
		}
		return
	}

	// find unmatched index
	unmatchedIdx := -1
	for idx := range args.Entries {
		if len(rf.log)-1 < (args.PrevLogIndex+idx) || // rf.log does not contain entries[idx]
			rf.log[args.PrevLogIndex+idx].Term != args.Entries[idx].Term { // term is different
			unmatchedIdx = idx
			break
		}
	}

	if unmatchedIdx != -1 {
		// delete conflicting entries
		rf.log = rf.log[:(args.PrevLogIndex + unmatchedIdx)]
		// append leader's log
		rf.log = append(rf.log, args.Entries[unmatchedIdx:]...)
	}

	// DPrintf("Raft %v log is length %v\n", rf.me, len(rf.log))

	if args.LeaderCommit > rf.commitIndex {
		rf.commit(args.LeaderCommit)
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	if !ok {
		return ok
	}

	rf.mu.Lock()
	if reply.Success { // success
		DPrintf("Raft %v replies success AppendEntries RPC\n", server)
		rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[server] = rf.matchIndex[server] + 1
		DPrintf("matchIndex is %v\n", rf.matchIndex)
		DPrintf("nextIndex is %v\n", rf.nextIndex)
		for N := len(rf.log); N > rf.commitIndex; N-- {
			peerCount := 0
			for _, idx := range rf.matchIndex {
				if idx >= N {
					peerCount++
				}
			}

			if peerCount > len(rf.peers)/2 {
				DPrintf("Master %v has log %v replicated on majority\n", rf.me, N)
				rf.commit(N)
				break
			}
		}
	} else {
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.persist()
			rf.changeState(FOLLOWER)
		} else { // fails because of log inconsistency
			rf.nextIndex[server] = reply.XLen + 1
			if reply.XTerm != -1 {
				rf.nextIndex[server] = reply.Xindex
			}
			if reply.Xindex != -1 {
				for i := args.PrevLogIndex; i >= 1; i-- {
					if rf.log[i-1].Term == reply.XTerm {
						rf.nextIndex[server] = i
						break
					}
				}
			}
		}
	}
	rf.mu.Unlock()

	return ok
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1

	rf.mu.Lock()
	isLeader := rf.state == LEADER
	rf.mu.Unlock()

	// Your code here (2B).
	// return false if not leader
	if !isLeader {
		return index, term, isLeader
	}

	// return gracefully if this Raft has been killed
	if rf.killed() {
		return index, term, false
	}

	// is leader, start agreement
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	rf.log = append(rf.log, logEntry{command, rf.currentTerm})
	index = len(rf.log)
	rf.matchIndex[rf.me] = index
	rf.nextIndex[rf.me] = index + 1
	term = rf.currentTerm
	DPrintf("=== 2B: Master %v received Log, rf.log has length %v\n", rf.me, len(rf.log))
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	DPrintf("Making raft%v...\n", rf.me)
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]logEntry, 0)
	rf.electionTimeout = rand.Intn(HEARTBEAT_TIMEOUT) + HEARTBEAT_TIMEOUT*3
	rf.state = FOLLOWER
	rf.lastHeardFromLeader = time.Now()

	// 2B
	rf.applyCh = applyCh
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	// initialize from state persisted before a crash
	rf.mu.Lock()
	rf.readPersist(persister.ReadRaftState())
	rf.mu.Unlock()

	go rf.kickOffElection()

	return rf
}

// a background goroutine that will kick off leader election periodically
// by sending out RequestVote RPCs when it hasn't heard from another peer for a
// while

func (rf *Raft) kickOffElection() {
	/// DPrintf("raft %v kicks Off Election\n", rf.me)
	for {
		time.Sleep(20 * time.Millisecond)

		rf.mu.Lock()
		state := rf.state
		if rf.electionTimeout == 0 {
			rf.electionTimeout = rand.Intn(HEARTBEAT_TIMEOUT) + HEARTBEAT_TIMEOUT*3
		}
		rf.mu.Unlock()
		timePassed := time.Since(rf.lastHeardFromLeader).Seconds() * 1000
		timeout := float64(rf.electionTimeout) < timePassed
		// if timeout {
		// 	DPrintf("Raft %v timeout true at term %v\n", rf.me, rf.currentTerm)
		// }

		switch state {
		case FOLLOWER:
			if timeout {
				rf.mu.Lock()
				rf.changeState(CANDIDATE)
				rf.mu.Unlock()
				rf.startElection()
			}
		case CANDIDATE:
			// start new election
			if timeout {
				rf.startElection()
			}
		case LEADER:
			rf.heartbeats()
			time.Sleep(HEARTBEAT_TIMEOUT * time.Millisecond)
		}
	}
}

func (rf *Raft) startElection() {
	DPrintf("raft %v of state %s startsElection at term %v\n", rf.me, rf.state, rf.currentTerm+1)
	rf.mu.Lock()
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()
	rf.voteCount = 1
	rf.lastHeardFromLeader = time.Now()

	lastLogIndex := len(rf.log)
	lastLogTerm := -1
	if lastLogIndex > 0 {
		lastLogTerm = rf.log[len(rf.log)-1].Term
	}
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.me,
		LastLogIndex: len(rf.log),
		LastLogTerm:  lastLogTerm,
	}
	rf.mu.Unlock()

	for peer := range rf.peers {
		if peer != rf.me && rf.state == CANDIDATE {
			go rf.sendRequestVote(peer, &args, &RequestVoteReply{})
		}
	}
}

// send heartbeat() to peers separately
func (rf *Raft) heartbeats() {
	for peer := range rf.peers {
		if peer != rf.me {
			go rf.heartbeat(peer)
		}
	}
}

func (rf *Raft) heartbeat(peer int) {
	// construct args
	rf.mu.Lock()
	prevLogIndex := rf.nextIndex[peer] - 1
	prevLogTerm := -1
	if len(rf.log) > 0 && prevLogIndex >= 1 {
		prevLogTerm = rf.log[prevLogIndex-1].Term // prevLog
	}
	var entries []logEntry
	if prevLogIndex >= 0 {
		entries = make([]logEntry, len(rf.log[prevLogIndex:]))
		copy(entries, rf.log[prevLogIndex:]) // send log entries starting at nextIndex
	}
	DPrintf("== 2B: Raft %v sending AppendEntries RPC to peer %v. prevLogIndex is %v, rf.log has length %v, Entry has length %v\n", rf.me, peer, prevLogIndex, len(rf.log), len(entries))

	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderID:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()

	// send RPC
	var reply AppendEntriesReply
	rf.sendAppendEntries(peer, &args, &reply)
}

func (rf *Raft) commit(commitIdx int) {
	rf.commitIndex = commitIdx
	if rf.commitIndex > rf.lastApplied {
		DPrintf("== COMMIT: Server %v Commit Log index %v\n", rf.me, commitIdx)
		entriesToApply := append([]logEntry{}, rf.log[(rf.lastApplied):(rf.commitIndex)]...)
		go func(startIdx int, entires []logEntry) {
			for idx, entry := range entires {
				msg := ApplyMsg{
					Command:      entry.Command,
					CommandIndex: startIdx + idx + 1,
					CommandValid: true,
				}
				rf.applyCh <- msg
				rf.mu.Lock()
				if rf.lastApplied < msg.CommandIndex {
					rf.lastApplied = msg.CommandIndex
				}
				rf.mu.Unlock()
			}
		}(rf.lastApplied, entriesToApply)
	}

}

func (rf *Raft) changeState(state string) {
	if rf.state == state {
		return
	}

	if state == FOLLOWER {
		rf.state = FOLLOWER
		DPrintf("Raft %v change to Follower in Term %v\n", rf.me, rf.currentTerm)
		rf.votedFor = -1
	}
	if state == CANDIDATE {
		rf.state = CANDIDATE
		DPrintf("Raft %v change to Candidate in Term %v\n", rf.me, rf.currentTerm)
	}
	if state == LEADER {
		rf.state = LEADER
		for peerIdx := range rf.peers {
			rf.nextIndex[peerIdx] = len(rf.log) + 1 // init to leader's last log index + 1, log index starts from 1
			rf.matchIndex[peerIdx] = 0              // init to 0
		}
		rf.heartbeats()
		DPrintf("Raft %v change to Leader in Term %v\n", rf.me, rf.currentTerm)
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
