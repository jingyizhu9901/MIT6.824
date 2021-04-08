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
	CommandTerm  int
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
	lastIncludedInd int
	lastIncludedTerm int

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
	data := rf.getPersistState()
	//DPrintf("** 2C: Persist() -> len(log) = %v; currentTerm = %v; votedFor = %v; lastIncludedInd = %v; lastIncludedTerm = %v\n", len(rf.log), rf.currentTerm, rf.votedFor,rf.lastIncludedInd, rf.lastIncludedTerm)
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) getPersistState() []byte{
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.log)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.lastApplied)
	e.Encode(rf.lastIncludedInd)
	e.Encode(rf.lastIncludedTerm)
	data := w.Bytes()
	return data
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
	var lastApplied int
	var lastIncludedInd int
	var lastIncludedTerm int
	if d.Decode(&log) != nil || d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&lastApplied) != nil || d.Decode(&lastIncludedInd) != nil || d.Decode(&lastIncludedTerm) != nil {
		//error...
		DPrintf("Error when reading persist\n")
	} else {
		rf.log = log
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.lastApplied = lastApplied
		rf.lastIncludedInd = lastIncludedInd
		rf.lastIncludedTerm = lastIncludedTerm
		DPrintf("** 2C: ReadPersist() got data -> len(log) = %v; currentTerm = %v; votedFor = %v; lastIncludedInd = %v; lastIncludedTerm = %v\n", len(rf.log), rf.currentTerm, rf.votedFor,rf.lastIncludedInd, rf.lastIncludedTerm)
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
	DPrintf("Raft %v receives request vote from raft %v\n", rf.me, args.CandidateID)
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
	voterLastEntryIndex := rf.getBeforeTruncateInd(len(rf.log)) // index start from 1
	voterLastEntryTerm := -1           // empty
	if len(rf.log) > 0 {
		voterLastEntryTerm = rf.log[len(rf.log)-1].Term
	}
	
	if voterLastEntryTerm > args.LastLogTerm || (voterLastEntryTerm == args.LastLogTerm && voterLastEntryIndex > args.LastLogIndex) {
		reply.VoteGranted = false
		return
	}

	reply.VoteGranted = false
	reply.Term = rf.currentTerm

	if rf.votedFor == -1 || rf.votedFor == args.CandidateID {
		rf.votedFor = args.CandidateID
		rf.state = FOLLOWER
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
	rf.state = FOLLOWER

	// reset timer
	rf.lastHeardFromLeader = time.Now()
	rf.electionTimeout = 0

	// LOG CONSISTENCY CHECK
	// reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
	lastLogIndex := rf.getBeforeTruncateInd(len(rf.log))
	prevLogTerm := -1
	actualPrevLogIndex := rf.getAfterTruncateInd(args.PrevLogIndex)
	if len(rf.log) > 0 {
		if actualPrevLogIndex-1 >= 0 && actualPrevLogIndex-1 < len(rf.log) {
			prevLogTerm = rf.log[actualPrevLogIndex-1].Term
		}
	}

	if actualPrevLogIndex == 0 && rf.lastIncludedInd > 0 {
		prevLogTerm = rf.lastIncludedTerm
	}

	// new entries is already in the snapshot
	if args.PrevLogIndex < rf.lastIncludedInd {
		reply.Success = true
		return
	}

	if lastLogIndex < args.PrevLogIndex || prevLogTerm != args.PrevLogTerm { // my log is shorter than i should || my log is inconsistent
		DPrintf("Raft %v log is inconsistent with leader %v, lastLogIndex = %v, args.PrevLogIndex = %v, prevLogTerm = %v, args.PrevLogTerm = %v\n", rf.me, args.LeaderID, lastLogIndex, args.PrevLogIndex, prevLogTerm, args.PrevLogTerm)
		reply.Success = false
		if prevLogTerm != -1 {
			reply.XTerm = prevLogTerm
			reply.Xindex = args.PrevLogIndex - 1
			rf.getAfterTruncateInd(reply.Xindex)
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

	// up-to args.PrevLogIndex is the same
	// find unmatched index
	unmatchedIdx := -1
	prevLogIndex := rf.getAfterTruncateInd(args.PrevLogIndex)
	for idx := range args.Entries {
		if len(rf.log)-1 < (prevLogIndex+idx) || // rf.log does not contain entries[idx]
			rf.log[prevLogIndex+idx].Term != args.Entries[idx].Term { // term is different
			unmatchedIdx = idx
			break
		}
	}

	if unmatchedIdx != -1 {
		// delete conflicting entries
		rf.log = rf.log[:(prevLogIndex + unmatchedIdx)]
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

	if rf.state != LEADER {
		rf.mu.Unlock()
		return false
	}

	if reply.Success { // success
		DPrintf("Raft %v replies success AppendEntries RPC\n", server)
		rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[server] = rf.matchIndex[server] + 1
		DPrintf("matchIndex is %v\n", rf.matchIndex)
		DPrintf("nextIndex is %v\n", rf.nextIndex)
		length := rf.getBeforeTruncateInd(len(rf.log))
		for N := length; N > rf.commitIndex; N-- {
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
			rf.nextIndex[server] = rf.getBeforeTruncateInd(reply.XLen + 1)
			if reply.XTerm != -1 {
				rf.nextIndex[server] = rf.getBeforeTruncateInd(reply.Xindex)
			}
			if reply.Xindex != -1 {
				index := rf.getAfterTruncateInd(args.PrevLogIndex)
				for i := index; i >= 1; i-- {
					if rf.log[i-1].Term == reply.XTerm {
						rf.nextIndex[server] = rf.getBeforeTruncateInd(i)
						break
					}
				}
			}
		}
	}
	rf.mu.Unlock()

	return ok
}

// InstallSnapshot RPC
type InstallSnapshotArgs struct {
	Term int
	LeaderId int
	LastIncludedIndex int
	LastIncludedTerm int
	Data []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	
	// reset timer
	rf.lastHeardFromLeader = time.Now()
	rf.electionTimeout = 0

	if args.LastIncludedIndex > rf.lastIncludedInd {
		DPrintf("Follower %d receive install snapshot request, rf.lastIncludedIndex: %d, args.LastIncludedIndex: %d",
			rf.me, rf.lastIncludedInd, args.LastIncludedIndex)
		truncationStartIndex := rf.getAfterTruncateInd(args.LastIncludedIndex)
		rf.lastIncludedInd = args.LastIncludedIndex
		rf.lastIncludedTerm = args.LastIncludedTerm
		rf.commitIndex = max(rf.commitIndex, rf.lastIncludedInd)

		if truncationStartIndex < len(rf.log) { // snapshot contain a prefix of its log
			if rf.log[truncationStartIndex].Term == args.LastIncludedTerm {
				rf.log = append(rf.log[truncationStartIndex:])
			}
		} else { // snapshot contain new information not already in the follower's log
			rf.log = make([]logEntry, 0) // discards entire log
		}
		rf.persister.SaveStateAndSnapshot(rf.getPersistState(), args.Data)
		rf.handInSnapshot(args.LastIncludedTerm)
		DPrintf("Follower %d install snapshot successfully, notify kv server", rf.me)
	}
}

func (rf *Raft) sendInstallSnapshot(server int) {
	rf.mu.Lock()

	if rf.state != LEADER {
		rf.mu.Unlock()
		return
	}

	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedInd,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              rf.persister.ReadSnapshot(),
	}
	rf.mu.Unlock()
	var reply InstallSnapshotReply
	ok := rf.peers[server].Call("Raft.InstallSnapshot", &args, &reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != LEADER {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.persist()
		rf.changeState(FOLLOWER)
	} else {
		previousNextIndex, previousMatchIndex := rf.nextIndex[server], rf.matchIndex[server]
		rf.nextIndex[server] = max(rf.nextIndex[server], rf.lastIncludedInd+1)
		rf.matchIndex[server] = max(rf.matchIndex[server], rf.lastIncludedInd)
		DPrintf("Raft server %d install snapshot for server %d successfully, update next index, previous: %d, now: %d, update match index, previous: %d, now: %d",
			rf.me, server, previousNextIndex, rf.nextIndex[server], previousMatchIndex, rf.matchIndex[server])
	}
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
	index = rf.getBeforeTruncateInd(len(rf.log))
	rf.matchIndex[rf.me] = index
	rf.nextIndex[rf.me] = index + 1
	term = rf.currentTerm
	DPrintf("=== 2B: Master %v received command %v, index is %v\n", rf.me, command, index)
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

	// 3B
	rf.lastIncludedInd = 0
	rf.lastIncludedTerm = 0

	// initialize from state persisted before a crash
	rf.mu.Lock()
	rf.readPersist(persister.ReadRaftState())
	// DPrintf("Server %v's log is:", me)
	// for i, log := range(rf.log) {
	// 	DPrintf("index %v => %v", i,log.Command)
	// }
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
		if rf.killed() {
			return
		}
		state := rf.state
		if rf.electionTimeout == 0 {
			rf.electionTimeout = rand.Intn(HEARTBEAT_TIMEOUT) + HEARTBEAT_TIMEOUT*3
		}
		timePassed := time.Since(rf.lastHeardFromLeader).Seconds() * 1000
		timeout := float64(rf.electionTimeout) < timePassed
		rf.mu.Unlock()

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

	lastLogIndex := rf.getBeforeTruncateInd(len(rf.log))
	lastLogTerm := -1
	if len(rf.log) > 0 {
		lastLogTerm = rf.log[len(rf.log)-1].Term
	}
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	rf.mu.Unlock()

	for peer := range rf.peers {
		rf.mu.Lock()
		if peer != rf.me && rf.state == CANDIDATE {
			rf.mu.Unlock()
			go rf.sendRequestVote(peer, &args, &RequestVoteReply{})
		} else {
			rf.mu.Unlock()
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

	if prevLogIndex < rf.lastIncludedInd {
		rf.mu.Unlock()
		go rf.sendInstallSnapshot(peer)
		return
	}

	prevLogTerm := -1
	actualPrevLogIndex := rf.getAfterTruncateInd(prevLogIndex)
	if len(rf.log) > 0 {
		if actualPrevLogIndex >= 1 {
			prevLogTerm = rf.log[actualPrevLogIndex-1].Term // prevLog
		} else if rf.lastIncludedInd != 0 {
			prevLogTerm = rf.lastIncludedTerm
		}
	}

	var entries []logEntry
	if actualPrevLogIndex >= 0 {
		entries = make([]logEntry, len(rf.log[actualPrevLogIndex:]))
		copy(entries, rf.log[actualPrevLogIndex:]) // send log entries starting at nextIndex
	}
	DPrintf("== 2B: Raft %v sending AppendEntries RPC to peer %v. prevLogIndex is %v, prevLogTerm is %v, actualPrevLogIndex is %v, rf.log has length %v, Entry has length %v\n", rf.me, peer, prevLogIndex, prevLogTerm, actualPrevLogIndex, len(rf.log), len(entries))

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
		actualCommitIndex := rf.getAfterTruncateInd(rf.commitIndex)
		actualLastApplied := rf.getAfterTruncateInd(rf.lastApplied)
		DPrintf("== COMMIT: Server %v Commit Log index %v ~ %v, actualIndex is %v ~ %v\n", rf.me, rf.lastApplied, commitIdx, actualLastApplied, actualCommitIndex)
		entriesToApply := append([]logEntry{}, rf.log[(actualLastApplied):(actualCommitIndex)]...)
		// if (rf.state == LEADER) {
			DPrintf("Server %v's log is:", rf.me)
			for i, log := range(rf.log) {
				DPrintf("index %v => %v", i,log.Command)
			}
		// }
		go func(startIdx int, entries []logEntry) {
			for idx, entry := range entries {
				msg := ApplyMsg{
					Command:      entry.Command,
					CommandIndex: startIdx + idx + 1,
					CommandValid: true,
					CommandTerm: entry.Term,
				}
				if cmd, ok := entry.Command.(string); ok && cmd == "" {
					msg.CommandValid = false
					DPrintf("Empty Command found")
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

// a follower receives an InstallSnapshot RPC, it must hand the included snapshot to its kvserver
func (rf *Raft) handInSnapshot(term int) {
	if rf.lastApplied < rf.lastIncludedInd {
		rf.lastApplied = rf.lastIncludedInd
		msg := ApplyMsg{
			Command:      "InstallSnapshot",
			CommandIndex: rf.lastIncludedInd,
			CommandValid: false,
			CommandTerm: term,
		}
		DPrintf("handInSnapshot to kvserver")

		go func(msg ApplyMsg) {
			rf.applyCh <- msg
		} (msg)
	}
}

func (rf *Raft) changeState(state string) {
	if rf.state == state {
		return
	}

	if state == FOLLOWER {
		if rf.state == LEADER {
			rf.notifyWrongLeader(rf.commitIndex + 1, rf.getBeforeTruncateInd(len(rf.log)))
		}
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
		rf.matchIndex[rf.me] = len(rf.log)
		rf.heartbeats()
		DPrintf("Raft %v change to Leader in Term %v\n", rf.me, rf.currentTerm)
	}
}

func (rf *Raft) notifyWrongLeader(start, end int) {
	DPrintf("!!! Raft %v is no longer the leader, send notify to applyCh on CommandIndex %v ~ %v", rf.me, start, end)
	for i := start; i <= end; i++ {
		rf.applyCh <- ApplyMsg{CommandValid: false, CommandIndex: i, CommandTerm: -1, Command: "ErrWrongLeader"}
	}
}

// 3B
// kvserver calls this function when the persisted Raft state grows too large
// kvserver hands a snapshot to Raft and tells Raft that it can discard old log entries.
// Raft saves each snapshot with persister.SaveStateAndSnapshot()
func (rf *Raft) PersistAndSaveStateAndSnapshot(snapshotInd int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if snapshotInd > rf.lastIncludedInd {
		rf.discardLogEntriesBeforeIndex(snapshotInd)
		state := rf.getPersistState()
		rf.persister.SaveStateAndSnapshot(state, snapshot)
	}
}

// Given a log index, discard the entries before that index.
func (rf *Raft) discardLogEntriesBeforeIndex(ind int) {
	DPrintf("discardLogEntriesBeforeIndex %v, log length is %v, lastIncludedIndex is %v", ind, len(rf.log), rf.lastIncludedInd)
	if ind > rf.lastIncludedInd {
		ind--
		newStartInd := rf.getAfterTruncateInd(ind)
		rf.log = rf.log[newStartInd:]
		rf.lastIncludedInd = ind
		rf.lastIncludedTerm = rf.log[0].Term
	}
}

// Given an index, return the correct log index offset by the length of truncated logs
// fakeIndex -> index for rf.log
// rf.lastIncludedInd >= 0
func (rf *Raft) getAfterTruncateInd(ind int) int {
	return ind - rf.lastIncludedInd
}

// index for rf.log -> fakeIndex
func (rf *Raft) getBeforeTruncateInd(ind int) int {
	return ind + rf.lastIncludedInd
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
