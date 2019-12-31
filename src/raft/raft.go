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
	"context"
	"labrpc"
	"math/rand"
	"sort"
	"sync"
	"time"
)

// import "bytes"
// import "labgob"

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

type raftState int

const (
	Follower = iota
	Candidate
	Leader
)

type LogEntry struct {
	Command interface{} `json:"command"`
	Term    int         `json:"term"`
	Index   int         `json:"index"`
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	state       raftState
	leaderId    int

	log         []*LogEntry
	logLength   int
	commitIndex int //highest log entry known to be committed
	lastApplied int // highest log entry applied to state machine
	applyCh     chan ApplyMsg

	// leader only
	nextIndex  []int // index of the next log entry
	matchIndex []int // highest log entry known to be relicated

	inputEntry    []chan *AppendEntriesArgs
	outputEntry   []chan *AppendEntriesArgs
	heartbeatTime time.Time
}

func (rf *Raft) LockContext() context.Context {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.Context()
}

func (rf *Raft) Context() context.Context {
	ctx := context.Background()
	ctx = context.WithValue(ctx, "node", rf.me)
	ctx = context.WithValue(ctx, "term", rf.currentTerm)
	ctx = context.WithValue(ctx, "leader", rf.leaderId)
	ctx = context.WithValue(ctx, "index", rf.commitIndex)
	return ctx
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.state == Leader
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
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		rf.log = make([]*LogEntry, 0, 100)
		rf.log = append(rf.log, &LogEntry{}) // index start from 1
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

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // current term, for candidate to update itself
	VoteGranted bool // true means candidate receivted vote
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.heartbeatTime = time.Now()
	// Your code here (2A, 2B).
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		DPrintf(rf.Context(), "args.Term %v < currentTerm %v, return", args.Term, rf.currentTerm)
		return
	}
	if args.Term == rf.currentTerm && rf.leaderId != -1 {
		DPrintf(rf.Context(), "args.Term %v already has leader %d, return", args.Term, rf.leaderId)
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.leaderId = -1
		rf.votedFor = -1
		rf.state = Follower
	}
	entry := rf.log[len(rf.log)-1]
	if rf.votedFor == -1 &&
		(entry.Term < args.LastLogTerm || (entry.Term == args.LastLogTerm && entry.Index <= args.LastLogIndex)) {
		DPrintf(rf.Context(), "vote for server %v success, current term:index = %d:%d, candidate term:index = %d:%d", args.CandidateId, entry.Term, entry.Index, args.LastLogTerm, args.LastLogIndex)
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
	} else {
		DPrintf(rf.Context(), "not vote for server %v, voteFor %v", args.CandidateId, rf.votedFor)
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
	return ok
}

type AppendEntriesArgs struct {
	Term         int         // leader's term
	LeaderId     int         // so follower can redirect clients
	PrevLogIndex int         // index of log entry immediately preceding new ones
	PrevLogTerm  int         // term of PrevLogIndex entry
	Entries      []*LogEntry // log entries to store
	LeaderCommit int         // leader's commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching PrevLogIndex and PrevLogTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if rf.leaderId != -1 && rf.leaderId != args.LeaderId {
		DPrintf(rf.Context(), "args leaderId %d < currentLeaderId %d, return", args.LeaderId, rf.leaderId)
		return
	}

	if args.Term < rf.currentTerm {
		DPrintf(rf.Context(), "1: args Term %d < currentTerm %d, return", args.Term, rf.currentTerm)
		return
	}
	if len(rf.log) <= args.PrevLogIndex ||
		rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		DPrintf(rf.Context(), "2: log don't meet prev log condition")
		return
	}

	rf.heartbeatTime = time.Now()
	reply.Success = true
	// heartbeat
	if len(args.Entries) == 0 {
		rf.state = Follower
		rf.votedFor = -1
		rf.currentTerm = args.Term
		rf.leaderId = args.LeaderId
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommit, rf.log[len(rf.log)-1].Index)
			rf.applyLogs()
		}
		return
	}

	if len(rf.log) > args.PrevLogIndex+1 {
		// truncate
		rf.log = rf.log[:args.PrevLogIndex+1]
	}
	rf.log = append(rf.log, args.Entries...)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.log[len(rf.log)-1].Index)
		rf.applyLogs()
	}

	DPrintf(rf.Context(), "receive %d entries from leader %d", len(args.Entries), rf.leaderId)
	return
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ch := make(chan bool, 1)
	go func() {
		defer close(ch)
		ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
		ch <- ok
	}()
	var ok bool
	select {
	case res := <-ch:
		ok = res
		if ok {
			if !reply.Success {
				if len(args.Entries) == 0 {
					DPrintf(rf.LockContext(), "heartbeat to server %d failed", server)
				} else {
					DPrintf(rf.LockContext(), "append entries to server %d failed", server)
				}
			} else {
				if len(args.Entries) == 0 {
					DPrintf(rf.LockContext(), "send heartbeat to server %d success", server)
				} else {
					//DPrintf(rf.LockContext(), "append entries to server %d success", server)
				}
			}
		}
	case <-time.After(heartbeatInterval):
		DPrintf(rf.LockContext(), "contact server %d timeout", server)
	}
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
	isLeader := true

	// Your code here (2B).
	term, isLeader = rf.GetState()
	if !isLeader {
		return index, term, isLeader
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf(rf.Context(), "start raft %v", command)

	index = len(rf.log)
	le := &LogEntry{
		Command: command,
		Term:    rf.currentTerm,
		Index:   index,
	}
	rf.log = append(rf.log, le)

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
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
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.leaderId = -1

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.inputEntry = make([]chan *AppendEntriesArgs, len(peers))
	rf.outputEntry = make([]chan *AppendEntriesArgs, len(peers))
	for i, _ := range peers {
		if i == rf.me {
			continue
		}
		rf.inputEntry[i] = make(chan *AppendEntriesArgs, 10)
		rf.outputEntry[i] = make(chan *AppendEntriesArgs, 10)
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
	}

	go rf.leaderElection()
	go rf.heartbeatsGo()
	go rf.logReplication()

	return rf
}

func (rf *Raft) logReplication() {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			if server == rf.me {
				return
			}
			for {
				time.Sleep(time.Millisecond * 50)

				rf.mu.RLock()
				if rf.state != Leader || rf.nextIndex[server] >= len(rf.log) {
					rf.mu.RUnlock()
					continue
				}
				end := len(rf.log)
				msg := &AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: rf.nextIndex[server] - 1,
					PrevLogTerm:  rf.log[rf.nextIndex[server]-1].Term,
					LeaderCommit: rf.commitIndex,
					Entries:      append([]*LogEntry(nil), rf.log[rf.nextIndex[server]:end]...), //TODO:max entries?
				}
				rf.mu.RUnlock()

				var reply AppendEntriesReply
				if ok := rf.sendAppendEntries(server, msg, &reply); ok {

					rf.mu.Lock()
					rf.heartbeatTime = time.Now()
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.leaderId = -1
						rf.state = Follower
					} else if reply.Success {
						rf.nextIndex[server] = end
						rf.matchIndex[server] = end - 1
						rf.leaderUpdateCommitIndex()
						rf.applyLogs()
					} else if rf.nextIndex[server] > 2 {
						rf.nextIndex[server] -= 1
					}
					rf.mu.Unlock()

				}
			}
		}(i)
	}
}

func (rf *Raft) quorum() int {
	return len(rf.peers)/2 + 1
}

func (rf *Raft) leaderUpdateCommitIndex() {
	t := append([]int(nil), rf.matchIndex...)
	sort.Sort(sort.Reverse(sort.IntSlice(t)))
	min := t[rf.quorum()-2]
	if min > rf.commitIndex {
		rf.commitIndex = min
	}
}

func (rf *Raft) applyLogs() {
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied += 1
		entry := rf.log[rf.lastApplied]
		rf.applyCh <- ApplyMsg{
			CommandValid: entry.Command != nil,
			Command:      entry.Command,
			CommandIndex: entry.Index,
		}
		DPrintf(rf.Context(), "apply index %d, command %v", entry.Index, entry.Command)
	}
}

func (rf *Raft) heartbeatsGo() {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.heartbeatOnce(i)
	}
}

const heartbeatInterval = time.Millisecond * 150

func (rf *Raft) heartbeatOnce(server int) {
	for {
		time.Sleep(heartbeatInterval)
		if _, isLeader := rf.GetState(); !isLeader {
			continue
		}

		rf.mu.RLock()
		lastEntry := rf.log[len(rf.log)-1]
		heartbeatMsg := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: lastEntry.Index,
			PrevLogTerm:  lastEntry.Term,
			LeaderCommit: rf.commitIndex,
		}
		rf.mu.RUnlock()

		var reply AppendEntriesReply
		if ok := rf.sendAppendEntries(server, heartbeatMsg, &reply); ok {

			rf.mu.Lock()
			rf.heartbeatTime = time.Now()
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.leaderId = -1
				rf.state = Follower
			}
			rf.mu.Unlock()

		}
	}
}

func (rf *Raft) leaderElection() {
	for {
		now := time.Now()
		// rand.Seed(now.UnixNano())
		electionTimeout := time.Millisecond * time.Duration(rand.Intn(200)+300)
		time.Sleep(electionTimeout)

		rf.mu.Lock()
		if rf.leaderId != rf.me && rf.heartbeatTime.Before(now) {
			rf.state = Candidate
			rf.currentTerm += 1
			rf.votedFor = rf.me
			rf.leaderId = -1
			majority := len(rf.peers)/2 + 1
			if count, term := rf.voteSelf(); count >= majority {
				DPrintf(rf.Context(), "vote count %d, set to leader", count)
				rf.state = Leader
				rf.leaderId = rf.me
			} else {
				DPrintf(rf.Context(), "vote count %d, leader election failed, wait next round", count)
				if term > rf.currentTerm {
					rf.currentTerm = term
				}
				rf.votedFor = -1
				rf.state = Follower
			}
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) voteSelf() (int, int) {
	DPrintf(rf.Context(), "start to vote self")
	replies := make([]*RequestVoteReply, len(rf.peers))
	var m sync.Mutex
	var wg sync.WaitGroup
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		wg.Add(1)
		go func(server int) {
			defer wg.Done()
			var reply RequestVoteReply
			entry := rf.log[len(rf.log)-1]
			if ok := rf.sendRequestVote(server, &RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: entry.Index,
				LastLogTerm:  entry.Term,
			}, &reply); ok {

				m.Lock()
				replies[server] = &reply
				m.Unlock()

			}
		}(i)
	}
	waitTimeout(&wg, time.Millisecond*300)
	voteCount := 1

	m.Lock()
	defer m.Unlock()
	for i, reply := range replies {
		if i == rf.me {
			continue
		}
		if reply == nil {
			continue
		}
		if reply.Term > rf.currentTerm {
			return 0, reply.Term
		}
		if reply.VoteGranted {
			voteCount += 1
		} else {
		}
	}
	return voteCount, 0
}
