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
	"log"
	"math/rand"
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

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
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
	log         [][]byte

	commitIndex int64 //highest log entry known to be committed
	lastApplied int64 // highest log entry applied to state machine

	// leader only
	nextIndex  int64 // index of the next log entry
	matchIndex int64 // highest log entry known to be relicated

	inputEntry    []chan *AppendEntriesArgs
	outputEntry   []chan *AppendEntriesArgs
	heartbeatTime time.Time
}

func (rf *Raft) Context() context.Context {
	ctx := context.Background()
	ctx = context.WithValue(ctx, "node", rf.me)
	ctx = context.WithValue(ctx, "term", rf.currentTerm)
	ctx = context.WithValue(ctx, "leader", rf.leaderId)
	return ctx
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
	Term         int   // candidate's term
	CandidateId  int   // candidate requesting vote
	LastLogIndex int64 // index of candidate's last log entry
	LastLogTerm  int64 // term of candidate's last log entry
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
	// Your code here (2A, 2B).
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		DPrintf(rf.Context(), "args.Term %v < currentTerm %v, return", args.Term, rf.currentTerm)
		return
	}
	/**
	if rf.state == Candidate {
		DPrintf(rf.Context(), "vote for myself, return")
		return
	}
	*/

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.votedFor == -1 {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		DPrintf(rf.Context(), "vote for server %v success", args.CandidateId)
	} else if args.Term > rf.currentTerm {
		rf.votedFor = args.CandidateId
		args.Term = rf.currentTerm
		DPrintf(rf.Context(), "vote for server %v success", args.CandidateId)
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
	DPrintf(rf.Context(), "RequestVote to server %d request %v reply %v", server, args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int    // leader's term
	LeaderId     int    // so follower can redirect clients
	PrevLogIndex int64  // index of log entry immediately preceding new ones
	PrevLogTerm  int64  // term of PrevLogIndex entry
	Entries      []byte // log entries to store
	LeaderCommit int64  // leader's commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching PrevLogIndex and PrevLogTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		DPrintf(rf.Context(), "args Term %d < currentTerm %d, return", args.Term, rf.currentTerm)
		return
	}

	reply.Success = true
	rf.heartbeatTime = time.Now()
	if args.Term > rf.currentTerm {
		rf.backToFollower(args.Term, args.LeaderId)
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if !reply.Success {
		DPrintf(rf.Context(), "send heartbeat to server %d args %v failed", server, args)
	} else {
		DPrintf(rf.Context(), "send heartbeat to server %d args %v success", server, args)
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
	log.Printf("start to make node %d", me)
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.leaderId = -1

	rf.inputEntry = make([]chan *AppendEntriesArgs, len(peers))
	rf.outputEntry = make([]chan *AppendEntriesArgs, len(peers))
	for i, _ := range peers {
		rf.inputEntry[i] = make(chan *AppendEntriesArgs, 10)
		rf.outputEntry[i] = make(chan *AppendEntriesArgs, 10)
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.leaderElection()
	go rf.heartbeatsGo()
	//	go rf.logReplication()

	return rf
}

const heartbeatInterval = time.Millisecond * 150

func (rf *Raft) leaderElection() {
	for {
		now := time.Now()
		// rand.Seed(now.UnixNano())
		electionTimeout := time.Millisecond * time.Duration(rand.Intn(200)+300)
		time.Sleep(electionTimeout)
		if rf.leaderId != rf.me && rf.heartbeatTime.Before(now) {
			rf.voteSelf()
		}
	}
}

func (rf *Raft) promptToLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = Leader
	rf.votedFor = rf.me
	rf.leaderId = rf.me
}

func (rf *Raft) backToFollower(term, leaderId int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if term > rf.currentTerm {
		rf.state = Follower
		rf.currentTerm = term
		rf.votedFor = -1
		rf.leaderId = leaderId
	}
}

func (rf *Raft) becomeCandidate() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = Candidate
	rf.currentTerm += 1
	rf.votedFor = -1
	rf.leaderId = -1
}

func (rf *Raft) logReplication() {
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			for {
				select {
				case output := <-rf.outputEntry[server]:
					var reply AppendEntriesReply
					if ok := rf.sendAppendEntries(server, output, &reply); ok {
						// marchIndex?
					}
					if output.Term > rf.currentTerm {
						rf.backToFollower(output.Term, -1)
					}
				case input := <-rf.inputEntry[server]:
					if input.Term < rf.currentTerm {
						break
					}
					rf.heartbeatTime = time.Now()
					if input.Term > rf.currentTerm {
						rf.backToFollower(input.Term, input.LeaderId)
					}
				}
			}
		}(i)
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

func (rf *Raft) heartbeatOnce(server int) {
	for {
		time.Sleep(heartbeatInterval)
		if _, isLeader := rf.GetState(); !isLeader {
			continue
		}
		heartbeatMsg := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: rf.commitIndex - 1,
			PrevLogTerm:  0, // TODO
		}
		var reply AppendEntriesReply
		if ok := rf.sendAppendEntries(server, heartbeatMsg, &reply); ok {
			// marchIndex?
		}
		if reply.Term > rf.currentTerm {
			rf.backToFollower(reply.Term, -1)
		}
	}
}

func (rf *Raft) voteSelf() {
	if rf.state == Leader || rf.votedFor != -1 {
		return
	}
	DPrintf(rf.Context(), "start to vote self")
	rf.becomeCandidate()
	replies := make([]*RequestVoteReply, len(rf.peers))
	var wg sync.WaitGroup
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		wg.Add(1)
		go func(server int) {
			defer wg.Done()
			var reply RequestVoteReply
			if ok := rf.sendRequestVote(server, &RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: 0,
				LastLogTerm:  0,
			}, &reply); !ok {
				DPrintf(rf.Context(), "send request vote to %v error", server)
			} else {
				// DPrintf(rf.Context(), "send request vote to %v success, reply %v", server, reply)
				replies[server] = &reply
			}
		}(i)
	}
	waitTimeout(&wg, time.Millisecond*300)
	voteCount := 1
	for i, reply := range replies {
		if i == rf.me {
			continue
		}
		if reply == nil {
			continue
		}
		if reply.Term > rf.currentTerm {
			rf.backToFollower(reply.Term, -1)
			return
		}
		if reply.VoteGranted {
			// DPrintf(rf.Context(), "node %d vote for me", i)
			voteCount += 1
		} else {
			// DPrintf(rf.Context(), "node %d not vote for me", i)
		}
	}
	majority := len(rf.peers)/2 + 1
	if voteCount >= majority {
		// set to leader
		// DPrintf(rf.Context(), "heartbeats success count %d", rf.heartbeats())
		rf.promptToLeader()
		DPrintf(rf.Context(), "vote count %d, set to leader", voteCount)
	} else {
		DPrintf(rf.Context(), "vote count %d, leader election failed, wait next round", voteCount)
	}
}
