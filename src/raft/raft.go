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
	"context"
	"fmt"
	"labgob"
	"labrpc"
	"math/rand"
	"os"
	"reflect"
	"runtime/pprof"
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
	CommandTerm  int
}

type SnapShotCommand struct {
	Notify bool // used to notify kvserver that SaveSnapshot had succeed, it can schedule another snapshot

	// If Notify is false, this is a InstallSnapshot request, kvserver should update it's state
	Snapshot []byte
	Index    int
}

type raftState int

const (
	Follower = iota
	Candidate
	Leader
	Killed
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

	// persisted state
	currentTerm int
	votedFor    int
	log         []*LogEntry

	// node status
	state    raftState
	leaderId int

	commitIndex int // highest log entry known to be committed
	lastApplied int // highest log entry applied to state machine
	applyCh     chan ApplyMsg

	// leader only
	nextIndex  []int // index of the next log entry
	matchIndex []int // highest log entry known to be relicated

	heartbeatTime time.Time   // controll leader election
	newEntriesCh  []chan bool // notify logReplication to run
	killedChan    chan bool   // server had been killed

	// snapshot
	snapshotIndex int                  // this hightest index that had been snapshoted, log[0].Index == snapshotIndex
	snapshotChan  chan SnapShotCommand // we should snapshot now
}

func (rf *Raft) lockContext() context.Context {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.context()
}

func (rf *Raft) context() context.Context {
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
	rf.persister.SaveRaftState(rf.getPersistData())
}

func (rf *Raft) getPersistData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.snapshotIndex)
	return w.Bytes()
}

// SaveSnapshot save snapshot data and raft sate into persister
// called by kvserver
func (rf *Raft) SaveSnapshot(snapshot []byte, index int) {
	rf.snapshotChan <- SnapShotCommand{
		Snapshot: snapshot,
		Index:    index,
	}
}

func (rf *Raft) saveSnapshot(snapshot []byte, index int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.commitIndex < index {
		DPrintf(rf.context(), "index %d not committed %d, return", index, rf.commitIndex)
		return false
	}

	ok := rf.installSnapshot(snapshot, index)
	msg := ApplyMsg{
		CommandValid: false,
		CommandIndex: 0, // not used
		CommandTerm:  rf.currentTerm,
		Command: SnapShotCommand{
			Notify: true,
		},
	}
	select {
	case <-rf.killedChan:
		return ok
	case rf.applyCh <- msg:
	case <-time.After(time.Millisecond * 50):
		DPrintf(rf.context(), "save snapshot not notify, ignore")
		//pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
		//panic(fmt.Sprintf("server %d blocked on install snapshot index %d for 5s", rf.me, index))
	}
	return ok

}

func (rf *Raft) installSnapshot(snapshot []byte, index int) bool {
	if index <= rf.snapshotIndex {
		DPrintf(rf.context(), "index %d already snapshot", index)
		return true
	}
	DPrintf(rf.context(), "start to snapshot, index %d, snapshotIndex %d, lastApplied %d, len(log) %d", index, rf.snapshotIndex, rf.lastApplied, len(rf.log))
	originIndex := rf.snapshotIndex
	logTruncate := index - originIndex
	if logTruncate > len(rf.log) {
		// should append a entry into rf.log for last index and term
		logTruncate = len(rf.log)
	}
	rf.snapshotIndex = index
	if rf.lastApplied < rf.snapshotIndex {
		rf.lastApplied = rf.snapshotIndex
	}
	if rf.commitIndex < rf.snapshotIndex {
		rf.commitIndex = rf.snapshotIndex
	}
	rf.log = rf.log[logTruncate:]
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		rf.nextIndex[i] = max(rf.nextIndex[i], rf.snapshotIndex+1)
		rf.matchIndex[i] = max(rf.matchIndex[i], rf.snapshotIndex)
	}

	state := rf.getPersistData()
	rf.persister.SaveStateAndSnapshot(state, snapshot)
	DPrintf(rf.context(), "Install snapshot success, index %d, snapshotIndex %d, lastApplied %d, len(log) %d", index, rf.snapshotIndex, rf.lastApplied, len(rf.log))
	return true
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var snapshotIndex int
	var logs []*LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil || len(logs) == 0 ||
		d.Decode(&snapshotIndex) != nil {
		rf.log = make([]*LogEntry, 0, 100)
		rf.log = append(rf.log, &LogEntry{}) // index start from 1
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = logs
		rf.snapshotIndex = snapshotIndex
		if rf.commitIndex < rf.snapshotIndex {
			rf.commitIndex = rf.snapshotIndex
		}
		DPrintf(rf.context(), "lastApplied %d, snapshotIndex %d, len(log) %d", rf.lastApplied, rf.snapshotIndex, len(rf.log))
	}
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

	// Your code here (2A, 2B).
	// 1: check term
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		DPrintf(rf.context(), "args.Term %v < currentTerm %v, return", args.Term, rf.currentTerm)
		return
	}
	if args.Term == rf.currentTerm && rf.leaderId != -1 {
		DPrintf(rf.context(), "args.Term %v already has leader %d, return", args.Term, rf.leaderId)
		return
	}

	if args.Term > rf.currentTerm {
		rf.heartbeatTime = time.Now()
		rf.currentTerm = args.Term
		rf.leaderId = -1
		rf.votedFor = -1
		rf.state = Follower
	}
	entry := rf.log[len(rf.log)-1]
	if rf.votedFor == -1 &&
		(entry.Term < args.LastLogTerm || (entry.Term == args.LastLogTerm && entry.Index <= args.LastLogIndex)) {
		rf.heartbeatTime = time.Now()
		DPrintf(rf.context(), "vote for server %v success, current term:index = %d:%d, candidate term:index = %d:%d", args.CandidateId, entry.Term, entry.Index, args.LastLogTerm, args.LastLogIndex)
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
	} else {
		DPrintf(rf.context(), "not vote for server %v, voteFor %v, entry term:index=%d:%d, args term:index=%d:%d", args.CandidateId, rf.votedFor, entry.Term, entry.Index, args.LastLogTerm, args.LastLogIndex)
	}
	rf.persist()
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
	/**
	if rf.leaderId != -1 && rf.leaderId != args.LeaderId {
		DPrintf(rf.context(), "args leaderId %d < currentLeaderId %d, return", args.LeaderId, rf.leaderId)
		return
	}
	*/

	if args.Term < rf.currentTerm {
		DPrintf(rf.context(), "1: args Term %d < currentTerm %d, return", args.Term, rf.currentTerm)
		return
	}
	index := args.PrevLogIndex - rf.snapshotIndex
	//DPrintf(rf.context(), "prevLogIndex %d, snapshotIndex %d, len(log) %d, args.Entry %d", args.PrevLogIndex, rf.snapshotIndex, len(rf.log), len(args.Entries))
	if index < 0 || index >= len(rf.log) {
		DPrintf(rf.context(), "2: log don't meet prev log condition, index %d not in log len(log) %d, from leader %d", index, len(rf.log), args.LeaderId)
		return
	}
	if rf.log[index].Term != args.PrevLogTerm {
		DPrintf(rf.context(), "2: log don't meet prev log condition, index %d log term %d, args term %d", index, rf.log[index].Term, args.PrevLogTerm)
		return
	}

	rf.heartbeatTime = time.Now()
	reply.Success = true
	rf.leaderId = args.LeaderId
	rf.state = Follower
	rf.votedFor = -1
	rf.currentTerm = args.Term
	// heartbeat
	if len(args.Entries) == 0 {
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommit, rf.log[len(rf.log)-1].Index)
			if !rf.applyLogs() {
				//DPrintf(rf.context(), "receive %d entries from leader %d, apply failed", len(args.Entries), rf.leaderId)
				reply.Success = false
				return
			}
		}
		return
	}

	// 3: delete conflict entry
	nlog := len(rf.log)
	var j int
	for j < len(args.Entries) && index+1+j < nlog {
		if !reflect.DeepEqual(args.Entries[j], rf.log[index+1+j]) {
			break
		}
		j++
	}
	if j == len(args.Entries) {
		// All new entries are equal, just return
		reply.Success = true
		return
	}
	if index+1+j < nlog {
		DPrintf(rf.context(), "truncate log len %d to %d, from leader %d, prevLogIndex %d, snapshotIndex %d, lastApplied %d", len(rf.log), index+1+j, args.LeaderId, args.PrevLogIndex, rf.snapshotIndex, rf.lastApplied)
		rf.log = rf.log[:index+1+j]
	}
	// double check the index
	if rf.log[len(rf.log)-1].Index+1 != args.Entries[j].Index {
		panic(fmt.Sprintf("%v, %v", rf.log[len(rf.log)-1], args.Entries[j]))
	}

	// 4: append new entry
	rf.log = append(rf.log, args.Entries[j:]...)

	// 5: apply log
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.log[len(rf.log)-1].Index)
		if !rf.applyLogs() {
			//DPrintf(rf.context(), "receive %d entries from leader %d, apply failed", len(args.Entries), rf.leaderId)
			reply.Success = false
			return
		}
	}

	rf.persist()
	//DPrintf(rf.context(), "receive %d entries from leader %d", len(args.Entries), rf.leaderId)
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
					DPrintf(rf.lockContext(), "heartbeat to server %d failed", server)
				} else {
					DPrintf(rf.lockContext(), "append entries to server %d failed", server)
				}
			} else {
				if len(args.Entries) == 0 {
					//DPrintf(rf.lockContext(), "send heartbeat to server %d success", server)
				} else {
					//DPrintf(rf.lockContext(), "append entries to server %d success", server)
				}
			}
		}
	case <-time.After(heartbeatInterval):
		DPrintf(rf.lockContext(), "contact server %d timeout", server)
	}
	return ok
}

type InstallSnapshotArgs struct {
	Term              int    // leader's term
	LeaderId          int    // leader id, so follower can redirect clients
	LastIncludedIndex int    // the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int    // term of LastIncludedIndex
	Offset            int    // byte offset where chunk is positioned in the snapshot file
	Data              []byte // raw bytes of the snapshot chunk, starting at offset
	Done              bool   // true if this is the last chunk. Always true in this impl
}

type InstallSnapshotReply struct {
	Term int // currentTerm, for leader to update itself
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
	}
	if !rf.installSnapshot(args.Data, args.LastIncludedIndex) {
		return
	}
	if len(rf.log) == 0 {
		rf.log = append(rf.log, &LogEntry{
			Index: args.LastIncludedIndex,
			Term:  args.LastIncludedTerm,
		})
	}
	msg := ApplyMsg{
		CommandValid: false,
		CommandIndex: 0, // not used
		CommandTerm:  rf.currentTerm,
		Command: SnapShotCommand{
			Snapshot: args.Data,
			Index:    args.LastIncludedIndex,
		},
	}
	select {
	case <-rf.killedChan:
		return
	case rf.applyCh <- msg:
		DPrintf(rf.context(), "apply install snapshot index %v success", args.LastIncludedIndex)
	case <-time.After(time.Second * 5):
		pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
		panic(fmt.Sprintf("server %d blocked on install snapshot index %d for 5s", rf.me, args.LastIncludedIndex))
	}
	DPrintf(rf.context(), "install snapshot success")
	return
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ch := make(chan bool, 1)
	go func() {
		defer close(ch)
		ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
		//DPrintf(rf.context(), "receive installSnapshot reply %v", reply.Term)
		ch <- ok
	}()
	var ok bool
	select {
	case res := <-ch:
		ok = res
	case <-time.After(heartbeatInterval):
		DPrintf(rf.lockContext(), "InstallSnapshot server %d timeout", server)
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
	defer rf.heartbeatsNow()
	defer rf.mu.Unlock()

	prevEntry := rf.log[len(rf.log)-1]
	index = len(rf.log) + rf.snapshotIndex
	if prevEntry.Index+1 != index {
		panic("index not +1")
	}
	le := &LogEntry{
		Command: command,
		Term:    rf.currentTerm,
		Index:   index,
	}
	rf.log = append(rf.log, le)
	DPrintf(rf.context(), "Start raft, index %d, snapshotIndex %d, lastApplied %d, len(log) %d", index, rf.snapshotIndex, rf.lastApplied, len(rf.log))
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here, if desired.
	rf.state = Killed
	close(rf.killedChan)
	DPrintf(rf.context(), "kill raft node")
	// close(rf.applyCh)
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
	rf.killedChan = make(chan bool)
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.leaderId = -1

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.lastApplied = rf.snapshotIndex
	rf.snapshotChan = make(chan SnapShotCommand)

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.newEntriesCh = make([]chan bool, len(peers))
	for i, _ := range peers {
		if i == rf.me {
			continue
		}
		rf.nextIndex[i] = len(rf.log) + rf.snapshotIndex
		rf.matchIndex[i] = 0
		rf.newEntriesCh[i] = make(chan bool, 100)
	}

	go rf.leaderElection()
	go rf.logReplication()

	DPrintf(rf.context(), "make raft node")
	return rf
}

func (rf *Raft) logReplication() {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			for {
				select {
				case cmd := <-rf.snapshotChan:
					rf.saveSnapshot(cmd.Snapshot, cmd.Index)
					continue
				case <-rf.newEntriesCh[server]:
				case <-time.After(heartbeatInterval):
				}

				rf.mu.RLock()
				drainCh := false
				for !drainCh {
					select {
					case <-rf.newEntriesCh[server]:
						//DPrintf(rf.context(), "drain one notify")
					default:
						drainCh = true
					}
				}
				if rf.state == Killed {
					DPrintf(rf.context(), "start to append entry, killed return")
					rf.mu.RUnlock()
					return
				}
				if rf.state != Leader {
					rf.mu.RUnlock()
					continue
				}
				startIndex := rf.nextIndex[server] - rf.snapshotIndex
				endIndex := len(rf.log)
				nextIndex := endIndex + rf.snapshotIndex
				if startIndex > endIndex {
					rf.mu.RUnlock()
					rf.mu.Lock()
					rf.nextIndex[server] = len(rf.log) + rf.snapshotIndex
					rf.mu.Unlock()
					continue
				}
				prevEntry := rf.log[startIndex-1]
				msg := &AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: prevEntry.Index,
					PrevLogTerm:  prevEntry.Term,
					LeaderCommit: rf.commitIndex,
				}
				if startIndex < endIndex {
					//DPrintf(rf.context(), "next %d, end %d, first %v", rf.nextIndex[server], end, rf.log[rf.nextIndex[server]])
					msg.Entries = rf.log[startIndex:endIndex] //append([]*LogEntry(nil), rf.log[startIndex:]...) //TODO:max entries?
				}
				rf.mu.RUnlock()

				var reply AppendEntriesReply
				//DPrintf(rf.context(), "append entries to %d, msg %v", server, msg)
				if ok := rf.sendAppendEntries(server, msg, &reply); ok {

					rf.mu.Lock()
					var shouldInstallSnapshot bool
					rf.heartbeatTime = time.Now()
					if reply.Term > rf.currentTerm {
						DPrintf(rf.context(), "append entry failed, back to follower")
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.leaderId = -1
						rf.state = Follower
					} else if reply.Success {
						// DPrintf(rf.context(), "append entry success, %d", server)
						// log might be snapshoted, so end is invalid now...
						// TODO: may have bad case
						rf.nextIndex[server] = max(nextIndex, rf.snapshotIndex+1)
						rf.matchIndex[server] = rf.nextIndex[server] - 1
						rf.leaderUpdateCommitIndex()
						rf.applyLogs()
					} else if rf.nextIndex[server]-rf.snapshotIndex > 2 {
						DPrintf(rf.context(), "append entry error, try decrement %v", rf.nextIndex[server])
						rf.nextIndex[server] -= 1
					} else if rf.nextIndex[server]-rf.snapshotIndex == 1 && rf.log[0].Index > 0 {
						shouldInstallSnapshot = true
					}
					rf.persist()
					rf.mu.Unlock()

					if shouldInstallSnapshot {
						rf.leaderSendSnapshot(server)
					}
				} else {
					DPrintf(rf.lockContext(), "append entries timeout %v", server)
				}
			}
		}(i)
	}
}

func (rf *Raft) leaderSendSnapshot(server int) {
	rf.mu.RLock()
	entry := rf.log[0]
	DPrintf(rf.context(), "append entry error, try install snapshot %v", entry.Index)
	snapshotArgs := &InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.leaderId,
		LastIncludedIndex: entry.Index,
		LastIncludedTerm:  entry.Term,
		Offset:            0,
		Data:              rf.persister.ReadSnapshot(),
		Done:              true,
	}
	var snapshotReply InstallSnapshotReply
	rf.mu.RUnlock()

	if ok := rf.sendInstallSnapshot(server, snapshotArgs, &snapshotReply); ok {

		rf.mu.Lock()
		if snapshotReply.Term > rf.currentTerm {
			rf.currentTerm = snapshotReply.Term
			rf.votedFor = -1
			rf.leaderId = -1
			rf.state = Follower
		}
		rf.persist()
		rf.mu.Unlock()

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

func (rf *Raft) applyLogs() bool {
	if rf.state == Killed {
		return false
	}
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied += 1
		applyIndex := rf.lastApplied - rf.snapshotIndex
		if applyIndex >= len(rf.log) || applyIndex <= 0 {
			errMsg := fmt.Sprintf("lastApplied %d, snapshotIndex %d, len(log) %d, commitIndex %d", rf.lastApplied, rf.snapshotIndex, len(rf.log), rf.commitIndex)
			DPrintf(rf.context(), errMsg)
			panic(errMsg)
		}
		entry := rf.log[applyIndex]
		msg := ApplyMsg{
			CommandValid: true,
			Command:      entry.Command,
			CommandIndex: entry.Index,
			CommandTerm:  entry.Term,
		}
		if rf.lastApplied != entry.Index {
			panic("lastApplied should == entry.Index")
		}
		select {
		case <-rf.killedChan:
			rf.lastApplied -= 1
			return false
		case rf.applyCh <- msg:
			DPrintf(rf.context(), "apply index %d success", entry.Index)
		case <-time.After(time.Millisecond * 500):
			rf.lastApplied -= 1
			errMsg := fmt.Sprintf("server %d blocked on index %d for 1s, will retry later", rf.me, entry.Index)
			DPrintf(rf.context(), errMsg)
			//pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
			// We should NOT panic here because applyCh may no hold lock
			//panic(errMsg)
			return false
		}
	}
	return true
}

func (rf *Raft) heartbeatsNow() {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		select {
		case rf.newEntriesCh[i] <- true:
			//DPrintf(rf.context(), "send to newEntriesCh %d success", i)
		case <-time.After(time.Millisecond * 5):
			DPrintf(rf.lockContext(), "send to newEntriesCh %d failed", i)
		}
	}
}

const heartbeatInterval = time.Millisecond * 150

func (rf *Raft) leaderElection() {
	electionBackoff := 300 // incase failed node always first propose election
	for {
		now := time.Now()
		// rand.Seed(now.UnixNano())
		electionTimeout := time.Millisecond * time.Duration(rand.Intn(200)+electionBackoff)
		time.Sleep(electionTimeout)

		rf.mu.Lock()
		if rf.state == Killed {
			rf.mu.Unlock()
			return
		}

		if rf.leaderId == rf.me || rf.heartbeatTime.After(now) {
			electionBackoff = 300
			rf.mu.Unlock()
			continue
		}
		rf.state = Candidate
		rf.currentTerm += 1
		rf.votedFor = rf.me
		rf.leaderId = -1
		majority := len(rf.peers)/2 + 1
		rf.mu.Unlock()

		count, term := rf.voteSelf()

		rf.mu.Lock()
		if term > rf.currentTerm {
			rf.currentTerm = term
			rf.votedFor = -1
			rf.state = Follower
		} else if count >= majority {
			DPrintf(rf.context(), "vote count %d, set to leader", count)
			rf.state = Leader
			rf.leaderId = rf.me
			rf.heartbeatsNow()
			electionBackoff = 300
		} else {
			DPrintf(rf.context(), "vote count %d, leader election failed, wait next round", count)
			if term > rf.currentTerm {
				rf.currentTerm = term
			}
			rf.votedFor = -1
			rf.state = Follower
			electionBackoff *= 2
		}
		rf.persist()
		rf.mu.Unlock()
	}
}

func (rf *Raft) voteSelf() (int, int) {
	DPrintf(rf.context(), "start to vote self")
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
	maxTerm := rf.currentTerm
	for i, reply := range replies {
		if i == rf.me {
			continue
		}
		if reply == nil {
			continue
		}
		if reply.Term > maxTerm {
			maxTerm = reply.Term
			continue
		}
		if reply.VoteGranted {
			voteCount += 1
		} else {
		}
	}
	if maxTerm > rf.currentTerm {
		return 0, maxTerm
	}
	return voteCount, 0
}
