package raftkv

import (
	"bytes"
	"context"
	"fmt"
	"labgob"
	"labrpc"
	"log"
	"os"
	"raft"
	"sync"
	"time"
)

var Debug = "0"

func init() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	if debug := os.Getenv("kvdebug"); len(debug) > 0 {
		Debug = debug
	}
}

func DPrintf(ctx context.Context, format string, a ...interface{}) (n int, err error) {
	if Debug != "0" {
		log.Printf("%s, node: %d, log: %s", ctx.Value("type"), ctx.Value("node"), fmt.Sprintf(format, a...))
	}
	return
}

type OpCommand int

const (
	OpCommandNone = iota
	OpCommandGet
	OpCommandPut
	OpCommandAppend
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Command   OpCommand
	Key       string
	Value     string
	ClientId  int64
	RequestId int64
}

type KVServer struct {
	mu      sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	state map[string]string
	acks  map[int64]int64
	index int

	resultCh map[int]chan result

	serverKilled chan bool

	// snapshot
	persister   *raft.Persister
	snapshoting bool
}

type result struct {
	Term  int
	Value string
}

func (kv *KVServer) context() context.Context {
	ctx := context.Background()
	ctx = context.WithValue(ctx, "node", kv.me)
	ctx = context.WithValue(ctx, "type", "server")
	return ctx
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	reply.Err, reply.Value = kv.raft(Op{
		Command: OpCommandGet,
		Key:     args.Key,
	})
	if reply.Err == ErrWrongLeader {
		reply.WrongLeader = true
	}
	return
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	command := OpCommandAppend
	if args.Op == "Put" {
		command = OpCommandPut
	}
	reply.Err, _ = kv.raft(Op{
		Command:   OpCommand(command),
		Key:       args.Key,
		Value:     args.Value,
		RequestId: args.RequestId,
		ClientId:  args.ClientId,
	})
	if reply.Err == ErrWrongLeader {
		reply.WrongLeader = true
	}
	return
}

func (kv *KVServer) raft(command interface{}) (Err, string) {
	index, term, ok := kv.rf.Start(command)
	if !ok {
		return ErrWrongLeader, ""
	}

	kv.mu.Lock()
	ch := make(chan result, 1) // make the send never block
	// the chan for this index might be overwrite by other requests, so this chan may never receive any message
	// we close this chan for `id := <=ch` to proceed
	kv.resultCh[index] = ch
	kv.mu.Unlock()

	select {
	case <-kv.serverKilled:
		return ErrWrongLeader, ""
	case r := <-ch:
		if r.Term != term {
			return ErrWrongLeader, ""
		}
		return OK, r.Value
	case <-time.After(time.Second):
		kv.mu.Lock()
		delete(kv.resultCh, index)
		kv.mu.Unlock()
		return ErrTimeout, ""
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	// Your code here, if desired.

	kv.mu.Lock()
	close(kv.serverKilled)
	for key := range kv.resultCh {
		delete(kv.resultCh, key)
	}
	kv.rf.Kill()
	DPrintf(kv.context(), "kill server")
	kv.mu.Unlock()
}

//
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
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.persister = persister

	// You may need initialization code here.
	kv.state = make(map[string]string)
	kv.acks = make(map[int64]int64)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.resultCh = make(map[int]chan result)
	kv.serverKilled = make(chan bool)

	kv.readPersist()
	go kv.readApplyCh()
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	DPrintf(kv.context(), "start kvserver")
	return kv
}

func (kv *KVServer) snapshot() {
	if !kv.snapshoting && kv.maxraftstate > 0 &&
		kv.persister.RaftStateSize() > kv.maxraftstate {
		kv.snapshoting = true
		go kv.rf.SaveSnapshot(kv.getPersistData(), kv.index)
	}
}

func (kv *KVServer) getPersistData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.state)
	e.Encode(kv.acks)
	e.Encode(kv.index)
	return w.Bytes()
}

func (kv *KVServer) readPersist() {
	data := kv.persister.ReadSnapshot()
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var state map[string]string
	var acks map[int64]int64
	var index int
	if d.Decode(&state) != nil ||
		d.Decode(&acks) != nil ||
		d.Decode(&index) != nil {
		panic("decode snapshot error")
	} else {
		kv.state = state
		kv.acks = acks
		kv.index = index
		DPrintf(kv.context(), "read persist succeed, snapshot index %d", kv.index)
	}
}

func (kv *KVServer) readApplyCh() {
	for {
		select {
		case <-kv.serverKilled:
			DPrintf(kv.context(), "server killed")
			return
		case msg := <-kv.applyCh:
			kv.mu.Lock()
			if !msg.CommandValid {
				kv.handleSnapshotCmd(msg)
				kv.mu.Unlock()
				break
			}

			kv.handleKVCommand(msg)
			kv.snapshot()
			kv.mu.Unlock()
		}
	}
}

func (kv *KVServer) handleSnapshotCmd(msg raft.ApplyMsg) {
	switch v := msg.Command.(type) {
	case raft.SnapShotCommand:
		if v.Notify {
			// make sure we can snapshot again
			kv.snapshoting = false
		} else if v.Index > kv.index {
			kv.readPersist()
		}
	default:
		panic(fmt.Sprintf("unknow command %v", v))
	}
}

func (kv *KVServer) handleKVCommand(msg raft.ApplyMsg) {
	// validate msg
	if msg.CommandIndex <= kv.index {
		// ignore old messages
		return
	}
	if msg.CommandIndex != kv.index+1 {
		errMsg := fmt.Sprintf("server %d index %d get command index %d", kv.me, kv.index, msg.CommandIndex)
		DPrintf(kv.context(), errMsg)
		panic(errMsg)
	}
	kv.index = msg.CommandIndex

	// handle cmd
	op, _ := msg.Command.(Op)
	if op.Command != OpCommandGet {
		if kv.acks[op.ClientId] != op.RequestId {
			DPrintf(kv.context(), "get apply msg %v", msg.CommandIndex)
			// filter old message
			switch op.Command {
			case OpCommandAppend:
				kv.state[op.Key] += op.Value
			case OpCommandPut:
				kv.state[op.Key] = op.Value
			default:
				// do nothing
			}
		}
		kv.acks[op.ClientId] = op.RequestId
	}
	// notify new result
	if ch, ok := kv.resultCh[kv.index]; ok {
		delete(kv.resultCh, kv.index)
		select {
		case <-kv.serverKilled:
			return
		case ch <- result{Term: msg.CommandTerm, Value: kv.state[op.Key]}:
			DPrintf(kv.context(), "notify result ch index %v success", kv.index)
		case <-time.After(time.Millisecond * 30):
			DPrintf(kv.context(), "abandon result ch index %d", kv.index)
		}
	}
}
