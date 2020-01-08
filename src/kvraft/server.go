package raftkv

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"labgob"
	"labrpc"
	"log"
	"os"
	"raft"
	"sync"
	"time"
)

// NOTE: don't call rf.Func inside lock!!

var Debug = "1"

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
	Command OpCommand
	Key     string
	Value   string
	UUID    int64
}

type KVServer struct {
	mu      sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	state map[string]string
	acks  map[int64]bool
	index int
	term  int

	resultCh map[int]chan int

	serverKilled chan bool

	// snapshot
	persister     *raft.Persister
	snapshotCh    chan bool
	snapshoting   bool
	snapshotIndex int
}

var TimeoutErr = errors.New("kv: timeout error")

func (kv *KVServer) context() context.Context {
	ctx := context.Background()
	ctx = context.WithValue(ctx, "node", kv.me)
	ctx = context.WithValue(ctx, "type", "server")
	return ctx
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	index, term, ok := kv.rf.Start(Op{
		Command: OpCommandGet,
		Key:     args.Key,
		UUID:    args.UUID,
	})
	if !ok {
		reply.WrongLeader = true
		return
	}

	if kv.waitResultTimeout(index, term) {
		DPrintf(kv.context(), "get index %d timeout return", index)
		reply.Err = ErrTimeout
		return
	} else {
		kv.mu.RLock()
		defer kv.mu.RUnlock()
		if v, ok := kv.state[args.Key]; ok {
			reply.Err = OK
			reply.Value = v
		} else {
			reply.Err = ErrNoKey
		}
		/**
		if value.OK {
			reply.Err = OK
			reply.Value = value.Value
		} else {
			reply.Err = ErrNoKey
		}
		*/
		return
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	command := OpCommandAppend
	if args.Op == "Put" {
		command = OpCommandPut
	}
	index, term, ok := kv.rf.Start(Op{
		Command: OpCommand(command),
		Key:     args.Key,
		Value:   args.Value,
		UUID:    args.UUID,
	})
	if !ok {
		reply.WrongLeader = true
		return
	}

	if kv.waitResultTimeout(index, term) {
		DPrintf(kv.context(), "%s index %d timeout err", args.Op, index)
		reply.Err = ErrTimeout
		return
	}
	reply.Err = OK
	return
}

func (kv *KVServer) waitResultTimeout(index, term int) bool {
	kv.mu.Lock()
	ch := make(chan int)
	// the chan for this index might be overwrite by other requests, so this chan may never receive any message
	// we close this chan for `id := <=ch` to proceed
	if ch, ok := kv.resultCh[index]; ok {
		close(ch)
		delete(kv.resultCh, index)
	}
	kv.resultCh[index] = ch
	kv.mu.Unlock()

	select {
	case r := <-ch:
		return r != term
	case <-kv.serverKilled:
		return true
	case <-time.After(time.Second):
		kv.mu.Lock()
		if ch, ok := kv.resultCh[index]; ok {
			close(ch)
			delete(kv.resultCh, index)
		}
		kv.mu.Unlock()
		return true
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
	for _, ch := range kv.resultCh {
		close(ch)
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
	kv.acks = make(map[int64]bool)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.resultCh = make(map[int]chan int)
	kv.serverKilled = make(chan bool)
	kv.snapshotCh = make(chan bool)

	kv.readPersist()

	go kv.readApplyCh()

	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	// You may need initialization code here.

	DPrintf(kv.context(), "start kvserver")
	return kv
}

func (kv *KVServer) shouldSnapshot() bool {
	return kv.maxraftstate > 0 &&
		kv.index > kv.snapshotIndex &&
		kv.persister.RaftStateSize() > kv.maxraftstate
}

func (kv *KVServer) snapshot(force bool) {
	if force || !kv.snapshoting {
		if kv.shouldSnapshot() {
			kv.snapshoting = true
			kv.snapshotIndex = kv.index
			go kv.rf.SaveSnapshot(kv.getPersistData(), kv.index)
		}
	}
}

func (kv *KVServer) getPersistData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.state)
	e.Encode(kv.index)
	e.Encode(kv.term)
	e.Encode(kv.acks)
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
	var snapshotIndex int
	var term int
	var acks map[int64]bool
	if d.Decode(&state) != nil ||
		d.Decode(&snapshotIndex) != nil ||
		d.Decode(&term) != nil ||
		d.Decode(&acks) != nil {
		panic("decode snapshot error")
	} else if kv.index < snapshotIndex {
		kv.state = state
		kv.index = snapshotIndex
		kv.term = term
		kv.acks = acks
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
				switch v := msg.Command.(type) {
				case raft.SnapShotCommand:
					if v.Notify {
						// make sure we can snapshot again
						kv.snapshoting = false
					} else if v.Index > kv.index {
						kv.readPersist()
					}
					break
				default:
					panic(fmt.Sprintf("unknow command %v", v))
				}
				kv.mu.Unlock()
				break
			}

			if msg.CommandIndex <= kv.index {
				// ignore old messages
				kv.mu.Unlock()
				break
			}
			if msg.CommandIndex != kv.index+1 {
				errMsg := fmt.Sprintf("server %d index %d get command index %d", kv.me, kv.index, msg.CommandIndex)
				DPrintf(kv.context(), errMsg)
				panic(errMsg)
			}
			kv.index = msg.CommandIndex

			op, _ := msg.Command.(Op)
			if op.Command != OpCommandGet {
				if !kv.acks[op.UUID] {
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
				kv.acks[op.UUID] = true
			}
			if ch, ok := kv.resultCh[kv.index]; ok {
				select {
				case ch <- msg.CommandTerm:
					DPrintf(kv.context(), "notify result ch index %v success", kv.index)
				case <-kv.serverKilled:
					kv.mu.Unlock()
					return
					/**
					case <-time.After(time.Millisecond * 30):
						DPrintf(kv.context(), "abandon result ch index %d", kv.index)
					*/
				}
				close(ch)
				delete(kv.resultCh, kv.index)
			}
			kv.snapshot(false)
			kv.mu.Unlock()
		case <-time.After(snapshotInterval):
			kv.mu.Lock()
			kv.snapshot(true)
			kv.mu.Unlock()
		}
	}
}

const snapshotInterval = time.Millisecond * 100
