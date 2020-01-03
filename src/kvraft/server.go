package raftkv

import (
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
	UUID    string
}

type KVServer struct {
	mu      sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	state map[string]string
	acks  map[string]bool
	index int

	resultCh map[int]chan string

	serverKilled chan bool
}

var TimeoutErr = errors.New("kv: timeout error")

func (kv *KVServer) context() context.Context {
	ctx := context.Background()
	ctx = context.WithValue(ctx, "node", kv.me)
	ctx = context.WithValue(ctx, "type", "server")
	return ctx
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.RLock()
	if kv.acks[args.UUID] {
		defer kv.mu.RUnlock()
		if v, ok := kv.state[args.Key]; ok {
			reply.Err = OK
			reply.Value = v
			return
		} else {
			reply.Err = ErrNoKey
			return
		}
	}
	kv.mu.RUnlock()

	index, _, ok := kv.rf.Start(Op{
		Command: OpCommandGet,
		Key:     args.Key,
		UUID:    args.UUID,
	})
	if !ok {
		reply.WrongLeader = true
		return
	}

	if kv.waitResultTimeout(index, args.UUID) {
		DPrintf(kv.context(), "get wait index %d timeout, return", index)
		reply.Err = ErrTimeout
		return
	}

	kv.mu.RLock()
	defer kv.mu.RUnlock()
	DPrintf(kv.context(), "get %s success", args.UUID)
	if v, ok := kv.state[args.Key]; ok {
		reply.Err = OK
		reply.Value = v
		return
	} else {
		reply.Err = ErrNoKey
		return
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.RLock()
	if kv.acks[args.UUID] {
		kv.mu.RUnlock()
		reply.Err = OK
		return
	}
	kv.mu.RUnlock()

	command := OpCommandAppend
	if args.Op == "Put" {
		command = OpCommandPut
	}
	index, _, ok := kv.rf.Start(Op{
		Command: OpCommand(command),
		Key:     args.Key,
		Value:   args.Value,
		UUID:    args.UUID,
	})
	if !ok {
		reply.WrongLeader = true
		return
	}

	if kv.waitResultTimeout(index, args.UUID) {
		DPrintf(kv.context(), "%s wait index %d timeout, return", args.Op, index)
		reply.Err = ErrTimeout
		return
	}
	DPrintf(kv.context(), "%s %s success", args.Op, args.UUID)
	reply.Err = OK
	return
}

func (kv *KVServer) waitResultTimeout(index int, uuid string) bool {
	kv.mu.Lock()
	ch := make(chan string)
	// the chan for this index might be overwrite by other requests, so this chan may never receive any message
	// we close this chan for `id := <=ch` to proceed
	if _, ok := kv.resultCh[index]; ok {
		close(kv.resultCh[index])
		delete(kv.resultCh, index)
	}
	kv.resultCh[index] = ch
	kv.mu.Unlock()

	select {
	case id := <-ch:
		return id != uuid
	case <-time.After(time.Second * 10):
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
	defer kv.mu.Unlock()

	kv.rf.Kill()
	kv.serverKilled <- true
	close(kv.serverKilled)
	for _, ch := range kv.resultCh {
		close(ch)
	}
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

	// You may need initialization code here.
	kv.state = make(map[string]string)
	kv.acks = make(map[string]bool)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.resultCh = make(map[int]chan string)
	kv.serverKilled = make(chan bool)

	// You may need initialization code here.
	go kv.readApplyCh()

	return kv
}

func (kv *KVServer) readApplyCh() {
	for {
		select {
		case msg := <-kv.applyCh:
			if !msg.CommandValid {
				break
			}
			op, ok := msg.Command.(Op)
			if !ok {
				break
			}
			kv.mu.Lock()
			if kv.acks[op.UUID] {
				kv.mu.Unlock()
				break
			}
			// DPrintf(kv.context(), "get apply msg %v", msg)
			kv.acks[op.UUID] = true
			kv.index = msg.CommandIndex
			switch op.Command {
			case OpCommandAppend:
				kv.state[op.Key] += op.Value
			case OpCommandPut:
				kv.state[op.Key] = op.Value
			default:
				// do nothing
			}
			if _, ok := kv.resultCh[kv.index]; ok {
				select {
				case kv.resultCh[kv.index] <- op.UUID:
					// do nothing
				case <-time.After(time.Millisecond * 100):
					DPrintf(kv.context(), "abandon result ch index %d", kv.index)
				}
				close(kv.resultCh[kv.index])
				delete(kv.resultCh, kv.index)
				DPrintf(kv.context(), "notify result ch index %v, delete ch", kv.index)
			}
			kv.mu.Unlock()
		case <-kv.serverKilled:
			DPrintf(kv.context(), "server killed")
			return
		}
	}
}
