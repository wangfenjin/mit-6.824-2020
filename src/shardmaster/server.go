package shardmaster

import (
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
	if debug := os.Getenv("masterdebug"); len(debug) > 0 {
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
	OpCommandJoin
	OpCommandLeave
	OpCommandMove
	OpCommandQuery
)

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	acks         map[int64]int64
	index        int
	resultCh     map[int]chan result
	serverKilled chan bool

	configs []Config // indexed by config num
}

type result struct {
	Term  int
	Value interface{}
}

type Op struct {
	// Your data here.
	Command OpCommand
	//Key       string
	Value     interface{}
	ClientId  int64
	RequestId int64
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	DPrintf(sm.context(), "raft join %v", args.Servers)
	reply.Err, _ = sm.raft(Op{
		Command:   OpCommandJoin,
		Value:     args.Servers,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	})
	if reply.Err == ErrWrongLeader {
		reply.WrongLeader = true
	}
	return
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	DPrintf(sm.context(), "raft leave %v", args.GIDs)
	reply.Err, _ = sm.raft(Op{
		Command:   OpCommandLeave,
		Value:     args.GIDs,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	})
	if reply.Err == ErrWrongLeader {
		reply.WrongLeader = true
	}
	return
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	value := map[int]int{args.Shard: args.GID}
	DPrintf(sm.context(), "raft move %v", value)
	reply.Err, _ = sm.raft(Op{
		Command:   OpCommandMove,
		Value:     value,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	})
	if reply.Err == ErrWrongLeader {
		reply.WrongLeader = true
	}
	return
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	DPrintf(sm.context(), "raft query %v", args.Num)
	reply.Err, _ = sm.raft(Op{
		Command: OpCommandQuery,
		Value:   args.Num,
	})
	if reply.Err == ErrWrongLeader {
		reply.WrongLeader = true
		return
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()
	if args.Num == -1 {
		reply.Config = sm.configs[len(sm.configs)-1]
	} else if len(sm.configs) > args.Num {
		reply.Config = sm.configs[args.Num]
	}
	DPrintf(sm.context(), "get config %v", reply.Config)
	return
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
	sm.mu.Lock()
	defer sm.mu.Unlock()
	close(sm.serverKilled)
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	labgob.Register(map[int][]string{})
	labgob.Register([]int{})
	labgob.Register(map[int]int{})

	sm.applyCh = make(chan raft.ApplyMsg)

	// Your code here.
	sm.acks = make(map[int64]int64)
	sm.resultCh = make(map[int]chan result)
	sm.serverKilled = make(chan bool)

	go sm.readApplyCh()
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	DPrintf(sm.context(), "start shardmaster")

	return sm
}

func (sm *ShardMaster) context() context.Context {
	ctx := context.Background()
	ctx = context.WithValue(ctx, "node", sm.me)
	ctx = context.WithValue(ctx, "type", "server")
	return ctx
}

func (sm *ShardMaster) raft(command interface{}) (Err, interface{}) {
	index, term, ok := sm.rf.Start(command)
	if !ok {
		return ErrWrongLeader, ""
	}

	sm.mu.Lock()
	ch := make(chan result, 1) // make the send never block
	// the chan for this index might be overwrite by other requests, so this chan may never receive any message
	// we close this chan for `id := <=ch` to proceed
	sm.resultCh[index] = ch
	sm.mu.Unlock()

	select {
	case <-sm.serverKilled:
		return ErrWrongLeader, ""
	case r := <-ch:
		if r.Term != term {
			return ErrWrongLeader, ""
		}
		return OK, r.Value
	case <-time.After(time.Second):
		sm.mu.Lock()
		delete(sm.resultCh, index)
		sm.mu.Unlock()
		return ErrTimeout, ""
	}
}

func (sm *ShardMaster) readApplyCh() {
	for {
		select {
		case <-sm.serverKilled:
			DPrintf(sm.context(), "server killed")
			return
		case msg := <-sm.applyCh:
			sm.mu.Lock()
			if !msg.CommandValid {
				sm.mu.Unlock()
				break
			}

			sm.handleKVCommand(msg)
			sm.mu.Unlock()
		}
	}
}

func (sm *ShardMaster) handleKVCommand(msg raft.ApplyMsg) {
	// validate msg
	if msg.CommandIndex <= sm.index {
		// ignore old messages
		DPrintf(sm.context(), "get old msg %v", msg.CommandIndex)
		return
	}
	if msg.CommandIndex != sm.index+1 {
		errMsg := fmt.Sprintf("server %d index %d get command index %d", sm.me, sm.index, msg.CommandIndex)
		DPrintf(sm.context(), errMsg)
		panic(errMsg)
	}
	sm.index = msg.CommandIndex

	// handle cmd
	op, _ := msg.Command.(Op)
	if op.Command != OpCommandQuery {
		if sm.acks[op.ClientId] != op.RequestId {
			DPrintf(sm.context(), "get apply msg %v", msg.CommandIndex)
			// filter old message
			switch op.Command {
			case OpCommandJoin:
				gids := op.Value.(map[int][]string)
				// copy old
				conf := sm.configs[len(sm.configs)-1]
				var newConf Config
				newConf.Num = conf.Num + 1
				newConf.Groups = make(map[int][]string, len(conf.Groups)+len(gids))
				for k, v := range conf.Groups {
					newConf.Groups[k] = v
				}

				// generate new
				for gid, server := range gids {
					newConf.Groups[gid] = server
				}
				newConf.Shards = sm.generateShards(newConf.Groups)
				sm.configs = append(sm.configs, newConf)
			case OpCommandLeave:
				gids := op.Value.([]int)
				conf := sm.configs[len(sm.configs)-1]
				var newConf Config
				newConf.Num = conf.Num + 1
				newConf.Groups = make(map[int][]string, len(conf.Groups))
				for k, v := range conf.Groups {
					newConf.Groups[k] = v
				}
				for _, gid := range gids {
					if _, ok := conf.Groups[gid]; ok {
						delete(newConf.Groups, gid)
					}
				}
				newConf.Shards = sm.generateShards(newConf.Groups)
				sm.configs = append(sm.configs, newConf)
			case OpCommandMove:
				conf := sm.configs[len(sm.configs)-1]
				newConf := Config{Num: conf.Num + 1}
				newConf.Groups = make(map[int][]string, len(conf.Groups))
				for k, v := range conf.Groups {
					newConf.Groups[k] = v
				}
				for k, v := range conf.Shards {
					newConf.Shards[k] = v
				}
				shardToGid := op.Value.(map[int]int)
				for shard, gid := range shardToGid {
					if newConf.Shards[shard] != gid {
						newConf.Shards[shard] = gid
					}
				}
				sm.configs = append(sm.configs, newConf)
			default:
				// do nothing
			}
		}
		sm.acks[op.ClientId] = op.RequestId
	}
	// notify new result
	if ch, ok := sm.resultCh[sm.index]; ok {
		delete(sm.resultCh, sm.index)
		select {
		case <-sm.serverKilled:
			return
		case ch <- result{Term: msg.CommandTerm}:
			DPrintf(sm.context(), "notify result ch index %v success", sm.index)
		case <-time.After(time.Millisecond * 30):
			DPrintf(sm.context(), "abandon result ch index %d", sm.index)
		}
	}
}

func (sm *ShardMaster) generateShards(groups map[int][]string) [NShards]int {
	var r [NShards]int
	ng := len(groups)
	if ng == 0 {
		return r
	}

	gids := make([]int, 0, len(groups))
	for k := range groups {
		gids = append(gids, k)
	}
	for i := 0; i < NShards; i++ {
		r[i] = gids[i%len(groups)]
	}
	return r
}
