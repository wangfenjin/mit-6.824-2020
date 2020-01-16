package shardkv

// import "shardmaster"
import (
	"bytes"
	"context"
	"fmt"
	"labgob"
	"labrpc"
	"log"
	"os"
	"raft"
	"shardmaster"
	"sync"
	"time"
)

var Debug = "0"

func init() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	if debug := os.Getenv("svdebug"); len(debug) > 0 {
		Debug = debug
	}
}

func DPrintf(ctx context.Context, format string, a ...interface{}) (n int, err error) {
	if Debug != "0" {
		log.Printf("%s, node: %d, gid: %d, num: %d,log: %s", ctx.Value("type"), ctx.Value("node"), ctx.Value("gid"), ctx.Value("num"), fmt.Sprintf(format, a...))
	}
	return
}

type OpCommand int

const (
	OpCommandNone = iota
	OpCommandGet
	OpCommandPut
	OpCommandAppend
	OpCommandMigrate
	OpCommandNewConfig
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
	State     map[int]map[string]string
	Acks      map[int]map[int64]int64
	Config    shardmaster.Config
}

type ShardKV struct {
	mu           sync.RWMutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	state        map[int]map[string]string
	acks         map[int]map[int64]int64
	index        int
	resultCh     map[int]chan result
	serverKilled chan bool

	// snapshot
	persister   *raft.Persister
	snapshoting bool

	// shard
	sm        *shardmaster.Clerk
	config    shardmaster.Config
	reloadNow chan bool
	migrating bool
}

type result struct {
	Term  int
	Value string
}

func (kv *ShardKV) context() context.Context {
	ctx := context.Background()
	ctx = context.WithValue(ctx, "node", kv.me)
	ctx = context.WithValue(ctx, "type", "sk")
	ctx = context.WithValue(ctx, "gid", kv.gid)
	ctx = context.WithValue(ctx, "num", kv.config.Num)
	return ctx
}

func (kv *ShardKV) Migrate(args *MigrateArgs, reply *MigrateReply) {
	if _, leader := kv.rf.GetState(); !leader {
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		return
	}
	for {
		kv.mu.RLock()
		if kv.config.Num >= args.Num {
			kv.mu.RUnlock()
			break
		}
		kv.mu.RUnlock()
		time.Sleep(time.Millisecond * 10)
		DPrintf(kv.context(), "wait migrate request %d", args.Num)
	}

	DPrintf(kv.context(), "start to migrate request num %d, shard %d", args.Num, args.Shard)
	reply.Err, _ = kv.raft(&Op{
		Command: OpCommandMigrate,
		State:   map[int]map[string]string{args.Shard: args.State},
		Acks:    map[int]map[int64]int64{args.Shard: args.Acks},
	})
	if reply.Err == ErrWrongLeader {
		reply.WrongLeader = true
	}
	DPrintf(kv.context(), "migrate reply %v num %d, shard %d", reply, args.Num, args.Shard)
	return
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if err := kv.checkLeader(args.Key); err != OK {
		reply.Err = err
		return
	}
	reply.Err, reply.Value = kv.raft(Op{
		Command: OpCommandGet,
		Key:     args.Key,
	})
	if reply.Err == ErrWrongLeader {
		reply.WrongLeader = true
	}
	return
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	defer func() {
		DPrintf(kv.context(), "%s %s:%s, %d, reply %v", args.Op, args.Key, args.Value, args.RequestId, reply.Err)
	}()
	if err := kv.checkLeader(args.Key); err != OK {
		reply.Err = err
		return
	}
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

func (kv *ShardKV) checkLeader(key string) Err {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	shard := key2shard(key)
	if kv.config.Shards[shard] != kv.gid {
		DPrintf(kv.context(), "conf %d, shard gid %d", kv.config.Num, kv.config.Shards[shard])
		return ErrWrongGroup
	}
	if _, ok := kv.state[shard]; !ok {
		DPrintf(kv.context(), "state not migrate for shard %v, state %v", shard, kv.state)
		return ErrWrongGroup
	}
	return OK
}

func (kv *ShardKV) raft(command interface{}) (Err, string) {
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
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	// Your code here, if desired.
	kv.mu.Lock()
	close(kv.serverKilled)
	kv.rf.Kill()
	DPrintf(kv.context(), "kill server")
	kv.mu.Unlock()
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(map[int]map[string]string{})
	labgob.Register(map[int]map[int64]int64{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters
	kv.sm = shardmaster.MakeClerk(masters)

	// Your initialization code here.

	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.state = make(map[int]map[string]string)
	kv.acks = make(map[int]map[int64]int64)
	/**
	for i := 0; i < shardmaster.NShards; i++ {
		kv.state[i] = make(map[string]string)
		kv.acks[i] = make(map[int64]int64)
	}
	*/
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.resultCh = make(map[int]chan result)
	kv.serverKilled = make(chan bool)
	kv.persister = persister

	kv.readPersist()
	go kv.readApplyCh()
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.reloadNow = make(chan bool)
	go kv.reloadConfig()

	DPrintf(kv.context(), "start kvserver")
	return kv
}

func (kv *ShardKV) reloadConfig() {
	for {
		select {
		case <-kv.serverKilled:
			return
		case <-kv.reloadNow:
		case <-time.After(time.Millisecond * 100):
		}
		if _, leader := kv.rf.GetState(); !leader {
			continue
		}

		kv.mu.RLock()
		newNum := kv.config.Num + 1
		//old := kv.config
		kv.mu.RUnlock()

		new := kv.sm.Query(newNum)
		if new.Num != newNum {
			continue
		}

		DPrintf(kv.context(), "start to reload %d", new.Num)
		if ok, _ := kv.raft(Op{
			Command: OpCommandNewConfig,
			Config:  new,
		}); ok != OK {
			DPrintf(kv.context(), "raft new config %d error %v", new.Num, ok)
		}
	}
}

func (kv *ShardKV) checkConfig(old, new shardmaster.Config) {
	var wg sync.WaitGroup
	for i := 0; i < shardmaster.NShards; i++ {
		if new.Shards[i] == kv.gid && old.Shards[i] == 0 {
			// new conf
			DPrintf(kv.context(), "init to serve shard %d", i)
			kv.state[i] = make(map[string]string)
			kv.acks[i] = make(map[int64]int64)
			continue
		}
		if new.Shards[i] != kv.gid && old.Shards[i] == kv.gid {
			// need to send migrate
			wg.Add(1)
			go kv.startMigrate(&wg, new, i)
		}
	}
	if waitTimeout(&wg, time.Second) {
		msg := fmt.Sprintf("wait check config %v timeout", new.Num)
		panic(msg)
	}
}

func (kv *ShardKV) startMigrate(wg *sync.WaitGroup, new shardmaster.Config, shard int) {
	defer wg.Done()

	DPrintf(kv.context(), "start migrate shard %d", shard)
	if _, leader := kv.rf.GetState(); !leader {
		DPrintf(kv.context(), "start migrate shard %d, not leader return", shard)
		return
	}
	DPrintf(kv.context(), "start migrate shard %d, real do", shard)
	kv.mu.RLock()
	toGid := new.Shards[shard]
	servers := new.Groups[toGid]
	state := kv.state[shard]
	acks := kv.acks[shard]
	kv.mu.RUnlock()

	for {
		for si := 0; si < len(servers); si++ {
			srv := kv.make_end(servers[si])
			var reply MigrateReply
			ok := srv.Call("ShardKV.Migrate", &MigrateArgs{
				Num:   new.Num,
				Shard: shard,
				State: state,
				Acks:  acks,
			}, &reply)
			if ok && !reply.WrongLeader && reply.Err == OK {
				// delete shard after migration
				kv.mu.Lock()
				delete(kv.state, shard)
				kv.mu.Unlock()
				DPrintf(kv.context(), "migrate shard %d to group %d success", shard, toGid)
				return
			}
		}
		DPrintf(kv.context(), "migrate shard %d to group %d failed, will retry...", shard, toGid)
		time.Sleep(time.Millisecond * 10)
	}
}

func (kv *ShardKV) snapshot() {
	if !kv.snapshoting && kv.maxraftstate > 0 &&
		kv.persister.RaftStateSize() > kv.maxraftstate {
		kv.snapshoting = true
		go kv.rf.SaveSnapshot(kv.getPersistData(), kv.index)
	}
}

func (kv *ShardKV) getPersistData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.state)
	e.Encode(kv.acks)
	e.Encode(kv.index)
	e.Encode(kv.config)
	return w.Bytes()
}

func (kv *ShardKV) readPersist() {
	data := kv.persister.ReadSnapshot()
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var state map[int]map[string]string
	var acks map[int]map[int64]int64
	var index int
	var config shardmaster.Config
	if d.Decode(&state) != nil ||
		d.Decode(&acks) != nil ||
		d.Decode(&index) != nil ||
		d.Decode(&config) != nil {
		panic("decode snapshot error")
	} else {
		kv.state = state
		kv.acks = acks
		kv.index = index
		kv.config = config
		DPrintf(kv.context(), "read persist succeed, snapshot index %d", kv.index)
	}
}

func (kv *ShardKV) readApplyCh() {
	for {
		select {
		case <-kv.serverKilled:
			DPrintf(kv.context(), "server killed")
			return
		case msg := <-kv.applyCh:
			DPrintf(kv.context(), "start read msg %d", msg.CommandIndex)
			kv.mu.Lock()
			if !msg.CommandValid {
				kv.handleSnapshotCmd(msg)
				kv.mu.Unlock()
				break
			}

			kv.handleKVCommand(msg)
			kv.snapshot()
			kv.mu.Unlock()
			DPrintf(kv.context(), "end read msg %d", msg.CommandIndex)
		}
	}
}

func (kv *ShardKV) handleSnapshotCmd(msg raft.ApplyMsg) {
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

func (kv *ShardKV) handleKVCommand(msg raft.ApplyMsg) {
	// validate msg
	if msg.CommandIndex <= kv.index {
		DPrintf(kv.context(), "ignore old message index %d", msg.CommandIndex)
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
	DPrintf(kv.context(), "get apply msg %v, op %v", msg, op)
	switch op.Command {
	case OpCommandAppend:
		shard := key2shard(op.Key)
		if kv.acks[shard][op.ClientId] != op.RequestId {
			kv.state[shard][op.Key] += op.Value
			kv.acks[shard][op.ClientId] = op.RequestId
		}
	case OpCommandPut:
		shard := key2shard(op.Key)
		if kv.acks[shard][op.ClientId] != op.RequestId {
			kv.state[shard][op.Key] = op.Value
			kv.acks[shard][op.ClientId] = op.RequestId
		}
	case OpCommandNewConfig:
		DPrintf(kv.context(), "raft get config %d", op.Config.Num)
		if op.Config.Num != kv.config.Num+1 {
			DPrintf(kv.context(), "config new %d, old %d, break", op.Config.Num, kv.config.Num)
			break
		}
		old := kv.config
		go kv.checkConfig(old, op.Config)
		kv.config = op.Config
		DPrintf(kv.context(), "raft reload config %d success", op.Config.Num)
	case OpCommandMigrate:
		DPrintf(kv.context(), "before migrate state %v, acks %v", kv.state, kv.acks)
		for k, v := range op.State {
			DPrintf(kv.context(), "migrate to serve shard %v", k)
			kv.state[k] = v
		}
		for k, v := range op.Acks {
			kv.acks[k] = v
		}
		DPrintf(kv.context(), "after migrate state %v, acks %v", kv.state, kv.acks)
	default:
		// do nothing
	}
	// notify new result
	if ch, ok := kv.resultCh[kv.index]; ok {
		delete(kv.resultCh, kv.index)
		select {
		case <-kv.serverKilled:
			return
		case ch <- result{Term: msg.CommandTerm, Value: kv.state[key2shard(op.Key)][op.Key]}:
			DPrintf(kv.context(), "notify result ch index %v success", kv.index)
		case <-time.After(time.Millisecond * 30):
			DPrintf(kv.context(), "abandon result ch index %d", kv.index)
		}
	}
}
