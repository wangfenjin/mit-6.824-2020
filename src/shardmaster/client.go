package shardmaster

//
// Shardmaster clerk.
//

import (
	"crypto/rand"
	"labrpc"
	"math/big"
	"sync"
	"time"
)

type Clerk struct {
	sync.RWMutex

	servers []*labrpc.ClientEnd
	// Your data here.
	n      int
	leader int
	id     int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.n = len(servers)
	ck.id = nrand()
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.Num = num
	ck.RLock()
	index := ck.leader
	ck.RUnlock()
	var updateLeader bool
	for {
		// try each known server.
		for i := 0; i < ck.n; i++ {
			var reply QueryReply
			ok := ck.servers[index].Call("ShardMaster.Query", args, &reply)
			if ok && !reply.WrongLeader && reply.Err == OK {
				if updateLeader {
					ck.Lock()
					ck.leader = index
					ck.Unlock()
				}
				return reply.Config
			} else {
				index = (index + 1) % ck.n
				updateLeader = true
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{
		ClientId:  ck.id,
		RequestId: nrand(),
	}
	// Your code here.
	args.Servers = servers
	ck.RLock()
	index := ck.leader
	ck.RUnlock()
	for {
		for i := 0; i < ck.n; i++ {
			var reply JoinReply
			ok := ck.servers[index].Call("ShardMaster.Join", args, &reply)
			if ok && reply.WrongLeader == false && reply.Err == OK {
				ck.Lock()
				ck.leader = index
				ck.Unlock()
				return
			} else {
				index = (index + 1) % ck.n
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{
		ClientId:  ck.id,
		RequestId: nrand(),
	}
	// Your code here.
	args.GIDs = gids
	ck.RLock()
	index := ck.leader
	ck.RUnlock()
	for {
		for i := 0; i < ck.n; i++ {
			var reply LeaveReply
			ok := ck.servers[index].Call("ShardMaster.Leave", args, &reply)
			if ok && reply.WrongLeader == false && reply.Err == OK {
				ck.Lock()
				ck.leader = index
				ck.Unlock()
				return
			} else {
				index = (index + 1) % ck.n
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{
		ClientId:  ck.id,
		RequestId: nrand(),
	}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	ck.RLock()
	index := ck.leader
	ck.RUnlock()
	for {
		// try each known server.
		for i := 0; i < ck.n; i++ {
			var reply MoveReply
			ok := ck.servers[i].Call("ShardMaster.Move", args, &reply)
			if ok && reply.WrongLeader == false && reply.Err == OK {
				ck.Lock()
				ck.leader = index
				ck.Unlock()
				return
			} else {
				index = (index + 1) % ck.n
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
