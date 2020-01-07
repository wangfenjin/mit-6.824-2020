package raftkv

import (
	"crypto/rand"
	"labrpc"
	"math/big"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	n      int // server number
	leader int

	id int64
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
	// You'll have to add code here.
	ck.n = len(servers)
	ck.leader = 0
	ck.id = nrand()
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	args := GetArgs{
		Key:  key,
		UUID: nrand(),
	}
	index := ck.leader
	for {
		var reply GetReply
		ok := ck.servers[index].Call("KVServer.Get", &args, &reply)
		if !ok || reply.WrongLeader {
			index = (index + 1) % ck.n
			continue
		}
		if reply.Err == OK || reply.Err == ErrNoKey {
			ck.leader = index
			return reply.Value
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{
		Op:    op,
		Key:   key,
		Value: value,
		UUID:  nrand(),
	}
	index := ck.leader
	for {
		var reply PutAppendReply
		ok := ck.servers[index].Call("KVServer.PutAppend", &args, &reply)
		if !ok || reply.WrongLeader {
			index = (index + 1) % ck.n
			continue
		}
		if reply.Err == OK {
			ck.leader = index
			return
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
