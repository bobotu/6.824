package raftkv

import (
	"crypto/rand"
	"labrpc"
	"math/big"
)

type Clerk struct {
	servers []*labrpc.ClientEnd

	supposedLeader int
	prevID         int64
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
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	args := GetArgs{
		Key:      key,
		UniqueID: nrand(),
		PrevID:   ck.prevID,
	}

	DPrintf("get %s start\n", key)

	for {
		total := len(ck.servers)
		for i, l := 0, ck.supposedLeader; i < total; i, l = i+1, (l+1)%total {
			var reply GetReply
			if ok := ck.servers[i].Call("RaftKV.Get", &args, &reply); ok && !reply.WrongLeader {
				ck.supposedLeader = i
				ck.prevID = args.UniqueID
				return reply.Value
			}
		}
		DPrintf("no leader!\n")
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{
		Key:      key,
		Op:       op,
		Value:    value,
		UniqueID: nrand(),
		PrevID:   ck.prevID,
	}

	DPrintf("%s %s start\n", op, key)

	for {
		total := len(ck.servers)
		for i, l := 0, ck.supposedLeader; i < total; i, l = i+1, (l+1)%total {
			var reply PutAppendReply
			if ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply); ok && !reply.WrongLeader {
				ck.supposedLeader = i
				ck.prevID = args.UniqueID
				return
			}
		}
		DPrintf("no leader!\n")
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
