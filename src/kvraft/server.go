package raftkv

import (
	"bytes"
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
	"unsafe"
)

func init() {
	gob.Register(Snapshot{})
}

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const (
	opPut = iota
	opAppend
	opGet
)

type Op struct {
	OpType   int
	Key      string
	Value    string
	UniqueID int64
	PrevID   int64
}

type request struct {
	term  int
	reply chan string
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	kv map[string]string

	maxraftstate int // snapshot if log grows this big

	requests  map[int64]request
	duplicate map[int64]struct{}
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	op := Op{
		OpType:   opGet,
		Key:      args.Key,
		UniqueID: args.UniqueID,
		PrevID:   args.PrevID,
	}

	reply.WrongLeader = true

	if term, isLeader := kv.rf.GetState(); isLeader {
		resp := make(chan string, 1)
		kv.mu.Lock()
		kv.requests[args.UniqueID] = request{term, resp}
		kv.mu.Unlock()
		if _, _, isLeader = kv.rf.Start(op); isLeader {
			for {
				select {
				case <-time.After(10 * time.Millisecond):
					if ct, isLeader := kv.rf.GetState(); ct != term || !isLeader {
						reply.WrongLeader = true
						return
					}
				case reply.Value = <-resp:
					reply.WrongLeader = false
					return
				}
			}
		}
	}
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	op := Op{
		Key:      args.Key,
		Value:    args.Value,
		UniqueID: args.UniqueID,
		PrevID:   args.PrevID,
	}

	reply.WrongLeader = true

	if args.Op == "Put" {
		op.OpType = opPut
	} else {
		op.OpType = opAppend
	}

	if term, isLeader := kv.rf.GetState(); isLeader {
		resp := make(chan string, 1)

		kv.mu.Lock()
		kv.requests[args.UniqueID] = request{term, resp}
		kv.mu.Unlock()

		if _, _, isLeader = kv.rf.Start(op); isLeader {
			for {
				select {
				case <-time.After(10 * time.Millisecond):
					if ct, isLeader := kv.rf.GetState(); ct != term || !isLeader {
						reply.WrongLeader = true
						return
					}
				case <-resp:
					reply.WrongLeader = false
					return
				}
			}
		}
	}
}

type Snapshot struct {
	KV         map[string]string
	Duplicates map[int64]struct{}
}

func (kv *RaftKV) applyOpLoop() {
	var stateSize int64
	logSize := int64(unsafe.Sizeof(raft.Log{}))
	for msg := range kv.applyCh {
		if msg.UseSnapshot {
			kv.mu.Lock()
			var snap Snapshot
			gob.NewDecoder(bytes.NewReader(msg.Snapshot)).Decode(&snap)
			kv.kv = snap.KV
			kv.duplicate = snap.Duplicates
			kv.mu.Unlock()
			continue
		}

		op := msg.Command.(Op)

		var reply string

		switch op.OpType {
		case opPut:
			if _, ex := kv.duplicate[op.UniqueID]; !ex {
				kv.kv[op.Key] = op.Value
			}
		case opAppend:
			if _, ex := kv.duplicate[op.UniqueID]; !ex {
				kv.kv[op.Key] += op.Value
			}
		case opGet:
			reply = kv.kv[op.Key]
		}

		kv.duplicate[op.UniqueID] = struct{}{}
		delete(kv.duplicate, op.PrevID)

		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.mu.Lock()

			if req, ok := kv.requests[op.UniqueID]; ok {
				delete(kv.requests, op.UniqueID)
				DPrintf("reply\n")
				req.reply <- reply
				DPrintf("reply done\n")
			}

			kv.mu.Unlock()
		}

		stateSize += int64(unsafe.Sizeof(msg.Command)) + logSize

		if kv.maxraftstate > 0 && stateSize > int64(kv.maxraftstate)/4*3 {
			buf := bytes.NewBuffer(nil)
			enc := gob.NewEncoder(buf)
			enc.Encode(Snapshot{
				KV:         kv.kv,
				Duplicates: kv.duplicate,
			})

			kv.rf.SaveSnapshot(raft.Snapshot{
				Index: msg.Index,
				Data:  buf.Bytes(),
			})

			stateSize = 0
		}
	}
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.kv = make(map[string]string)
	kv.requests = make(map[int64]request)
	kv.duplicate = make(map[int64]struct{})

	kv.applyCh = make(chan raft.ApplyMsg)

	go kv.applyOpLoop()

	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	return kv
}
