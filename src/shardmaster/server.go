package shardmaster

import (
	"encoding/gob"
	"sync"
	"time"

	"labrpc"
	"raft"
)

type request struct {
	uniqueID int64
	resp     chan interface{}
}

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	configs []Config // indexed by config num

	request    chan request
	requestBuf []request

	close chan struct{}
}

const (
	typeJoin  = iota
	typeLeave
	typeMove
	typeQuery
)

type Op struct {
	UniqueID int64
	PrevID   int64
	Arg      interface{}
	Type     int
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	if sm.doRPC(*args, args.UniqueID, typeJoin) {
		reply.WrongLeader = false
		reply.Err = OK
	} else {
		reply.Err = "error: not leader"
		reply.WrongLeader = true
	}
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	if sm.doRPC(*args, args.UniqueID, typeLeave) {
		reply.WrongLeader = false
		reply.Err = OK
	} else {
		reply.Err = "error: not leader"
		reply.WrongLeader = true
	}
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	if sm.doRPC(*args, args.UniqueID, typeMove) {
		reply.WrongLeader = false
		reply.Err = OK
	} else {
		reply.Err = "error: not leader"
		reply.WrongLeader = true
	}
}

func (sm *ShardMaster) doRPC(args interface{}, id int64, t int) bool {
	if _, isLeader := sm.rf.GetState(); !isLeader {
		return false
	}

	resp := make(chan interface{}, 1)
	sm.request <- request{uniqueID: id, resp: resp}

	if _, term, isLeader := sm.rf.Start(Op{Type: t, Arg: args, UniqueID: id}); !isLeader {
		return false
	} else {
		for {
			select {
			case <-resp:
				return true
			case <-time.After(10 * time.Millisecond):
				if ct, isLeader := sm.rf.GetState(); ct != term || !isLeader {
					return false
				}
			}
		}
	}
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	reply.Err = "error: not leader"
	reply.WrongLeader = true

	if _, isLeader := sm.rf.GetState(); !isLeader {
		return
	}

	resp := make(chan interface{}, 1)
	sm.request <- request{uniqueID: args.UniqueID, resp: resp}

	if _, term, isLeader := sm.rf.Start(Op{Type: typeQuery, Arg: *args, UniqueID: args.UniqueID}); !isLeader {
		return
	} else {
		for {
			select {
			case r := <-resp:
				reply.WrongLeader = false
				reply.Err = OK
				reply.Config = r.(Config)
				return
			case <-time.After(10 * time.Millisecond):
				if ct, isLeader := sm.rf.GetState(); ct != term || !isLeader {
					return
				}
			}
		}
	}

}

func (sm *ShardMaster) mainLoop() {
	duplicate := make(map[int64]bool)

	for cmd := range sm.applyCh {
		var config Config
		var reply interface{}
		var update bool

		op := cmd.Command.(Op)
		switch op.Type {
		case typeJoin:
			config = sm.newJoinCfg(op.Arg.(JoinArgs))
			update = true
		case typeMove:
			config = sm.newMoveCfg(op.Arg.(MoveArgs))
			update = true
		case typeLeave:
			config = sm.newLeaveCfg(op.Arg.(LeaveArgs))
			update = true
		case typeQuery:
			reply = sm.configAt(op.Arg.(QueryArgs).Num)
			update = false
		default:
			continue
		}

		if update {
			sm.mu.Lock()
			sm.configs = append(sm.configs, config)
			sm.mu.Unlock()
		}

		delete(duplicate, op.PrevID)

		if _, isLeader := sm.rf.GetState(); isLeader {
			if duplicate[op.UniqueID] {
				continue
			}

			if req := sm.requestForID(op.UniqueID); req.resp != nil {
				req.resp <- reply
			}
		}

		duplicate[op.UniqueID] = true
	}
}

func (sm *ShardMaster) lastConfig() (cfg Config) {
	cfg = sm.configs[len(sm.configs)-1]
	return
}

func (sm *ShardMaster) configAt(i int) Config {
	if i < 0 {
		return sm.lastConfig()
	}

	if i >= len(sm.configs) {
		return sm.configs[0]
	}

	return sm.configs[i]
}

func (sm *ShardMaster) requestForID(id int64) request {
	for i, req := range sm.requestBuf {
		if req.uniqueID == id {
			sm.requestBuf = append(sm.requestBuf[:i], sm.requestBuf[i+1:]...)
			return req
		}
	}

	for req := range sm.request {
		if req.uniqueID == id {
			return req
		} else {
			sm.requestBuf = append(sm.requestBuf, req)
		}
	}

	return request{}
}

func (sm *ShardMaster) newJoinCfg(args JoinArgs) (cfg Config) {
	prevCfg := sm.lastConfig()

	cfg.Groups = make(map[int][]string, len(args.Servers)+len(prevCfg.Groups))
	for k, v := range prevCfg.Groups {
		cfg.Groups[k] = append(cfg.Groups[k], v...)
	}
	for k, v := range args.Servers {
		for _, n := range v {
			var e bool
			for _, old := range cfg.Groups[k] {
				if old == n {
					e = true
				}
			}
			if !e {
				cfg.Groups[k] = append(cfg.Groups[k], n)
			}
		}
	}

	cfg.Shards = prevCfg.Shards
	cfg.rebalance(nil)

	cfg.Num = prevCfg.Num + 1

	return
}

func (sm *ShardMaster) newMoveCfg(args MoveArgs) (cfg Config) {
	prevCfg := sm.lastConfig()

	cfg.Groups = prevCfg.Groups
	cfg.Shards = prevCfg.Shards
	cfg.Num = prevCfg.Num + 1

	if args.Shard >= 0 && args.Shard < NShards {
		cfg.Shards[args.Shard] = args.GID
	}

	return
}

func (sm *ShardMaster) newLeaveCfg(args LeaveArgs) (cfg Config) {
	prevCfg := sm.lastConfig()

	cfg.Groups = make(map[int][]string, len(prevCfg.Groups))
	for k, v := range prevCfg.Groups {
		cfg.Groups[k] = v
	}
	for _, gid := range args.GIDs {
		delete(cfg.Groups, gid)
	}

	cfg.Shards = prevCfg.Shards
	cfg.rebalance(args.GIDs)

	cfg.Num = prevCfg.Num + 1

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
	close(sm.close)
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

	gob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	sm.request = make(chan request, 1024)
	sm.requestBuf = make([]request, 0, 5)
	sm.close = make(chan struct{})

	go sm.mainLoop()

	return sm
}
