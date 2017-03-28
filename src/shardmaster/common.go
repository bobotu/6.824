package shardmaster

import (
	"encoding/gob"
)

//
// Master shard server: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// A GID is a replica group ID. GIDs must be uniqe and > 0.
// Once a GID joins, and leaves, it should never join again.
//
// You will need to add fields to the RPC arguments.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

const (
	OK = "OK"
)

func init() {
	gob.Register(JoinArgs{})
	gob.Register(LeaveArgs{})
	gob.Register(MoveArgs{})
	gob.Register(QueryArgs{})
}

type Err string

type JoinArgs struct {
	UniqueID int64
	PrevID   int64
	Servers  map[int][]string // new GID -> servers mappings
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	UniqueID int64
	PrevID   int64
	GIDs     []int
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	UniqueID int64
	PrevID   int64
	Shard    int
	GID      int
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	UniqueID int64
	PrevID   int64
	Num      int // desired config number
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}

func (cfg *Config) rebalance(removed []int) {
	unused := cfg.unused()
	empty := cfg.remove(removed)

	for _, shard := range empty {
		if len(unused) > 0 {
			cfg.Shards[shard] = unused[0]
			unused = unused[1:]
		} else {
			var i int
			for i = shard + 1; cfg.Shards[i%NShards] == 0; i++ {
				// find first no zero
			}
			cfg.Shards[shard] = cfg.Shards[i%NShards]
		}
	}

	busier := cfg.busier()
	var i int
	for _, shard := range busier {
		if len(unused) > 0 {
			cfg.Shards[shard] = unused[0]
			unused = unused[1:]
		} else {
			cfg.Shards[shard] = cfg.Shards[i%NShards]
			i++
		}
	}
}

func (cfg *Config) remove(removed []int) []int {
	empty := make([]int, 0)

	for i, mapped := range cfg.Shards {
		if mapped == 0 {
			empty = append(empty, i)
		} else {
			for _, gid := range removed {
				if mapped == gid {
					cfg.Shards[i] = 0
					empty = append(empty, i)
					break
				}
			}
		}
	}

	return empty
}

func (cfg *Config) unused() []int {
	unused := make([]int, 0)

	all := make([]int, 0, len(cfg.Groups))
	for k := range cfg.Groups {
		all = append(all, k)
	}

	for _, gid := range all {
		f := true
		for _, used := range cfg.Shards {
			if used == gid {
				f = false
				break
			}
		}
		if f {
			unused = append(unused, gid)
		}
	}

	return unused
}

func (cfg *Config) busier() []int {
	busier := make([]int, 0)

	counter := make(map[int]int, NShards)

	for shard, gid := range cfg.Shards {
		counter[gid]++
		if counter[gid] >= 2 {
			busier = append(busier, shard)
		}
	}

	return busier
}
