package shardctrler

import (
	"sort"
)

//
// Shard controler: assigns shards to replication groups.
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
// You will need to add fields to the RPC argument structs.
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

func MakeConfig() Config {
	return Config{
		Num:    0,
		Shards: [NShards]int{},
		Groups: map[int][]string{},
	}
}

// Clones the config (must be careful because maps and slices are reference types).
func (c *Config) Clone() Config {
	newGroups := make(map[int][]string, len(c.Groups))

	for k, v := range c.Groups {
		newSlice := make([]string, len(v))
		copy(newSlice, v)
		newGroups[k] = newSlice
	}

	return Config{Num: c.Num, Shards: c.Shards, Groups: newGroups}
}

// Gets the list of GIDs in the configuration (in a deterministic order).
func (c *Config) gids() []int {
	keys := make([]int, 0, len(c.Groups))

	for key := range c.Groups {
		keys = append(keys, key)
	}

	sort.Ints(keys)

	return keys
}

type stack struct {
	list []int
}

func newStack() *stack {
	return &stack{list: []int{}}
}

func (s *stack) size() int {
	return len(s.list)
}

func (s *stack) push(v int) {
	s.list = append(s.list, v)
}

func (s *stack) pop() (r int) {
	r = s.top()
	s.list = s.list[:len(s.list)-1]
	return
}

func (s *stack) top() int {
	return s.list[len(s.list)-1]
}

// Rebalances shards among replica groups such that:
//
//	(1) Shards are divided as evenly as possible.
//	(2) Move as few shards as possible to achieve that goal.
func (c *Config) rebalanceShards() {
	// We have no more replica groups left.
	if len(c.Groups) == 0 {
		c.Shards = [NShards]int{}
		return
	}

	gids := c.gids()                           // determinstic list of replica group IDs
	ownlist := make(map[int]*stack, len(gids)) // gid -> []sid ownership lists
	minShardCount := NShards / len(gids)       // min shard count per replica group
	freelist := newStack()

	// Step 0. Make sure ownlist doesn't have nils in them
	for _, gid := range gids {
		ownlist[gid] = newStack()
	}

	// Step 1. Set up map form gid -> []sid (which shards a group owns)
	// Step 2. Add shards with no owner to free list.
	for sid, gid := range c.Shards {
		if v, ok := ownlist[gid]; ok {
			v.push(sid)
		} else {
			freelist.push(sid)
		}
	}

	// Step 3. Calculate the number of required shards we need in freelist
	freecount := 0

	for _, gid := range gids {
		freecount += max(0, minShardCount-ownlist[gid].size())
	}

	// Step 4. Remove shards from replicas if we do not have enough in our freelist.
outer:
	for {
		for _, gid := range gids {
			if freelist.size() >= freecount {
				break outer
			}

			if ownlist[gid].size() > minShardCount {
				x := ownlist[gid].pop()
				freelist.push(x)
			}
		}
	}

	// Step 5. Allocate shards on freelist via round robin
outer2:
	for {
		for _, gid := range gids {
			if freelist.size() == 0 {
				break outer2
			}
			if (freecount <= 0) || ownlist[gid].size() < minShardCount {
				x := freelist.pop()
				ownlist[gid].push(x)
				freecount--
			}
		}
	}

	c.Shards = [NShards]int{}

	for gid, shards := range ownlist {
		for _, sid := range shards.list {
			c.Shards[sid] = gid
		}
	}
}

const (
	OK              Err = "OK"
	ErrWrongLeader      = "WrongLeader"
	ErrKilled           = "Killed"
	ErrNotCommitted     = "NotCommitted"
)

type Err string

type JoinArgs struct {
	ClerkId string
	SeqNum  int
	Servers map[int][]string // new GID -> servers mappings
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	ClerkId string
	SeqNum  int
	GIDs    []int
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	ClerkId string
	SeqNum  int
	Shard   int
	GID     int
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	ClerkId string
	SeqNum  int
	Num     int // desired config number
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}
