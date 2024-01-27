package shardkv

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"strconv"

	"6.5840/labgob"
	"6.5840/shardctrler"
)

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

func init() {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Command{})
	labgob.Register(shardctrler.Config{})
	labgob.Register(Result{})
	labgob.Register(ShardDict{})
}

const (
	OK              Err = "OK"
	ErrNoKey        Err = "ErrNoKey"
	ErrWrongGroup   Err = "ErrWrongGroup"
	ErrWrongLeader  Err = "ErrWrongLeader"
	ErrWrongConfig  Err = "ErrWrongConfig"
	ErrNotCommitted Err = "ErrNotCommitted"
	ErrKilled       Err = "ErrKilled"
)

type shard = map[string]string

type Argser interface {
	Seq(clerkId string, seqNum int)
}

type Replier interface {
	Error() Err
	Clear()
}

type Err string

type PutAppendArgs struct {
	ClerkId string
	SeqNum  int
	Key     string
	Value   string
	Type    string
}

func (a *PutAppendArgs) Seq(clerkId string, seqNum int) {
	a.ClerkId = clerkId
	a.SeqNum = seqNum
}

type PutAppendReply struct {
	Err Err
}

func (r *PutAppendReply) Error() Err {
	return r.Err
}

func (r *PutAppendReply) Clear() {
	r.Err = ""
}

type GetArgs struct {
	ClerkId string
	SeqNum  int
	Key     string
}

func (a *GetArgs) Seq(clerkId string, seqNum int) {
	a.ClerkId = clerkId
	a.SeqNum = seqNum
}

type GetReply struct {
	Err   Err
	Value string
}

func (r *GetReply) Error() Err {
	return r.Err
}

func (r *GetReply) Clear() {
	r.Err = ""
	r.Value = ""
}

type MigrateShardArgs struct {
	ShardNum  int
	ConfigNum int
	GroupId   int
}

type MigrateShardReply struct {
	Err     Err
	Shard   shard
	Results map[string]Result
}

type ShardDict struct {
	Data [shardctrler.NShards]map[string]string
	Owns [shardctrler.NShards]bool
}

func MakeShardDict() ShardDict {
	store := ShardDict{}
	for i := 0; i < shardctrler.NShards; i++ {
		store.Data[i] = map[string]string{}
		store.Owns[i] = false
	}
	return store
}

func (s *ShardDict) KeyInstalled(key string) bool {
	shard := key2shard(key)
	return s.Owns[shard]
}

func (s *ShardDict) AddShard(shard int) {
	s.Owns[shard] = true
}

func (s *ShardDict) RemoveShard(shard int) {
	s.Owns[shard] = false
}

func (s *ShardDict) Contains(key string) bool {
	shard := key2shard(key)
	_, ok := s.Data[shard][key]
	return ok
}

func (s *ShardDict) Get(key string) string {
	shard := key2shard(key)
	return s.Data[shard][key]
}

func (s *ShardDict) Put(key string, value string) {
	shard := key2shard(key)
	s.Data[shard][key] = value
}

func (s *ShardDict) Append(key string, value string) {
	shard := key2shard(key)
	s.Data[shard][key] += value
}

// which shard is a key in?
// please use this function,
// and please do not change it.
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func id() string {
	return fmt.Sprintf("%-032s", strconv.FormatInt(nrand(), 16))[0:8]
}
