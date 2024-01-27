package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"time"

	"6.5840/labrpc"
	"6.5840/shardctrler"
)

type Clerk struct {
	sm          *shardctrler.Clerk
	config      shardctrler.Config
	make_end    func(string) *labrpc.ClientEnd
	lastLeaders map[int]int // gid -> replica #, used to cache the last known leader
	clerkId     string      // unique identifier for clerk
	seqNum      int
}

// Sends an RPC to the appropriate replica group given a key.
// Returns when response has been received.
func (ck *Clerk) call(method string, shard int, args Argser, reply Replier) {
	s := ck.seqNum
	ck.seqNum++

	for {
		gid := ck.config.Shards[shard]

		if servers, ok := ck.config.Groups[gid]; ok {
			start := ck.lastLeaders[gid]

			for si := 0; si < len(servers); si++ {
				i := (start + si) % len(servers)
				srv := ck.make_end(servers[i])
				args.Seq(ck.clerkId, s)
				reply.Clear()
				ok := srv.Call(method, args, reply)
				if ok && reply.Error() == OK {
					ck.lastLeaders[gid] = i
					return
				}
				if ok && reply.Error() == ErrWrongGroup {
					break
				}

				// While we are getting not installed, we should just keep
				// retrying the same group
				// ... not ok, or ErrWrongLeader
			}
		}

		time.Sleep(100 * time.Millisecond)
		ck.config = ck.sm.Query(-1)
	}
}

// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	ck.clerkId = id()
	ck.seqNum = 1
	ck.lastLeaders = map[int]int{}
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
func (ck *Clerk) Get(key string) string {
	var (
		args  GetArgs
		reply GetReply
	)

	args.Key = key

	ck.call("ShardKV.Get", key2shard(key), &args, &reply)
	return reply.Value
}

// shared by Put and Append.
// You will have to modify this function.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	var (
		args  PutAppendArgs
		reply PutAppendReply
	)

	args.Key = key
	args.Type = op
	args.Value = value

	ck.call("ShardKV.PutAppend", key2shard(key), &args, &reply)
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "PUT")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "APPEND")
}
