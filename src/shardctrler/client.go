package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"strconv"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	servers    []*labrpc.ClientEnd
	lastLeader int    // id of server known to be most recent leader
	clerkId    string // unique identifier for this client to send to services
	seqNum     int    // monotonically increasing sequence number (for duplicate detection)
}

func (ck *Clerk) debug(topic topic, format string, a ...interface{}) {
	debug(topic, 0, format+fmt.Sprintf(" (%v | %v)", ck.clerkId, ck.seqNum), a...)
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
	ck.lastLeader = -1
	ck.clerkId = strconv.FormatInt(nrand(), 16)
	ck.seqNum = 1
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.Num = num
	args.ClerkId = ck.clerkId
	args.SeqNum = ck.seqNum

	ck.debug(topicClerk, "Starting RPC for QUERY(num: %v)", args.Num)
	defer func() {
		ck.debug(topicClerk, "Finishing RPC for QUERY(num: %v)", args.Num)
		ck.seqNum++
	}()

	// First try cached leader.
	if ck.lastLeader != -1 {
		var reply QueryReply
		ok := ck.servers[ck.lastLeader].Call("ShardCtrler.Query", args, &reply)
		if ok && reply.WrongLeader == false {
			return reply.Config
		}
	}

	for {
		// try each known server.
		for id, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.lastLeader = id
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers
	args.ClerkId = ck.clerkId
	args.SeqNum = ck.seqNum

	ck.debug(topicClerk, "Starting RPC for JOIN(servers: %v)", args.Servers)
	defer func() {
		ck.debug(topicClerk, "Finishing RPC for JOIN(servers: %v)", args.Servers)
		ck.seqNum++
	}()

	// First try cached leader.
	if ck.lastLeader != -1 {
		var reply JoinReply
		ok := ck.servers[ck.lastLeader].Call("ShardCtrler.Join", args, &reply)
		if ok && reply.WrongLeader == false {
			return
		}
	}

	for {
		// try each known server.
		for id, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardCtrler.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.lastLeader = id
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids
	args.ClerkId = ck.clerkId
	args.SeqNum = ck.seqNum

	ck.debug(topicClerk, "Starting RPC for LEAVE(gids: %v)", args.GIDs)
	defer func() {
		ck.debug(topicClerk, "Finishing RPC for LEAVE(gids: %v)", args.GIDs)
		ck.seqNum++
	}()

	// First try cached leader.
	if ck.lastLeader != -1 {
		var reply LeaveReply
		ok := ck.servers[ck.lastLeader].Call("ShardCtrler.Leave", args, &reply)
		if ok && reply.WrongLeader == false {
			return
		}
	}

	for {
		// try each known server.
		for id, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardCtrler.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.lastLeader = id
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	args.ClerkId = ck.clerkId
	args.SeqNum = ck.seqNum

	ck.debug(topicClerk, "Starting RPC for MOVE(shard: %v, gid: %v)", args.Shard, args.GID)
	defer func() {
		ck.debug(topicClerk, "Finishing RPC for MOVE(shard: %v, gid: %v)", args.Shard, args.GID)
		ck.seqNum++
	}()

	// First try cached leader.
	if ck.lastLeader != -1 {
		var reply MoveReply
		ok := ck.servers[ck.lastLeader].Call("ShardCtrler.Move", args, &reply)
		if ok && reply.WrongLeader == false {
			return
		}
	}

	for {
		// try each known server.
		for id, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardCtrler.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.lastLeader = id
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
