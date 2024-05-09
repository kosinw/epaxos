package epaxoskv

import (
	"crypto/rand"
	"errors"
	"fmt"
	"math/big"
	"strconv"

	"6.5840/labrpc"
)

type Clerk struct {
	servers    []*labrpc.ClientEnd
	lastLeader int
	uid        string // unique identifier for this client to send to service
	seq        int    // monotonically increasing sequence number
	// You will have to modify this struct.
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
	ck.uid = strconv.FormatInt(nrand(), 16)
	ck.seq = 1 // start at 1 to avoid annoying 0 problem with RPCs
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (s string) {
	// First try the last leader if possible.
	attempts := 0
	seqNum := ck.nextSeq()

	ck.debug(topicClerk, "GET(%v): starting (#%v)", key, seqNum)

	if ck.lastLeader != -1 {
		if v, err := ck.remoteCallGet(ck.lastLeader, key, seqNum); err == nil {
			s = v
			ck.debug(topicClerk, "GET(%v): finished %v (#%v)", key, s, seqNum)
			return
		}
		attempts++
	}

	for {
		for server := range ck.servers {
			for {
				v, err := ck.remoteCallGet(server, key, seqNum)

				if err == nil {
					s = v
					ck.lastLeader = server
					ck.debug(topicClerk, "GET(%v): finished %v (#%v)", key, s, seqNum)
					return
				} else if err.Error() == ErrNetworkTimeout {
					break
				}

				attempts++
			}
		}

		ck.debug(topicInfo, "Attempts for GET(%v): %v (#%v)", attempts, seqNum)
	}
}

// Remote procedure call into KVServer.Get.
func (ck *Clerk) remoteCallGet(server int, key string, seqNum int) (s string, err error) {
	args := GetArgs{Key: key, ClerkId: ck.uid, SeqNum: seqNum}
	reply := GetReply{}

	ck.debug(topicInfo, "-> %v GET(%v): starting attempt...", server, key)

	ok := ck.servers[server].Call("KVServer.Get", &args, &reply)

	if !ok {
		err = errors.New(ErrNetworkTimeout)
		ck.debug(topicInfo, "GET(%v): %v", key, err.Error())
		return
	}

	if reply.Err != OK {
		err = errors.New(string(reply.Err))
		ck.debug(topicInfo, "GET(%v): %v", key, err.Error())
		return
	}

	s = reply.Value
	err = nil

	return
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// First try the last leader if possible.
	attempts := 0
	seqNum := ck.nextSeq()

	ck.debug(topicClerk, "%v(%v, '%v'): starting (#%v)", op, key, value, seqNum)
	defer ck.debug(topicClerk, "%v(%v, '%v'): finished (#%v)", op, key, value, seqNum)

	if ck.lastLeader != -1 {
		if ck.remoteCallPutAppend(ck.lastLeader, key, value, op, seqNum) == nil {
			return
		}
		attempts++
	}

	for {
		for server := range ck.servers {
			for {
				err := ck.remoteCallPutAppend(server, key, value, op, seqNum)
				if err == nil {
					ck.lastLeader = server
					return
				} else if err.Error() == ErrNetworkTimeout {
					break
				}
			}

			attempts++
		}

		// TODO(kosinw): Maybe implement some sort of exponential backoff?
		ck.debug(topicInfo, "Attempts for %v(%v, '%v'): %v (#%v)", op, key, value, attempts, seqNum)
	}
}

// Remote procedure call into KVServer.PutAppend.
func (ck *Clerk) remoteCallPutAppend(server int, key string, value string, op string, seqNum int) (err error) {
	args := PutAppendArgs{Op: op, Key: key, Value: value, ClerkId: ck.uid, SeqNum: seqNum}
	reply := PutAppendReply{}

	ck.debug(topicInfo, "-> %v %v(%v, '%v'): starting attempt...", server, op, key, value)

	ok := ck.servers[server].Call("KVServer.PutAppend", &args, &reply)

	if !ok {
		err = errors.New(ErrNetworkTimeout)
		ck.debug(topicInfo, "%v(%v, '%v'): %v", op, key, value, err.Error())
		return
	}

	if reply.Err != OK {
		err = errors.New(string(reply.Err))
		ck.debug(topicInfo, "%v(%v, '%v'): %v", op, key, value, err.Error())
		return
	}

	err = nil

	return
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, OpPut)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, OpAppend)
}

func (ck *Clerk) debug(topic topic, format string, a ...interface{}) {
	debug(topic, 0, fmt.Sprintf("%v ", ck.uid[0:5])+format, a...)
}

func (ck *Clerk) nextSeq() int {
	defer func() { ck.seq++ }()
	return ck.seq
}
