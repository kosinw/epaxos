package kvsrv

import (
	"6.5840/labrpc"
)

type Clerk struct {
	server  *labrpc.ClientEnd
	clerkId int
	seqNum  int
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
	ck.clerkId = id()
	ck.seqNum = 1
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	var reply GetReply
	var args GetArgs

	args.Key = key
	args.ClerkId = ck.clerkId
	args.SeqNum = ck.seqNum

	for !ck.server.Call("KVServer.Get", &args, &reply) {
		reply.Value = ""
	}

	ck.seqNum++
	return reply.Value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) string {
	// You will have to modify this function.
	var reply PutAppendReply
	var args PutAppendArgs

	args.Key = key
	args.Value = value
	args.ClerkId = ck.clerkId
	args.SeqNum = ck.seqNum

	for !ck.server.Call("KVServer."+op, &args, &reply) {
		reply.Value = ""
	}

	ck.seqNum++
	return reply.Value
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}
