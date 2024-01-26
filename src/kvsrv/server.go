package kvsrv

import (
	"sync"
)

type result struct {
	value  string
	seqNum int
}

type KVServer struct {
	lock    sync.Mutex
	data    map[string]string
	results map[int]result
}

func (kv *KVServer) hasProcessed(clerkId int, seqNum int) bool {
	if v, ok := kv.results[clerkId]; ok {
		return v.seqNum == seqNum
	}

	return false
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.lock.Lock()
	defer kv.lock.Unlock()

	reply.Value = kv.data[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.lock.Lock()
	defer kv.lock.Unlock()

	if !kv.hasProcessed(args.ClerkId, args.SeqNum) {
		kv.results[args.ClerkId] = result{
			seqNum: args.SeqNum,
		}

		kv.data[args.Key] = args.Value
	}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.lock.Lock()
	defer kv.lock.Unlock()

	if !kv.hasProcessed(args.ClerkId, args.SeqNum) {
		kv.results[args.ClerkId] = result{
			value:  kv.data[args.Key],
			seqNum: args.SeqNum,
		}

		kv.data[args.Key] += args.Value
	}

	reply.Value = kv.results[args.ClerkId].value
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.lock = sync.Mutex{}
	kv.data = map[string]string{}
	kv.results = map[int]result{}
	return kv
}
