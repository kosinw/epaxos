package epaxoskv

import (
	"sync"
	"sync/atomic"
	"time"

	"6.5840/epaxos"
	"6.5840/labgob"
	"6.5840/labrpc"
)

type Command struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op      string // type of operation, one of (PUT, GET, APPEND)
	Key     string // key (e.g. "x")
	Value   string // value (e.g. "3")
	ClerkId string // unique ID for the clerk that requested this operation
	SeqNum  int    // monotonically increasing sequence number (per clerk)
}

type SeqValue struct {
	SeqNum int
	Value  string
}

type KVServer struct {
	lock       sync.Mutex
	me         int
	Ep         *epaxos.EPaxos
	applyCh    chan epaxos.Instance
	dead       int32                   // set by Kill()
	persister  *epaxos.Persister       // Object to hold this server's persisted state
	kvs        map[string]string       // k/v store which holds data for the service
	duplicates map[string]SeqValue     // client ID -> (seq, value) table for duplicate detection
	applyIndex []int                   // log index of most recently applied operation
	applied    map[epaxos.LogIndex]int // set of already applied operations
}

// RPC Handler for Get.
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here (4A).
	kv.lock.Lock()
	defer kv.lock.Unlock()

	kv.debug(topicInfo, "Starting GET(%v) for %v #%v", args.Key, args.ClerkId[0:5], args.SeqNum)

	reply.Value = ""

	// Check if we have already seen this exact request.
	if kv.hasProcessed(args.ClerkId, args.SeqNum) {
		reply.Value = kv.duplicates[args.ClerkId].Value
		reply.Err = OK
		kv.debug(topicService, "Succeeded GET(%v): '%v' (duplicate)", args.Key, reply.Value)
		return
	}

	// Prepare a log entry for a GET (a no-op), then try to submit
	// that to our EPaxos instance.
	cmd := Command{Op: OpGet, Key: args.Key, ClerkId: args.ClerkId, SeqNum: args.SeqNum}

	logIndex := kv.Ep.Start(cmd)

	kv.debug(topicInfo, "Running GET(%v) for %v #%v", args.Key, args.ClerkId[0:5], args.SeqNum)

	// We sleep on this goroutine until the applyIndex has not
	// yet reached this operation.
	t0 := time.Now()
	for time.Since(t0) < 4*time.Second {
		if kv.killed() { // this server has been killed
			return
		}

		if _, ok := kv.applied[logIndex]; ok {
			break
		}

		kv.lock.Unlock()
		time.Sleep(10 * time.Millisecond)
		kv.lock.Lock()
	}

	// We can read the value of the applied operation
	if kv.hasProcessed(args.ClerkId, args.SeqNum) {
		reply.Value = kv.duplicates[args.ClerkId].Value
		reply.Err = OK
		kv.debug(topicService, "Succeeded GET(%v): '%v'", args.Key, reply.Value)
	} else {
		reply.Err = ErrNotCommitted
		kv.debug(topicService, "Failed GET(%v): %v", args.Key, reply.Err)
	}
}

// RPC Handler for PutAppend.
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here (4A).
	kv.lock.Lock()
	defer kv.lock.Unlock()

	kv.debug(topicInfo, "Starting %v(%v, '%v') for %v #%v", args.Op, args.Key, args.Value, args.ClerkId[0:5], args.SeqNum)

	// Check if we have already seen this exact request.
	if kv.hasProcessed(args.ClerkId, args.SeqNum) {
		reply.Err = OK
		kv.debug(
			topicService,
			"Succeeded %v(%v, '%v') (duplicate)",
			args.Op,
			args.Key,
			args.Value,
		)
		return
	}

	// Prepare a log entry for either a PUT or APPEND, then try to submit
	// that to our EPaxos instance
	cmd := Command{
		Op:      args.Op,
		Key:     args.Key,
		Value:   args.Value,
		ClerkId: args.ClerkId,
		SeqNum:  args.SeqNum,
	}

	logIndex := kv.Ep.Start(cmd)

	kv.debug(topicInfo, "Running %v(%v, '%v') for %v #%v", args.Op, args.Key, args.Value, args.ClerkId[0:5], args.SeqNum)

	// We sleep on this goroutine until the applyIndex has caught up.
	t0 := time.Now()
	for time.Since(t0) < 4*time.Second {
		if kv.killed() {
			return
		}

		if _, ok := kv.applied[logIndex]; ok {
			break
		}

		// Wait like 10ms between checking
		kv.lock.Unlock()
		time.Sleep(10 * time.Millisecond)
		kv.lock.Lock()
	}

	if kv.hasProcessed(args.ClerkId, args.SeqNum) {
		reply.Err = OK
		kv.debug(topicService, "Succeeded %v(%v, '%v')", args.Op, args.Key, args.Value)
	} else {
		reply.Err = ErrNotCommitted
		kv.debug(topicService, "Failed %v(%v, '%v'): %v", args.Op, args.Key, args.Value, reply.Err)
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.Ep.Kill()
	kv.debug(topicInfo, "Exiting...")
	disableLogging()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// Process EPaxos message as a command.
func (kv *KVServer) processCommand(msg *epaxos.Instance) {
	kv.lock.Lock()
	defer kv.lock.Unlock()

	if msg.Command == -1 {
		kv.debug(topicService, "No operation...")

		if msg.Position.Index > kv.applyIndex[msg.Position.Replica] {
			kv.debug(topicService, "Updating applyIndex[%v]: %v -> %v", msg.Position.Replica, kv.applyIndex[msg.Position.Replica], msg.Position.Index)
			kv.applyIndex[msg.Position.Replica] = msg.Position.Index
			kv.applied[msg.Position] = 1
		}

		return
	}

	cmd := msg.Command.(Command)

	// If we have already seen the sequence number, just skip it
	if kv.hasProcessed(cmd.ClerkId, cmd.SeqNum) {
		kv.debug(topicService, "Already applied %v(%v, '%v') %v #%v", cmd.Op, cmd.Key, cmd.Value, cmd.ClerkId[0:5], cmd.SeqNum)
		kv.debug(topicService, "Updating applyIndex[%v]: %v -> %v", msg.Position.Replica, kv.applyIndex[msg.Position.Replica], msg.Position.Index)
		if msg.Position.Index > kv.applyIndex[msg.Position.Replica] {
			kv.applyIndex[msg.Position.Replica] = msg.Position.Index
			kv.applied[msg.Position] = 1
		}
		return
	}

	kv.debug(topicService, "Applying %v(%v, '%v') %v #%v", cmd.Op, cmd.Key, cmd.Value, cmd.ClerkId[0:5], cmd.SeqNum)

	switch cmd.Op {
	case OpGet:
	case OpAppend:
		kv.kvs[cmd.Key] += cmd.Value
	case OpPut:
		kv.kvs[cmd.Key] = cmd.Value
	default:
		kv.assert(false, "Operation cannot be %v", cmd.Op)
	}

	if msg.Position.Index > kv.applyIndex[msg.Position.Replica] {
		kv.debug(topicService, "Updating applyIndex[%v]: %v -> %v", msg.Position.Replica, kv.applyIndex[msg.Position.Replica], msg.Position.Index)
		kv.applyIndex[msg.Position.Replica] = msg.Position.Index
		kv.duplicates[cmd.ClerkId] = SeqValue{SeqNum: cmd.SeqNum, Value: kv.kvs[cmd.Key]}
		kv.applied[msg.Position] = 1
	}
}

// A long-running goroutine that listens to the apply channel
// for messages to apply to the k/v store.
func (kv *KVServer) EPaxosListener() {
	for msg := range kv.applyCh {
		if kv.killed() {
			return
		}

		kv.processCommand(&msg)
	}
}

func (kv *KVServer) debug(topic topic, format string, a ...interface{}) {
	debug(topic, kv.me+1, format, a...)
}

func (kv *KVServer) assert(cond bool, format string, a ...interface{}) {
	assert(cond, kv.me+1, format, a...)
}

// Returns whether or not a request (identified by clerk id and sequence number) has already been processed.
func (kv *KVServer) hasProcessed(clerkId string, seqNum int) bool {
	if v, ok := kv.duplicates[clerkId]; ok {
		return v.SeqNum >= seqNum
	}

	return false
}

func interferes(cmd1, cmd2 interface{}) bool {
	if s1, ok1 := cmd1.(Command); ok1 {
		if s2, ok2 := cmd2.(Command); ok2 {
			return s1.Key == s2.Key
		}
	}

	return false
}

// servers[] contains the ports of the set of
// servers that will cooperate via EPaxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying EPaxos
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the EPaxos state along with the snapshot.
// the k/v server should snapshot when EPaxos's saved state exceeds maxEPaxosstate bytes,
// in order to allow EPaxos to garbage-collect its log. if maxEPaxosstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *epaxos.Persister) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	enableLogging()
	labgob.Register(Command{})

	kv := new(KVServer)
	kv.lock = sync.Mutex{}
	kv.me = me
	kv.dead = 0
	kv.persister = persister
	kv.applyCh = make(chan epaxos.Instance)
	kv.applyIndex = make([]int, len(servers))
	kv.applied = map[epaxos.LogIndex]int{}

	for i := 0; i < len(kv.applyIndex); i++ {
		kv.applyIndex[i] = -1
	}

	// You may need initialization code here (4A).
	kv.kvs = make(map[string]string)
	kv.duplicates = make(map[string]SeqValue)

	kv.Ep = epaxos.Make(servers, me, persister, kv.applyCh, interferes)
	kv.debug(topicService, "Starting with applyIndex: %v", kv.applyIndex)

	// Spawn goroutine to apply commands from EPaxos.
	go kv.EPaxosListener()

	return kv
}
