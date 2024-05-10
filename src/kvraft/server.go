package kvraft

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
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
	lock      sync.Mutex
	me        int
	Rf        *raft.Raft
	applyCh   chan raft.ApplyMsg
	dead      int32           // set by Kill()
	persister *raft.Persister // Object to hold this server's persisted state

	maxraftstate int // snapshot if log grows this big

	kvs        map[string]string   // k/v store which holds data for the service
	duplicates map[string]SeqValue // client ID -> (seq, value) table for duplicate detection
	applyIndex int                 // log index of most recently applied operation
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
	// that to our Raft instance.
	cmd := Command{Op: OpGet, Key: args.Key, ClerkId: args.ClerkId, SeqNum: args.SeqNum}

	logIndex, startTerm, isLeader := kv.Rf.Start(cmd)

	// If we are not the leader just respond to the RPC already
	// with a WrongLeader error.
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.debug(topicInfo, "Failed GET(%v): wrong leader", args.Key)
		return
	}

	kv.debug(topicInfo, "Running GET(%v) as leader for %v #%v", args.Key, args.ClerkId[0:5], args.SeqNum)

	// We sleep on this goroutine until the applyIndex has not
	// yet reached this operation.
	for kv.applyIndex < logIndex {
		if kv.killed() { // this server has been killed
			return
		}

		// Have we been deposed?
		term, stillLeader := kv.Rf.GetState()

		if term > startTerm || !stillLeader {
			reply.Err = ErrWrongLeader
			kv.debug(topicInfo, "Failed GET(%v): deposed as leader", args.Key)
			return
		}

		// kv.cond.Wait()

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
	// that to our raft instance
	cmd := Command{
		Op:      args.Op,
		Key:     args.Key,
		Value:   args.Value,
		ClerkId: args.ClerkId,
		SeqNum:  args.SeqNum,
	}

	logIndex, startTerm, isLeader := kv.Rf.Start(cmd)

	// If we are not the leader just respond to the RPC already
	// with a WrongLeader error.
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.debug(topicInfo, "Failed %v(%v, '%v'): wrong leader", args.Op, args.Key, args.Value)
		return
	}

	kv.debug(topicInfo, "Running %v(%v, '%v') as leader for %v #%v", args.Op, args.Key, args.Value, args.ClerkId[0:5], args.SeqNum)

	// We sleep on this goroutine until the applyIndex has caught up.
	for kv.applyIndex < logIndex {
		if kv.killed() {
			return
		}

		// Have we been deposed?
		term, stillLeader := kv.Rf.GetState()

		if term > startTerm || !stillLeader {
			reply.Err = ErrWrongLeader
			kv.debug(topicInfo, "Failed %v(%v, '%v'): deposed as leader", args.Op, args.Key, args.Value)
			return
		}

		// kv.cond.Wait()

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
	kv.Rf.Kill()
	kv.debug(topicInfo, "Exiting...")
	disableLogging()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// Process Raft message as a command.
func (kv *KVServer) processCommand(msg *raft.ApplyMsg) {
	kv.lock.Lock()
	defer kv.lock.Unlock()

	kv.assert(msg.CommandValid, "Called processCommand() when not command message")

	cmd := msg.Command.(Command)

	// If we have already seen the sequence number, just skip it
	if kv.hasProcessed(cmd.ClerkId, cmd.SeqNum) {
		kv.debug(topicService, "Already applied %v(%v, '%v') %v #%v", cmd.Op, cmd.Key, cmd.Value, cmd.ClerkId[0:5], cmd.SeqNum)
		kv.debug(topicService, "Updating applyIndex %v -> %v", kv.applyIndex, msg.CommandIndex)
		if msg.CommandIndex > kv.applyIndex {
			kv.applyIndex = msg.CommandIndex
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

	if msg.CommandIndex > kv.applyIndex {
		kv.debug(topicService, "Updating applyIndex %v -> %v", kv.applyIndex, msg.CommandIndex)
		kv.applyIndex = msg.CommandIndex
		kv.duplicates[cmd.ClerkId] = SeqValue{SeqNum: cmd.SeqNum, Value: kv.kvs[cmd.Key]}

		// NOTE(kosinw): At this point we should check if our log is too large
		// and if so request for a snapshot.
		if kv.shouldSnapshot() {
			kv.debug(topicSnap,
				"Starting snapshot, size: %v, index: %v",
				kv.persister.RaftStateSize(),
				msg.CommandIndex,
			)

			// Marshall state into snapshot.
			snapshot := kv.marshall()

			// Send snapshot to Raft.
			kv.Rf.Snapshot(msg.CommandIndex, snapshot)
		}
	}
}

// Process Raft message as a snapshot.
func (kv *KVServer) processSnapshot(msg *raft.ApplyMsg) {
	kv.lock.Lock()
	defer kv.lock.Unlock()

	kv.assert(msg.SnapshotValid, "Called processSnapshot() when not snapshot message")

	// Only update snapshot if its newer than current state.
	if msg.SnapshotIndex > kv.applyIndex {
		kv.unmarshall(msg.Snapshot)
		kv.debug(topicService, "Updating applyIndex %v -> %v", kv.applyIndex, msg.SnapshotIndex)
		kv.applyIndex = msg.SnapshotIndex
	} else {
		kv.debug(
			topicSnap,
			"Not applying snapshot, too old, AI (%v) > SI (%v)",
			kv.applyIndex,
			msg.SnapshotIndex,
		)
	}
}

// A long-running goroutine that listens to the apply channel
// for messages to apply to the k/v store.
func (kv *KVServer) raftListener() {
	for msg := range kv.applyCh {
		if kv.killed() {
			return
		}

		if msg.SnapshotValid {
			kv.processSnapshot(&msg)
		} else if msg.CommandValid {
			kv.processCommand(&msg)
		} else {
			kv.assert(false, "Unknown message: %v", msg)
		}
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

// Returns whether or not the log is currently large enough to make a snapshot.
func (kv *KVServer) shouldSnapshot() bool {
	if kv.maxraftstate == -1 {
		return false
	}

	return kv.persister.RaftStateSize() >= kv.maxraftstate
}

// Marshall persistent state into raw bytes so it can be persisted
// into stable storage.
func (kv *KVServer) marshall() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	if err := e.Encode(kv.kvs); err != nil {
		panic(err)
	}

	if err := e.Encode(kv.duplicates); err != nil {
		panic(err)
	}

	if err := e.Encode(kv.applyIndex); err != nil {
		panic(err)
	}

	snapshot := w.Bytes()

	kv.debug(topicSnap, "Write snapshot, K: %v, D: %v", kv.kvs, kv.duplicates)

	return snapshot
}

// Unmarshall persistent state from prior snapshot.
func (kv *KVServer) unmarshall(snapshot []byte) {
	if snapshot == nil || len(snapshot) == 0 {
		return // we had no snapshotted state
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var kvs map[string]string
	var duplicates map[string]SeqValue
	var applyIndex int

	if err := d.Decode(&kvs); err != nil {
		panic(err)
	}

	if err := d.Decode(&duplicates); err != nil {
		panic(err)
	}

	if err := d.Decode(&applyIndex); err != nil {
		panic(err)
	}

	kv.debug(topicSnap, "Read snapshot, AI: %v", applyIndex)

	kv.kvs = kvs
	kv.duplicates = duplicates
	kv.applyIndex = applyIndex
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	enableLogging()
	labgob.Register(Command{})

	kv := new(KVServer)
	kv.lock = sync.Mutex{}
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.dead = 0
	kv.persister = persister
	kv.applyCh = make(chan raft.ApplyMsg)

	// You may need initialization code here (4A).
	kv.kvs = make(map[string]string)
	kv.duplicates = make(map[string]SeqValue)

	// Unmarshall state from a snapshot (4B).
	kv.unmarshall(persister.ReadSnapshot())

	kv.Rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.debug(topicService, "Starting with AI: %v", kv.applyIndex)

	// Spawn goroutine to apply commands from Raft.
	go kv.raftListener()

	return kv
}
