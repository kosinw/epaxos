package shardctrler

import (
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type CommandType string

const (
	TypeJoin  CommandType = "JOIN"
	TypeLeave CommandType = "LEAVE"
	TypeMove  CommandType = "MOVE"
	TypeQuery CommandType = "QUERY"
)

type Command struct {
	Type        CommandType      // type of operation, one of (JOIN, LEAVE, MOVE, QUERY)
	JoinServers map[int][]string // GID -> []servers mapping
	LeaveGIDs   []int            // list of groups to leave config
	MoveShard   int              // logical shard id
	MoveGID     int              // target group id
	QueryNum    int              // which configuration to query
	ClerkId     string           // id of clerk who submitted this command
	SeqNum      int              // sequence number from clerk
}

type SeqValue struct {
	SeqNum int
	Value  *Config
}

type ShardCtrler struct {
	lock    sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32

	// Your data here.
	configs    []Config            // indexed by config num
	duplicates map[string]SeqValue // client ID -> (seq, value) table for duplicate detection
	applyIndex int                 // log index of most recently applied operation
}

// Locks the coarse-grained lock for the shard controller.
func (sc *ShardCtrler) Lock() {
	sc.lock.Lock()
	sc.debug(topicLock, "--- Start critical section ---")
}

// Unlocks the coarse-graiend lock for the shard controller.
func (sc *ShardCtrler) Unlock() {
	sc.debug(topicLock, "--- End critical section ---")
	sc.lock.Unlock()
}

// Returns true if this server has been terminated.
func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// Outputs formatted debug information to stdout.
func (sc *ShardCtrler) debug(topic topic, format string, a ...interface{}) {
	if sc.killed() {
		return
	}

	debug(topic, sc.me, format, a...)
}

// If [cond] is not true, then panic with formatted message.
func (sc *ShardCtrler) assert(cond bool, format string, a ...interface{}) {
	assert(cond, sc.me, format, a...)
}

// Returns whether or not a request (identifier by clerkId and seqNum) has
// already been processed before (for duplicate detection).
func (sc *ShardCtrler) hasProcessed(clerkId string, seqNum int) bool {
	if v, ok := sc.duplicates[clerkId]; ok {
		return v.SeqNum >= seqNum
	}

	return false
}

// Updates applyIndex to new value
func (sc *ShardCtrler) updateApplyIndex(newIndex int) {
	sc.debug(topicService, "Updating applyIndex: %v -> %v", sc.applyIndex, newIndex)
	sc.applyIndex = newIndex
}

// Returns a pointer to the config given a config number.
// If config number == -1 or bigger than the largest known config
// return the latest configuration
func (sc *ShardCtrler) config(configNum int) *Config {
	if configNum == -1 || configNum >= len(sc.configs) {
		return &sc.configs[len(sc.configs)-1]
	}

	return &sc.configs[configNum]
}

// Dispatch handler for a JOIN command.
func (sc *ShardCtrler) join(clerkId string, seqNum int, servers map[int][]string) {
	oldConfig := sc.config(-1)
	newConfig := oldConfig.Clone()

	// Increment the config number
	newConfig.Num = oldConfig.Num + 1

	sc.debug(topicJoin, "Creating config %v", newConfig.Num)

	// Add a bunch of new servers to a new group.
	for gid, serverNames := range servers {
		// NOTE(kosinw): We should panic if the GID is in the current
		// configuration
		sc.debug(topicJoin, "Adding replica group (GID %v, servers %v)", gid, serverNames)

		if _, exists := oldConfig.Groups[gid]; exists {
			sc.assert(false, "Replica group (GID %v) already in cluster", gid)
		}

		newConfig.Groups[gid] = serverNames
	}

	// Rebalance shards.
	sc.debug(topicJoin, "Starting shard rebalancing: %v", newConfig.Shards)
	newConfig.rebalanceShards()
	sc.debug(topicJoin, "Finished shard rebalancing: %v", newConfig.Shards)

	sc.configs = append(sc.configs, newConfig)
	sc.duplicates[clerkId] = SeqValue{SeqNum: seqNum}
}

// Dispatch handler for a LEAVE command.
func (sc *ShardCtrler) leave(clerkId string, seqNum int, gids []int) {
	oldConfig := sc.config(-1)
	newConfig := oldConfig.Clone()

	// Increment the config number
	newConfig.Num = oldConfig.Num + 1

	sc.debug(topicLeave, "Creating config %v", newConfig.Num)

	// Remove a bunch of old groups.
	for _, gid := range gids {
		sc.debug(topicLeave, "Removing replica group (GID %v)", gid)

		if _, exists := oldConfig.Groups[gid]; !exists {
			sc.assert(false, "Removing replica group (GID %v) that does not exist", gid)
		}

		delete(newConfig.Groups, gid)
	}

	// Rebalance shards.
	sc.debug(topicLeave, "Starting shard rebalancing: %v", newConfig.Shards)
	newConfig.rebalanceShards()
	sc.debug(topicLeave, "Finished shard rebalancing: %v", newConfig.Shards)

	sc.configs = append(sc.configs, newConfig)
	sc.duplicates[clerkId] = SeqValue{SeqNum: seqNum}
}

// Dispatch handler for a MOVE command.
func (sc *ShardCtrler) move(clerkId string, seqNum int, gid int, shard int) {
	oldConfig := sc.config(-1)
	newConfig := oldConfig.Clone()

	// Increment the config number
	newConfig.Num = oldConfig.Num + 1

	sc.debug(topicMove, "Creating config %v", newConfig.Num)

	// Move shard to replica group.
	sc.assert(shard < NShards, "Invalid shard number: %v", shard)

	sc.debug(topicMove, "Moving shard %v from group %v -> %v", shard, oldConfig.Shards[shard], gid)

	newConfig.Shards[shard] = gid

	sc.configs = append(sc.configs, newConfig)
	sc.duplicates[clerkId] = SeqValue{SeqNum: seqNum}
}

func (sc *ShardCtrler) query(clerkId string, seqNum int, num int) {
	sc.debug(topicQuery, "Querying for config %v", num)

	sc.duplicates[clerkId] = SeqValue{
		SeqNum: seqNum,
		Value:  sc.config(num),
	}

	sc.debug(topicQuery, "%+v", *sc.duplicates[clerkId].Value)
}

// Processes a general command message sent by Raft.
func (sc *ShardCtrler) processCommand(msg *raft.ApplyMsg) {
	sc.Lock()
	defer sc.Unlock()

	cmd := msg.Command.(Command)

	// Update apply index if applicable.
	if msg.CommandIndex > sc.applyIndex {
		defer sc.updateApplyIndex(msg.CommandIndex)
	}

	// If we have already processed this command, we will skip it.
	if sc.hasProcessed(cmd.ClerkId, cmd.SeqNum) {
		sc.debug(topicService, "Already processed Command%+v", cmd)
		return
	}

	sc.debug(topicService, "Processing command for %v (%v | %v)", cmd.Type, cmd.ClerkId, cmd.SeqNum)

	// Dispatch state machine transition.
	switch cmd.Type {
	case TypeJoin:
		sc.join(cmd.ClerkId, cmd.SeqNum, cmd.JoinServers)
	case TypeLeave:
		sc.leave(cmd.ClerkId, cmd.SeqNum, cmd.LeaveGIDs)
	case TypeMove:
		sc.move(cmd.ClerkId, cmd.SeqNum, cmd.MoveGID, cmd.MoveShard)
	case TypeQuery:
		sc.query(cmd.ClerkId, cmd.SeqNum, cmd.QueryNum)
	default:
		sc.assert(false, "Unknown command type: %v", cmd.Type)
	}
}

// Starts agreement with other replicas over Raft.
// First return value is the error string, second value is false if this
// replica is the current leader.
func (sc *ShardCtrler) startRaftAgreement(cmd Command) (Err, bool) {
	sc.debug(topicService, "--- Starting RPC for %v (%v | %v) ---", cmd.Type, cmd.ClerkId, cmd.SeqNum)
	defer sc.debug(topicService, "--- Finishing RPC for %v (%v | %v) ---", cmd.Type, cmd.ClerkId, cmd.SeqNum)

	// Check if we have already seen this exact command (based on sequence number).
	// If so, just early return without submitting entry to Raft.
	if sc.hasProcessed(cmd.ClerkId, cmd.SeqNum) {
		sc.debug(topicService, "Duplicate for %v (%v | %v)", cmd.Type, cmd.ClerkId, cmd.SeqNum)
		return OK, false
	}

	// Start Raft agreement.
	logIndex, startTerm, isLeader := sc.rf.Start(cmd)

	// In this case, we are not the current leader so we cannot start the agreement.
	if !isLeader {
		sc.debug(topicService, "Wrong leader for %v (%v | %v)", cmd.Type, cmd.ClerkId, cmd.SeqNum)
		return ErrWrongLeader, true
	}

	// We need to wait for the applyIndex to catch up to the index where Raft told
	// us the entry would appear (if at all).
	for sc.applyIndex < logIndex {
		if sc.killed() {
			return ErrKilled, false
		}

		term, stillLeader := sc.rf.GetState()

		if term > startTerm || !stillLeader {
			sc.debug(topicService, "Wrong leader for %v (%v | %v)", cmd.Type, cmd.ClerkId, cmd.SeqNum)
			return ErrWrongLeader, true
		}

		sc.Unlock()
		time.Sleep(10 * time.Millisecond) // TODO(kosinw): Get this to work with sync.Cond
		sc.Lock()
	}

	// We need to check if we actually played the operation in our state machine.
	if !sc.hasProcessed(cmd.ClerkId, cmd.SeqNum) {
		sc.debug(topicService, "Command dropped: %v (%v | %v)", cmd.Type, cmd.ClerkId, cmd.SeqNum)
		return ErrNotCommitted, false
	}

	sc.debug(topicService, "OK for %v (%v | %v)", cmd.Type, cmd.ClerkId, cmd.SeqNum)

	return OK, false
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sc.Lock()
	defer sc.Unlock()

	cmd := Command{
		Type:        TypeJoin,
		JoinServers: args.Servers,
		ClerkId:     args.ClerkId,
		SeqNum:      args.SeqNum,
	}

	reply.Err, reply.WrongLeader = sc.startRaftAgreement(cmd)
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	sc.Lock()
	defer sc.Unlock()

	cmd := Command{
		Type:      TypeLeave,
		LeaveGIDs: args.GIDs,
		ClerkId:   args.ClerkId,
		SeqNum:    args.SeqNum,
	}

	reply.Err, reply.WrongLeader = sc.startRaftAgreement(cmd)
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	sc.Lock()
	defer sc.Unlock()

	cmd := Command{
		Type:      TypeMove,
		MoveShard: args.Shard,
		MoveGID:   args.GID,
		ClerkId:   args.ClerkId,
		SeqNum:    args.SeqNum,
	}

	reply.Err, reply.WrongLeader = sc.startRaftAgreement(cmd)
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	sc.Lock()
	defer sc.Unlock()

	cmd := Command{
		Type:     TypeQuery,
		QueryNum: args.Num,
		ClerkId:  args.ClerkId,
		SeqNum:   args.SeqNum,
	}

	reply.Err, reply.WrongLeader = sc.startRaftAgreement(cmd)

	if reply.Err == OK {
		reply.Config = sc.duplicates[args.ClerkId].Value.Clone()
	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.debug(topicService, "Exiting shard controller %v...", sc.me)
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// Long-running goroutine which listens for committed entries from Raft
// and then applies them to our state machine.
func (sc *ShardCtrler) Run() {
	for msg := range sc.applyCh {
		if sc.killed() {
			return
		}

		sc.assert(!msg.SnapshotValid, "Snapshots have not been implemented yet")
		sc.assert(msg.CommandValid, "Unknown message: %v", msg)

		sc.processCommand(&msg)
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	labgob.Register(Command{})

	sc := new(ShardCtrler)
	sc.lock = sync.Mutex{}
	sc.me = me
	sc.dead = 0
	sc.applyCh = make(chan raft.ApplyMsg)

	sc.configs = make([]Config, 1)
	sc.configs[0] = MakeConfig()
	sc.duplicates = map[string]SeqValue{}
	sc.applyIndex = 0

	sc.rf = raft.Make(servers, me, persister, sc.applyCh)
	sc.debug(topicService, "Starting shard controller %v...", me)

	// Your code here.
	go sc.Run()

	return sc
}
