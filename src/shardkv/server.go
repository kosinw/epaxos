package shardkv

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

// Amount of time to wait before checking for configuration changes
// from the shard controller.
const ConfigTimeout = 100 * time.Millisecond

type CommandType string

const (
	TypeGet     CommandType = "GET"
	TypePut     CommandType = "PUT"
	TypeAppend  CommandType = "APPEND"
	TypeConfig  CommandType = "CONFIG"
	TypeInstall CommandType = "INSTALL"
)

type Command struct {
	Type      CommandType         // type of operation, one of (GET, PUT, APPEND, CONFIG)
	Key       string              // key from k/v pair (for GET/PUT/APPEND operation)
	Value     string              // value from k/v pair (for PUT/APPEND operation)
	Config    *shardctrler.Config // config (for CONFIG operation)
	ConfigNum int                 // configuration number (for INSTALL operation)
	Shards    map[int]shard       // shard data (for INSTALL operation)
	Results   map[string]Result   // results (for INSTALL operation)
	ClerkId   string              // id of clerk who submitted this command
	SeqNum    int                 // sequence number from clerk
}

type Result struct {
	SeqNum int    // sequence number of client request
	Value  string // value from k/v service (for GET operation)
	Err    Err    // error
}

type ShardKV struct {
	// State for 3A, 3B
	lock         sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	dead         int32
	persister    *raft.Persister
	maxraftstate int // snapshot if log grows this big
	gid          int
	make_end     func(string) *labrpc.ClientEnd
	clerkId      string             // clerk id for this shard server
	seqNum       int64              // clerk sequence number for this shard server
	sc           *shardctrler.Clerk // clerk to communicate with shard controller

	// Persistent state
	conf       shardctrler.Config // current config
	data       ShardDict          // dictionary of all key/value data split up by shards
	results    map[string]Result  // client ID -> (seq, value) table for duplicate detection
	applyIndex int                // log index of most recently applied operation
	installing bool               // true if the current config has not install shards yet
}

// debug formats debugging output and writes it to standard out.
func (kv *ShardKV) debug(topic topic, format string, a ...interface{}) {
	if kv.killed() {
		return
	}

	// Just only log leader operations (for now)
	if term, leader := kv.rf.GetState(); term > 0 && !leader {
		return
	}

	debug(topic, kv.gid%10, format+fmt.Sprintf(" (gid %v)", kv.gid), a...)
}

// forceDebug formats debugging output and writes it to standard out. (does not check for leadership)
func (kv *ShardKV) forceDebug(topic topic, format string, a ...interface{}) {
	if kv.killed() {
		return
	}

	debug(topic, kv.gid%10, format+fmt.Sprintf(" (gid %v)", kv.gid), a...)
}

// seq atomically increments the sequence number
func (kv *ShardKV) seq() {
	atomic.AddInt64(&kv.seqNum, 1)
}

// configState returns the useful state for RunConfig
// In this case, whether or not we are the current Raft leader and also
// our current config number.
func (kv *ShardKV) configState() (leader bool, n int, installing bool) {
	kv.Lock()
	defer kv.Unlock()

	_, leader = kv.rf.GetState()
	n = kv.conf.Num
	installing = kv.installing

	return
}

// assert calls panic with formatted output if cond == false.
func (kv *ShardKV) assert(cond bool, format string, a ...interface{}) {
	assert(cond, kv.me, format+fmt.Sprintf(" (gid %v)", kv.gid), a...)
}

// Returns true, if [(*ShardKV).Kill] has been called.
func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// Returns whether or not a request (identifier by clerkId and seqNum) has
// already been processed before (for duplicate detection).
// If force is true, then we don't check for wrong group errors.
func (kv *ShardKV) hasProcessed(clerkId string, seqNum int, force bool) bool {
	if v, ok := kv.results[clerkId]; ok {
		// NOTE(kosinw): These can change over time
		if !force && v.Err == ErrWrongGroup {
			return false
		}

		return v.SeqNum == seqNum
	}

	return false
}

// Returns whether or not the log is currently large enough to make a snapshot.
func (kv *ShardKV) shouldSnapshot() bool {
	if kv.maxraftstate == -1 {
		return false
	}

	return kv.persister.RaftStateSize() >= kv.maxraftstate
}

// Writes snapshot of state machine into byte slice.
func (kv *ShardKV) snapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	if err := e.Encode(kv.conf); err != nil {
		panic(err)
	}

	if err := e.Encode(kv.data); err != nil {
		panic(err)
	}

	if err := e.Encode(kv.results); err != nil {
		panic(err)
	}

	if err := e.Encode(kv.applyIndex); err != nil {
		panic(err)
	}

	if err := e.Encode(kv.installing); err != nil {
		panic(err)
	}

	snapshot := w.Bytes()

	kv.debug(topicSnap, "Creating snapshot, index: %v", kv.applyIndex)

	return snapshot
}

// Restores state machine from prior snapshot.
func (kv *ShardKV) readSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) == 0 {
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var conf shardctrler.Config
	var data ShardDict
	var results map[string]Result
	var applyIndex int
	var installing bool

	if err := d.Decode(&conf); err != nil {
		panic(err)
	}

	if err := d.Decode(&data); err != nil {
		panic(err)
	}

	if err := d.Decode(&results); err != nil {
		panic(err)
	}

	if err := d.Decode(&applyIndex); err != nil {
		panic(err)
	}

	if err := d.Decode(&installing); err != nil {
		panic(err)
	}

	kv.debug(topicSnap, "Reading snapshot, index: %v", applyIndex)

	kv.conf = conf
	kv.data = data
	kv.results = results
	kv.applyIndex = applyIndex
	kv.installing = installing
}

// Updates applyIndex to new value.
func (kv *ShardKV) updateApplyIndex(newIndex int) {
	kv.debug(topicService, "Updating applyIndex: %v -> %v", kv.applyIndex, newIndex)
	kv.applyIndex = newIndex
}

// Dispatch handler for a GET command.
func (kv *ShardKV) get(seqNum int, key string) (result Result) {
	result.SeqNum = seqNum
	result.Err = OK

	kv.debug(topicGet, "Key \"%v\" in shard %v", key, key2shard(key))

	if !kv.data.KeyInstalled(key) {
		kv.debug(topicGet, "Key \"%v\" not installed (shard %v)", key, key2shard(key))
		result.Err = ErrWrongGroup
		return
	}

	if !kv.data.Contains(key) {
		result.Err = ErrNoKey
		return
	}

	result.Value = kv.data.Get(key)
	kv.debug(topicGet, "\"%v\": \"%v\"", key, result.Value)

	return
}

// Dispatch handler for a PUT command.
func (kv *ShardKV) put(seqNum int, key string, value string) (result Result) {
	result.SeqNum = seqNum
	result.Err = OK

	kv.debug(topicPut, "Key \"%v\" in shard %v", key, key2shard(key))

	if !kv.data.KeyInstalled(key) {
		kv.debug(topicPut, "Key \"%v\" not installed (shard %v)", key, key2shard(key))
		result.Err = ErrWrongGroup
		return
	}

	kv.debug(topicPut, "\"%v\": \"%v\"", key, value)
	kv.data.Put(key, value)

	return
}

// Dispatch handler for an APPEND command.
func (kv *ShardKV) append(seqNum int, key string, value string) (result Result) {
	result.SeqNum = seqNum
	result.Err = OK

	kv.debug(topicAppend, "Key \"%v\" in shard %v", key, key2shard(key))

	if !kv.data.KeyInstalled(key) {
		kv.debug(topicAppend, "Key \"%v\" not installed (shard %v)", key, key2shard(key))
		result.Err = ErrWrongGroup
		return
	}

	kv.debug(topicAppend, "\"%v\": \"%v\"", key, value)
	kv.data.Append(key, value)

	return
}

// Dispatch handler for a CONFIG command.
func (kv *ShardKV) config(seqNum int, config *shardctrler.Config) (result Result) {
	result.SeqNum = seqNum
	result.Err = OK

	// NOTE(kosinw): Maybe we should stop upgrades, if we are still installing the current config?
	if kv.installing || kv.conf.Num >= config.Num {
		kv.debug(topicConfig, "Failed to upgrade from config %v -> %v", kv.conf.Num, config.Num)
		result.Err = ErrWrongConfig
		return
	}

	kv.debug(topicConfig, "Upgrading from config %v -> %v", kv.conf.Num, config.Num)

	kv.debug(topicConfig, "old: %v", kv.conf.Shards)
	kv.debug(topicConfig, "new: %v", config.Shards)

	// Remove all shards we no longer own.
	for sid, owns := range kv.data.Owns {
		// We are acquiring a shard that was previously unowned.
		if kv.conf.Shards[sid] == 0 && config.Shards[sid] == kv.gid {
			kv.debug(topicConfig, "Now serving shard %v", sid)
			kv.data.AddShard(sid)
		}

		// We are removing shards that we no longer currently own
		if owns && config.Shards[sid] != kv.gid {
			kv.debug(topicConfig, "No longer serving shard %v", sid)
			kv.data.RemoveShard(sid)
		}

		// We are acquiring are shard that was previously owned.
		if kv.conf.Shards[sid] != 0 && kv.conf.Shards[sid] != kv.gid && config.Shards[sid] == kv.gid {
			if !kv.installing {
				kv.debug(topicConfig, "Updating installing: %v -> %v", false, true)
			}

			kv.installing = true
		}
	}

	kv.conf = config.Clone()

	return
}

// Dispatch handler for an INSTALL command.
func (kv *ShardKV) install(seqNum int, configNum int, shards map[int]shard, results map[string]Result) (result Result) {
	result.SeqNum = seqNum
	result.Err = OK

	if configNum != kv.conf.Num {
		kv.debug(topicInstall, "Failed to install for config %v (current config %v)",
			configNum, kv.conf.Num)
		result.Err = ErrWrongConfig
		return
	}

	// We have already installed before
	if !kv.installing {
		kv.debug(topicInstall, "Duplicate install, skipping")
		return
	}

	kv.installing = false

	kv.debug(topicInstall, "Installing %v shards for config %v", len(shards), configNum)

	// Install shards that we do not have
	for sid, data := range shards {
		kv.assert(!kv.data.Owns[sid], "Should not be installing shard we already have (shard %v)", sid)
		kv.debug(topicInstall, "Installing shard %v with data %v", sid, data)
		kv.data.AddShard(sid)
		for k, v := range data {
			kv.data.Put(k, v)
		}
	}

	// For sanity check, assert that we now match the config
	for sid, gid := range kv.conf.Shards {
		if kv.gid != gid {
			kv.assert(!kv.data.Owns[sid],
				"For shard ownership %v: expected %v, instead %v",
				sid, !kv.data.Owns[sid], kv.data.Owns[sid])
		} else {
			kv.assert(kv.data.Owns[sid],
				"For shard ownership %v: expected %v, instead %v",
				sid, !kv.data.Owns[sid], kv.data.Owns[sid])
		}
	}

	// Now install all the results as well
	for clerkId, theirResult := range results {
		if ourResult, exists := kv.results[clerkId]; !exists || ourResult.SeqNum <= theirResult.SeqNum {
			kv.results[clerkId] = theirResult
		}
	}

	return
}

// Processes a general command message sent by Raft.
func (kv *ShardKV) processCommand(msg *raft.ApplyMsg) {
	kv.Lock()
	defer kv.Unlock()

	cmd := msg.Command.(Command)

	// Update apply index if applicable.
	if msg.CommandIndex > kv.applyIndex {
		kv.updateApplyIndex(msg.CommandIndex)
	}

	// If we have already processed this command, we will skip it.
	if kv.hasProcessed(cmd.ClerkId, cmd.SeqNum, false) {
		kv.debug(topicService, "Already processed %v %v %v", cmd.Type, cmd.ClerkId, cmd.SeqNum)
		return
	}

	kv.debug(topicService, "%v %v %v", cmd.Type, cmd.ClerkId, cmd.SeqNum)

	// Dispatch state machine transition.
	switch cmd.Type {
	case TypeGet:
		kv.results[cmd.ClerkId] = kv.get(cmd.SeqNum, cmd.Key)
	case TypePut:
		kv.results[cmd.ClerkId] = kv.put(cmd.SeqNum, cmd.Key, cmd.Value)
	case TypeAppend:
		kv.results[cmd.ClerkId] = kv.append(cmd.SeqNum, cmd.Key, cmd.Value)
	case TypeConfig:
		kv.results[cmd.ClerkId] = kv.config(cmd.SeqNum, cmd.Config)
	case TypeInstall:
		kv.results[cmd.ClerkId] = kv.install(cmd.SeqNum, cmd.ConfigNum, cmd.Shards, cmd.Results)
	default:
		kv.assert(false, "Unknown command type: %v", cmd.Type)
	}

	// We should snapshot if necessary.
	if kv.shouldSnapshot() {
		kv.rf.Snapshot(kv.applyIndex, kv.snapshot())
	}
}

// Processes a snapshot message sent by Raft.
func (kv *ShardKV) processSnapshot(msg *raft.ApplyMsg) {
	kv.Lock()
	defer kv.Unlock()

	// Update apply index if applicable.
	if msg.SnapshotIndex > kv.applyIndex {
		kv.readSnapshot(msg.Snapshot)
		kv.updateApplyIndex(msg.SnapshotIndex)
	}
}

// Starts agreement with other replicas over Raft.
// First return value is the error string, second value is false if this
// replica is the current leader.
// Acquires global struct lock.
func (kv *ShardKV) agree(cmd Command) (result Result) {
	kv.Lock()
	defer kv.Unlock()

	kv.debug(topicClient, "Starting %v %v %v", cmd.Type, cmd.ClerkId, cmd.SeqNum)

	// Check if we have already seen this exact command (based on sequence number).
	// If so, just early return without submitting entry to Raft.
	if kv.hasProcessed(cmd.ClerkId, cmd.SeqNum, false) {
		result = kv.results[cmd.ClerkId]
		kv.debug(topicClient, "%v for %v %v %v (duplicate)", result.Err, cmd.Type, cmd.ClerkId, cmd.SeqNum)
		return
	}

	// Start Raft agreement.
	logIndex, startTerm, isLeader := kv.rf.Start(cmd)

	// In this case, we are not the current leader so we cannot start the agreement.
	if !isLeader {
		kv.debug(topicClient, "Wrong leader for %v %v %v", cmd.Type, cmd.ClerkId, cmd.SeqNum)
		result.Err = ErrWrongLeader
		return
	}

	// We need to wait for the applyIndex to catch up to the index where Raft told
	// us the entry would appear (if at all).
	for kv.applyIndex < logIndex {
		if kv.killed() {
			result.Err = ErrKilled
			return
		}

		term, stillLeader := kv.rf.GetState()

		if term > startTerm || !stillLeader {
			kv.debug(topicClient, "Wrong leader for %v %v %v", cmd.Type, cmd.ClerkId, cmd.SeqNum)
			result.Err = ErrWrongLeader
			return
		}

		kv.Unlock()
		time.Sleep(10 * time.Millisecond)
		kv.Lock()
	}

	// We need to check if we actually played the operation in our state machine.
	if !kv.hasProcessed(cmd.ClerkId, cmd.SeqNum, true) {
		kv.debug(topicClient, "Command dropped for %v %v %v", cmd.Type, cmd.ClerkId, cmd.SeqNum)
		result.Err = ErrNotCommitted
		return
	}

	result = kv.results[cmd.ClerkId]
	kv.debug(topicClient, "%v for %v %v %v", result.Err, cmd.Type, cmd.ClerkId, cmd.SeqNum)
	return
}

// Downloads shard [sid] from a particular replica group [gid], returns
// when successful or timeout has occurred.
func (kv *ShardKV) downloadShard(sid int, servers []string, config int) (ok bool, shard shard, results map[string]Result) {
	const smallTimeout = 10 * time.Millisecond
	const largeTimeout = 500 * time.Millisecond

	type channelResult = struct {
		shard   map[string]string
		results map[string]Result
	}

	ch := make(chan channelResult, 1)

	go func() {
		var args MigrateShardArgs

		args.ConfigNum = config
		args.ShardNum = sid
		args.GroupId = kv.gid

		for {
			for _, server := range servers {
				var reply MigrateShardReply

				srv := kv.make_end(server)
				ok := srv.Call("ShardKV.MigrateShard", &args, &reply)

				if ok && reply.Err == OK {
					shard := reply.Shard
					results := reply.Results
					ch <- channelResult{shard, results}
					return
				}
			}

			time.Sleep(smallTimeout)
		}
	}()

	select {
	case r := <-ch:
		ok = true
		shard = r.shard
		results = r.results
		return
	case <-time.After(largeTimeout):
		ok = false
		return
	}
}

// Downloads missing shards from the current configuration from the shard owners
// described in configuration [configNum].
func (kv *ShardKV) downloadShards(configNum int) (ok bool, shards map[int]shard, results map[string]Result) {
	ok = true
	shards = map[int]shard{}
	results = map[string]Result{}

	lock := new(sync.Mutex)
	wg := sync.WaitGroup{}
	prev := kv.sc.Query(configNum - 1)
	curr := kv.sc.Query(configNum)

	// Every shard we currently do not have, put into a map of sid -> gid
	shardToGroup := map[int]int{}

	for sid, gid := range curr.Shards {
		if gid != kv.gid {
			continue
		}

		if prev.Shards[sid] != 0 && gid != prev.Shards[sid] {
			shardToGroup[sid] = prev.Shards[sid]
		}
	}

	// Download shard [sid] from group [gid] and merge the results into a shared
	// map of shards and shared map of results.
	for sid, gid := range shardToGroup {
		wg.Add(1)

		go func(sid int, gid int) {
			defer wg.Done()

			kv.debug(topic("TRACE"), "Asking gid %v for shard %v", gid, sid)
			success, shard, res := kv.downloadShard(sid, prev.Groups[gid], configNum)

			lock.Lock()
			defer lock.Unlock()

			if !success {
				ok = false
				return
			}

			shards[sid] = map[string]string{}

			for k, v := range shard {
				shards[sid][k] = v
			}

			for k, v := range res {
				if s, e := results[k]; !e || s.SeqNum <= v.SeqNum {
					results[k] = v
				}
			}
		}(sid, gid)
	}

	wg.Wait()

	return
}

// Long-running goroutine which listens for committed entries from Raft
// and then applies them to our state machine.
func (kv *ShardKV) RunStateMachine() {
	for msg := range kv.applyCh {
		if kv.killed() {
			return
		}

		if msg.CommandValid {
			kv.processCommand(&msg)
		} else if msg.SnapshotValid {
			kv.processSnapshot(&msg)
		}
	}
}

// Long-running goroutine which listens for reconfiguration changes from
// the shard controller. On configuration changes, start a Raft agreement
// for a Config request.
func (kv *ShardKV) RunConfig() {
	for {
		time.Sleep(ConfigTimeout)

		// Abort this loop if this server was killed.
		if kv.killed() {
			return
		}

		leader, n, installing := kv.configState()

		// Go to the next iteration if we are not the leader or we are still installing.
		if !leader {
			continue
		}

		if installing {
			kv.RunInstall(n)
			continue
		}

		// Get the next immediate configuration from our current configuration.
		config := kv.sc.Query(n + 1)

		// This mean that the next immediate configuration number is not available.
		if config.Num != n+1 {
			continue
		}

		// Now, we try to add the command to our log. It can fail for a bunch of reasons
		// including: not leader, stale config, etc.
		cmd := Command{
			Type:    TypeConfig,
			Config:  &config,
			ClerkId: kv.clerkId,
			SeqNum:  int(kv.seqNum),
		}
		kv.seq()

		// Now debug log if command actually goes through agreement.
		result := kv.agree(cmd)

		if result.Err == OK {
			kv.RunInstall(n + 1)
		}
	}
}

// Installs shards by making RPCs to other servers.
func (kv *ShardKV) RunInstall(n int) {
	// Abort this loop if this server was killed.
	if kv.killed() {
		return
	}

	leader, _, installing := kv.configState()

	if !installing || !leader {
		return
	}

	// Download all shards we are missing from config n.
	ok, shards, results := kv.downloadShards(n)

	// We timed out on some RPCs, so just forget about it
	if !ok {
		return
	}

	// Now, we try to add the command to our log.
	cmd := Command{
		Type:      TypeInstall,
		ConfigNum: n,
		Shards:    shards,
		Results:   results,
		ClerkId:   kv.clerkId,
		SeqNum:    int(kv.seqNum),
	}
	kv.seq()

	kv.agree(cmd)
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.forceDebug(topicService, "Exiting replica %v...", kv.me)
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

// Locks the coarse-grained lock for the shard controller.
func (kv *ShardKV) Lock() {
	kv.lock.Lock()
	kv.debug(topicLock, "Acquiring lock...")
}

// Unlocks the coarse-graiend lock for the shard controller.
func (kv *ShardKV) Unlock() {
	kv.debug(topicLock, "Releasing lock...")
	kv.lock.Unlock()
}

// Remote procedure call handler for Get.
// If successful, tells the sharded key/value replica to start a Raft
// agreement for a Get request from the key/value service.
func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	cmd := Command{
		Type:    TypeGet,
		Key:     args.Key,
		ClerkId: args.ClerkId,
		SeqNum:  args.SeqNum,
	}

	result := kv.agree(cmd)

	reply.Err = result.Err
	reply.Value = result.Value
}

// Remote procedure call handler for PutAppend.
// If successful, tells the sharded key/value replica to start a Raft
// agreement for either a Put or Append request from the key/value service.
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	cmd := Command{
		Type:    CommandType(args.Type),
		Key:     args.Key,
		Value:   args.Value,
		ClerkId: args.ClerkId,
		SeqNum:  args.SeqNum,
	}

	result := kv.agree(cmd)
	reply.Err = result.Err
}

// Remote procedure call handler for MigrateShard.
// If successful, tells the sharded key/value replica to reply
// with the requested shard.
func (kv *ShardKV) MigrateShard(args *MigrateShardArgs, reply *MigrateShardReply) {
	kv.Lock()
	defer kv.Unlock()

	reply.Shard = shard{}
	reply.Results = map[string]Result{}

	if kv.conf.Num < args.ConfigNum {
		kv.debug(topicInstall, "Not migrating: gid %v for S%v, C%v", args.GroupId, args.ShardNum, args.ConfigNum)
		reply.Err = ErrWrongConfig
		return
	}

	kv.forceDebug(topicInstall, "Migrating: gid %v for S%v, C%v", args.GroupId, args.ShardNum, args.ConfigNum)
	for k, v := range kv.results {
		// NOTE(kosinw): We don't want to transfer over results that says its a wrong group.
		if v.Err != ErrWrongGroup {
			reply.Results[k] = v
		}
	}

	for k, v := range kv.data.Data[args.ShardNum] {
		reply.Shard[k] = v
	}
	reply.Err = OK

	return
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	kv := new(ShardKV)
	kv.lock = sync.Mutex{}
	kv.me = me
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.dead = 0
	kv.persister = persister
	kv.gid = gid
	kv.make_end = make_end
	kv.sc = shardctrler.MakeClerk(ctrlers)
	kv.clerkId = id()
	kv.seqNum = 1
	kv.maxraftstate = maxraftstate

	kv.conf = shardctrler.MakeConfig()
	kv.data = MakeShardDict()
	kv.results = map[string]Result{}
	kv.applyIndex = 0
	kv.installing = false

	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.forceDebug(topicService, "Starting replica %v (AI: %v)...", kv.me, kv.applyIndex)

	// Read in persistent state from a snapshot.
	kv.readSnapshot(persister.ReadSnapshot())

	// Spawn goroutine to listen for committed log entries from Raft.
	go kv.RunStateMachine()

	// Spawn goroutine to listen for configuration changes from shard controller.
	go kv.RunConfig()

	return kv
}
