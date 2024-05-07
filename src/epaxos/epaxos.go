package epaxos

// This is an outline of the API that EPaxos must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// e = Make(...)
//   create a new EPaxos server.
// e.Start(command interface{}) LogIndex
//   start agreement on a new log entry
// Instance
//   each time a new entry is committed to the log, each EPaxos peer
//   should send an Instance to the service (or tester) in the same server.

import (
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
)

// A Go type representing the persistent log.
type PaxosLog [][]Instance

func (e *EPaxos) makeLog() PaxosLog {
	log := make([][]Instance, e.numPeers())
	for i := range log {
		log[i] = make([]Instance, 0)
	}
	return log
}

// debug is used to do output debug information
func (e *EPaxos) debug(topic topic, format string, a ...interface{}) {
	if e.killed() {
		return
	}

	debug(topic, e.me, format, a...)
}

func (e *EPaxos) Lock() {
	e.lock.Lock()
	e.debug(topicLock, "Acquiring lock...")
}

func (e *EPaxos) Unlock() {
	e.debug(topicLock, "Releasing lock...")
	e.lock.Unlock()
}

// A Go object implementing a single EPaxos peer.
type EPaxos struct {
	lock        sync.Mutex                        // lock to protect shared access to this peer's state
	peers       []*labrpc.ClientEnd               // RPC end points of all peers
	persister   *Persister                        // Object to hold this peer's persisted state
	me          int                               // this peer's index into peers[]
	dead        int32                             // set by Kill()
	applyCh     chan<- Instance                   // channel used to send client messages
	interferes  func(cmd1, cmd2 interface{}) bool // function passed in by client that checks whether two commands interfere
	log         PaxosLog                          // log across all replicas; nested array indexed by replica number, instance number
	nextIndex   int                               // index of next instance to be added to this replica
	lastApplied []int                             // index of highest log entry known to be applied to state machine
}

func (e *EPaxos) numPeers() int {
	return len(e.peers)
}

// # replicas in fast path quorum
func (e *EPaxos) numFastPath() int {
	return e.numPeers() - 1
}

// checks if two maps are equal
func mapsEqual(map1, map2 map[LogIndex]int) bool {
	if len(map1) != len(map2) {
		return false
	}
	for key := range map1 {
		if _, ok := map2[key]; !ok {
			return false
		}
	}
	return true
}

// takes the union of two maps
func unionMaps(map1, map2 map[LogIndex]int) map[LogIndex]int {
	unionMap := make(map[LogIndex]int)
	for key, value := range map1 {
		unionMap[key] = value
	}
	for key, value := range map2 {
		unionMap[key] = value
	}
	return unionMap
}

// attributes calculates the sequence number and dependencies for a command at current instance number.
func (e *EPaxos) attributes(cmd interface{}, ix LogIndex) (seq int, deps map[LogIndex]int) {
	// find seq num & deps
	// assuming every instance depends on the instance before it within a replica
	maxSeq := 0
	deps = map[LogIndex]int{}

	// loop through all instances in replica L's 2D log
	for r, replica := range e.log {
		if r == ix.Replica {
			continue
		}

		for i := len(replica) - 1; i >= 0; i-- { // loop through each replica backwards
			instance := replica[i]

			if instance.Valid && e.interferes(cmd, instance.Command) {
				deps[LogIndex{Replica: r, Index: i}] = 1
				maxSeq = max(maxSeq, instance.Seq)
				break // only find the latest instance that interferes
			}
		}
	}

	if ix.Index > 0 {
		deps[LogIndex{Replica: ix.Replica, Index: ix.Index - 1}] = 1
	}

	// seq is larger than seq of all interfering commands in deps
	seq = maxSeq + 1

	// e.debug(topicInfo, "%v: deps: %v", ix, deps)
	// e.debug(topicInfo, "%v: seq: %v", ix, seq)

	return
}

// getInstance returns a pointer to an instance in our log given an index.
func (e *EPaxos) getInstance(index LogIndex) *Instance {
	// extend this replica, then append to this replica's logs
	for len(e.log[index.Replica]) <= index.Index {
		e.log[index.Replica] = append(e.log[index.Replica], Instance{Valid: false,
			Timer:    time.Now().Add(time.Duration(rand.Intn(300)) * time.Millisecond),
			Position: LogIndex{Replica: index.Replica, Index: len(e.log[index.Replica])}})
	}

	return &e.log[index.Replica][index.Index]
}

// preAcceptPhase executes the pre-accept phase for the EPaxos commit protocol.
func (e *EPaxos) preAcceptPhase(cmd interface{}, ix LogIndex, nofast bool) (commit bool, abort bool) {
	e.Lock()

	abort = false
	commit = false

	//assert(ix.Replica == e.me, e.me, "Log index %v should have replica %v", ix, replicaName(e.me))

	e.debug(topicPreAccept, "Starting pre-accept phase for instance %v...", ix)

	// Print out how phase 1 ended
	defer func() {
		e.debug(
			topicPreAccept,
			"Ending pre-accept phase for instance %v... (commit=%v abort=%v)",
			ix,
			commit,
			abort,
		)
	}()

	// calculate seq and deps
	seq, deps := e.attributes(cmd, ix)

	// put cmd into our log
	instance := e.getInstance(ix)
	prevBallot := instance.Ballot
	*instance = Instance{
		Deps:     deps,
		Seq:      seq,
		Command:  cmd,
		Position: ix,
		Status:   PREACCEPTED,
		Ballot:   Ballot{BallotNum: 0, ReplicaNum: ix.Replica},
		Valid:    true,
		Timer:    time.Time{},
	}
	if nofast {
		instance.Ballot = prevBallot
	}
	e.persist()

	instanceCopy := *instance

	e.Unlock()

	// broadcast and wait for pre-accept messages to our other peers
	updatedSeq, updatedDeps, abort := e.broadcastPreAccept(instanceCopy, nofast)

	e.Lock()
	defer e.Unlock()

	// If we have to abort just end this phase without moving onto the next one
	if abort {
		instance := e.getInstance(ix)
		instance.Timer = time.Now()
		commit = false
		return
	} else {
		instance := e.getInstance(ix)
		instance.Deps = updatedDeps
		instance.Seq = updatedSeq
		e.persist()
		e.debug(topicLog, "Updated %v: %v", ix, *instance)
	}

	// only commit if nothing changed
	commit = (seq == updatedSeq) && (mapsEqual(deps, updatedDeps))

	return
}

// acceptPhase executes the Paxos-Accept phase for the EPaxos commit protocol.
func (e *EPaxos) acceptPhase(pos LogIndex) (abort bool) {
	e.Lock()
	abort = false

	instance := e.getInstance(pos)

	// pre-condition: the instance must be valid
	assert(
		instance.Valid,
		e.me,
		"Expected %v to be valid",
		instance.Position,
	)

	defer func() {
		e.debug(
			topicAccept,
			"Ending Paxos-Accept phase for instance %v... (abort=%v)",
			pos,
			abort,
		)
	}()

	instance.Status = ACCEPTED
	instance.Timer = time.Time{}
	e.persist()
	instanceCopy := *instance

	e.debug(topicLog, "Updated %v: %v", pos, *instance)

	e.Unlock()

	// broadcast and wait to accept from floor(N/2) other peers
	abort = e.broadcastAccept(instanceCopy)

	return
}

// commitPhase executes the commit phase of the EPaxos commit protocol.
func (e *EPaxos) commitPhase(pos LogIndex) {
	// update our instance as committed
	e.Lock()
	instance := e.getInstance(pos)

	// pre-condition: the instance must be valid
	assert(
		instance.Valid,
		e.me,
		"Expected %v to be valid",
		instance.Position,
	)

	instance.Status = COMMITTED
	instance.Timer = time.Time{}
	e.persist()
	instanceCopy := *instance

	e.debug(topicCommit, "Updated %v: %v", pos, *instance)

	e.Unlock()

	// broadcast and asynchronously wait to commit to other peers
	_ = e.broadcastCommit(instanceCopy)

	return
}

func (e *EPaxos) processRequest(cmd interface{}, ix LogIndex, nofast bool) {
	// run pre-accept phase
	commit, abort := e.preAcceptPhase(cmd, ix, nofast)

	if abort || e.killed() {
		return
	}

	if !commit {
		e.debug(topicAccept, "On slow path for %v, running Paxos-Accept...", ix)
		abort = e.acceptPhase(ix)
	} else {
		e.debug(topicCommit, "On fast path for %v, running Commit...", ix)
	}

	if abort || e.killed() {
		return
	}

	e.commitPhase(ix)
}

// broadcastPreAccept sends out PreAccept messages to all nodes.
func (e *EPaxos) broadcastPreAccept(instance Instance, nofast bool) (seq int, deps map[LogIndex]int, abort bool) {
	replyCount := 1
	rejectCount := 0
	unionSeq := instance.Seq
	unionDeps := instance.Deps
	quorum := e.numFastPath()
	majority := e.numPeers()/2 + 1

	lk := sync.NewCond(new(sync.Mutex))

	args := PreAcceptArgs{
		Command:  instance.Command,
		Deps:     instance.Deps,
		Seq:      instance.Seq,
		Ballot:   instance.Ballot,
		Position: instance.Position,
	}

	for i := 0; i < e.numPeers(); i++ {
		if i == e.me {
			continue
		}

		go func(peer int) {
			reply := PreAcceptReply{}

			for !e.sendPreAccept(peer, &args, &reply) {
				reply = PreAcceptReply{}
			}

			lk.L.Lock()
			defer lk.L.Unlock()

			replyCount++

			if reply.Success {
				unionSeq = max(unionSeq, reply.Seq)
				unionDeps = unionMaps(unionDeps, reply.Deps)
			} else {
				rejectCount++
			}

			lk.Broadcast()
		}(i)
	}

	lk.L.Lock()
	defer lk.L.Unlock()

	for !e.killed() {
		lk.Wait()

		// Check if any of our peers have rejected our request
		if rejectCount > 0 {
			e.debug(topicPreAccept, "Stepping down as command leader for %v...", instance.Position)
			abort = true
			break
		}

		if replyCount < majority {
			continue
		}

		// Fast path quorum
		if !nofast && replyCount >= quorum && instance.Seq == unionSeq && mapsEqual(instance.Deps, unionDeps) {
			e.debug(topicPreAccept, "Received succesful fast path quorum for instance %v...", instance.Position)
			abort = false
			break
		} else {
			e.debug(topicPreAccept, "Received succesful slow path quorum for instance %v...", instance.Position)
			abort = false
			break
		}
	}

	seq = unionSeq
	deps = unionDeps

	return
}

// PreAccept RPC handler.
func (e *EPaxos) PreAccept(args *PreAcceptArgs, reply *PreAcceptReply) {
	e.Lock()
	defer e.Unlock()

	defer e.persist()

	e.debug(topicPreAccept, "Receiving PreAccept from %v for %v", replicaName(args.Ballot.ReplicaNum), args.Position)
	defer e.debug(topicPreAccept, "Finished PreAccept from %v for %v", replicaName(args.Ballot.ReplicaNum), args.Position)

	// First check if the ballot number is invalid for the RPC request
	// This is done to ensure freshness of messages
	instance := e.getInstance(args.Position)
	instance.Timer = time.Now().Add(time.Duration(rand.Intn(300)) * time.Millisecond)
	if instance.Valid && instance.Ballot.gt(args.Ballot) {
		index := args.Position
		b := e.log[index.Replica][index.Index].Ballot
		e.debug(topicPreAccept, "Ballot number too low: %v < %v", args.Ballot, b)
		reply.Success = false
		return
	}

	if instance.Valid && instance.Status > PREACCEPTED {
		e.debug(topicPreAccept, "Instance in a further phase: %v < %v", PREACCEPTED, instance.Status)
		reply.Deps = instance.Deps
		reply.Seq = instance.Seq
		reply.Success = true
		return
	}

	// Next, we need to calculate deps and seq number
	// We only initiailize them if they have not been initialized before
	seq, deps := e.attributes(args.Command, args.Position)
	seq = max(seq, args.Seq)
	deps = unionMaps(deps, args.Deps)

	*instance = Instance{
		Deps:     deps,
		Seq:      seq,
		Command:  args.Command,
		Position: args.Position,
		Status:   PREACCEPTED,
		Ballot:   args.Ballot,
		Valid:    true,
		Timer:    time.Now(),
	}

	e.debug(topicPreAccept, "Updated %v: %v", args.Position, *instance)

	reply.Deps = instance.Deps
	reply.Seq = instance.Seq
	reply.Success = true
}

func (e *EPaxos) sendPreAccept(server int, args *PreAcceptArgs, reply *PreAcceptReply) bool {
	e.debug(topicRpc, "Calling %v.PreAccept...: %v", replicaName(server), args.Position)

	ok := e.peers[server].Call("EPaxos.PreAccept", args, reply)

	if ok {
		e.debug(topicRpc, "Finishing %v.PreAccept...: %v", replicaName(server), reply.Success)
	} else {
		e.debug(topicRpc, "Dropping %v.PreAccept...", replicaName(server))
	}
	return ok
}

// broadcastAccept sends out Accept messages to all nodes but only waits for a simple majority.
func (e *EPaxos) broadcastAccept(instance Instance) (abort bool) {
	replyCount := 1
	rejectCount := 0
	majority := e.numPeers()/2 + 1

	lk := sync.NewCond(new(sync.Mutex))

	args := AcceptArgs{
		Command:  instance.Command,
		Deps:     instance.Deps,
		Seq:      instance.Seq,
		Ballot:   instance.Ballot,
		Position: instance.Position,
	}

	for i := 0; i < e.numPeers(); i++ {
		if i == e.me {
			continue
		}

		go func(peer int) {
			reply := AcceptReply{}

			for !e.sendAccept(peer, &args, &reply) {
				reply = AcceptReply{}
			}

			lk.L.Lock()
			defer lk.L.Unlock()

			replyCount++

			if !reply.Success {
				rejectCount++
			}

			lk.Broadcast()
		}(i)
	}

	lk.L.Lock()
	defer lk.L.Unlock()

	for !e.killed() {
		lk.Wait()

		// Check if any of our peers have rejected our request
		if rejectCount > 0 {
			e.debug(topicAccept, "Stepping down as command leader for %v...", instance.Position)
			abort = true
			break
		}

		// Fast path quorum
		if replyCount >= majority {
			e.debug(topicAccept, "Received succesful slow path majority for instance %v...", instance.Position)
			abort = false
			break
		}
	}

	return
}

func (e *EPaxos) Accept(args *AcceptArgs, reply *AcceptReply) {
	e.Lock()
	defer e.Unlock()

	defer e.persist()

	e.debug(topicRpc, "Receiving Accept from %v for %v", replicaName(args.Ballot.ReplicaNum), args.Position)
	defer e.debug(topicRpc, "Finished Accept from %v for %v", replicaName(args.Ballot.ReplicaNum), args.Position)

	// First check if the ballot number is invalid for the RPC request
	// This is done to ensure freshness of messages
	instance := e.getInstance(args.Position)

	if instance.Valid && instance.Ballot.gt(args.Ballot) {
		index := args.Position
		b := e.log[index.Replica][index.Index].Ballot
		e.debug(topicAccept, "Ballot number too low: %v < %v", args.Ballot, b)
		reply.Success = false
		return
	}

	if instance.Valid && instance.Status > ACCEPTED {
		e.debug(topicAccept, "Instance in a further phase: %v < %v", ACCEPTED, instance.Status)
		reply.Success = true
		return
	}

	*instance = Instance{
		Deps:     args.Deps,
		Seq:      args.Seq,
		Command:  args.Command,
		Position: args.Position,
		Status:   ACCEPTED,
		Ballot:   args.Ballot,
		Valid:    true,
		Timer:    time.Now(),
	}

	e.debug(topicAccept, "Updated %v: %v", args.Position, *instance)

	reply.Success = true
}

func (e *EPaxos) sendAccept(server int, args *AcceptArgs, reply *AcceptReply) bool {
	e.debug(topicRpc, "Calling %v.Accept...: %v", replicaName(server), args.Position)

	ok := e.peers[server].Call("EPaxos.Accept", args, reply)

	if ok {
		e.debug(topicRpc, "Finishing %v.Accept...: %v", replicaName(server), reply.Success)
	} else {
		e.debug(topicRpc, "Dropping %v.Accept...", replicaName(server))
	}
	return ok
}

func (e *EPaxos) broadcastCommit(instance Instance) (abort bool) {
	lk := sync.NewCond(new(sync.Mutex))
	replyCount := 1
	rejectCount := 0

	args := CommitArgs{
		Command:  instance.Command,
		Deps:     instance.Deps,
		Seq:      instance.Seq,
		Ballot:   instance.Ballot,
		Position: instance.Position,
	}

	for i := 0; i < e.numPeers(); i++ {
		if i == e.me {
			continue
		}

		go func(peer int) {
			reply := CommitReply{}

			for !e.sendCommit(peer, &args, &reply) {
				reply = CommitReply{}
			}

			lk.L.Lock()
			defer lk.L.Unlock()

			replyCount++

			if !reply.Success {
				rejectCount++
			}

			lk.Broadcast()

		}(i)
	}

	lk.L.Lock()
	defer lk.L.Unlock()

	for !e.killed() {
		lk.Wait()

		if rejectCount > 0 {
			e.debug(topicCommit, "Stepping down as command leader for %v...", instance.Position)
			abort = true
			return
		}

		if replyCount == e.numPeers() {
			e.debug(topicCommit, "Successfully broadcast commit for %v...", instance.Position)
			abort = false
			break
		}
	}

	return
}

func (e *EPaxos) Commit(args *CommitArgs, reply *CommitReply) {
	e.Lock()
	defer e.Unlock()

	defer e.persist()

	e.debug(topicCommit, "Receiving Commit from %v for %v", replicaName(args.Ballot.ReplicaNum), args.Position)
	defer e.debug(topicCommit, "Finished Commit from %v for %v", replicaName(args.Ballot.ReplicaNum), args.Position)

	instance := e.getInstance(args.Position)

	if instance.Valid && instance.Ballot.gt(args.Ballot) {
		index := args.Position
		b := e.log[index.Replica][index.Index].Ballot
		e.debug(topicCommit, "Ballot number too low: %v < %v", args.Ballot, b)
		reply.Success = false
		return
	}

	if instance.Valid && instance.Status > COMMITTED {
		e.debug(topicCommit, "Instance in a further phase: %v < %v", COMMITTED, instance.Status)
		reply.Success = true
		return
	}

	*instance = Instance{
		Deps:     args.Deps,
		Seq:      args.Seq,
		Command:  args.Command,
		Position: args.Position,
		Status:   COMMITTED,
		Ballot:   args.Ballot,
		Valid:    true,
		Timer:    time.Time{},
	}

	e.debug(topicCommit, "Updated %v: %v", args.Position, *instance)

	reply.Success = true
}

func (e *EPaxos) sendCommit(server int, args *CommitArgs, reply *CommitReply) bool {
	e.debug(topicRpc, "Calling %v.Commit...: %v", replicaName(server), args.Position)

	ok := e.peers[server].Call("EPaxos.Commit", args, reply)

	if ok {
		e.debug(topicRpc, "Finishing %v.Commit...: %v", replicaName(server), reply.Success)
	} else {
		e.debug(topicRpc, "Dropping %v.Commit...", replicaName(server))
	}
	return ok
}

// the service using EPaxos (e.g. a k/v server) wants to start
// agreement on the next command to be appended to EPaxos's log.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (e *EPaxos) Start(command interface{}) (li LogIndex) {
	instanceNum := -1

	// Your code here (3B).
	if e.killed() {
		return LogIndex{Replica: e.me, Index: instanceNum}
	}

	e.Lock()
	defer e.Unlock()

	instanceNum = e.nextIndex
	e.nextIndex++

	li = LogIndex{Replica: e.me, Index: instanceNum}

	e.debug(topicClient, "Starting command at instance %v", li)

	go e.processRequest(command, li, false)

	return
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (e *EPaxos) Kill() {
	// e.Lock()
	// defer e.Unlock()

	e.debug(topicWarn, "Killing replica %v...", e.me)
	atomic.StoreInt32(&e.dead, 1)
}

func (e *EPaxos) killed() bool {
	z := atomic.LoadInt32(&e.dead)
	return z == 1
}

// save EPaxos's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
func (e *EPaxos) persist() {
	w := new(bytes.Buffer)
	en := labgob.NewEncoder(w)

	if err := en.Encode(e.nextIndex); err != nil {
		panic(err)
	}

	if err := en.Encode(e.log); err != nil {
		panic(err)
	}

	state := w.Bytes()

	e.persister.Save(state, e.persister.ReadSnapshot())
	e.debug(topicPersist, "Write state: nextIndex=%v, log=%v", e.nextIndex, e.log)
}

// restore previously persisted state.
func (e *EPaxos) readPersist() {
	state := e.persister.ReadEPaxosState()

	if state == nil || len(state) < 1 {
		return
	}

	r := bytes.NewBuffer(state)
	d := labgob.NewDecoder(r)

	var nextIndex int
	var log PaxosLog

	if err := d.Decode(&nextIndex); err != nil {
		panic(err)
	}

	if err := d.Decode(&log); err != nil {
		panic(err)
	}

	e.nextIndex = nextIndex
	e.log = log

	e.debug(topicPersist, "Read state: nextIndex=%v, log=%v", e.nextIndex, e.log)
}

func (e *EPaxos) primeLog() {
	for R := range e.log {
		for i := range e.log[R] {
			if !e.log[R][i].Valid {
				continue
			}

			if e.log[R][i].Status == EXECUTED {
				e.log[R][i].Status = COMMITTED
			}

			if e.log[R][i].Status != COMMITTED {
				e.log[R][i].Timer = time.Now()
			}
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan Instance, interferes func(cmd1, cmd2 interface{}) bool) *EPaxos { // modify to take in some function that can process interference between commands
	enableLogging()

	e := new(EPaxos)

	e.lock = sync.Mutex{}
	e.peers = peers
	e.me = me
	e.dead = 0

	e.applyCh = applyCh
	e.persister = persister
	e.interferes = interferes
	e.log = e.makeLog()
	e.nextIndex = 0

	e.lastApplied = make([]int, len(peers))

	for i := 0; i < len(peers); i++ {
		e.lastApplied[i] = -1
	}

	// initialize from state persisted before a crash
	e.readPersist()

	// set all executed operations to committed
	e.primeLog()

	// print out what state we are starting at
	e.debug(
		topicStart,
		"Starting at nextIndex: %v",
		e.nextIndex,
	)

	// start long running goroutine for execution
	go e.execute()
	go e.ExplicitPreparer()
	return e
}
