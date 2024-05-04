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
	"sync"
	"sync/atomic"
	"time"

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
		// NOTE(kosinw): We don't want to have dependencies on later instances
		// if r == ix.Replica {
		// 	if ix.Index > 0 {
		// 		deps[LogIndex{Replica: ix.Replica, Index: ix.Index - 1}] = 1
		// 	}
		// 	continue
		// }

		for i := len(replica) - 1; i >= 0; i-- { // loop through each replica backwards
			instance := replica[i]

			if instance.Valid && e.interferes(cmd, instance.Command) {
				deps[LogIndex{Replica: r, Index: i}] = 1
				maxSeq = max(maxSeq, instance.Seq)
				break // only find the latest instance that interferes
			}
		}
	}

	// seq is larger than seq of all interfering commands in deps
	seq = maxSeq + 1

	e.debug(topicInfo, "%v: deps: %v", ix, deps)
	e.debug(topicInfo, "%v: seq: %v", ix, seq)

	return
}

// makeInstance adds [cmd] to instance log with given [deps] and [seq].
func (e *EPaxos) makeInstance(cmd interface{}, index LogIndex, deps map[LogIndex]int, seq int) Instance {
	instance := e.getInstance(index)

	// Update our log
	*instance = Instance{
		Deps:     deps,
		Seq:      seq,
		Command:  cmd,
		Position: index,
		Status:   PREACCEPTED,
		Ballot:   Ballot{BallotNum: 0, ReplicaNum: index.Replica},
		Valid:    true,
		Timer:    time.Time{},
	}

	e.debug(topicLog, "Created %v: %v", index, *instance)

	return *instance
}

// getInstance returns a pointer to an instance in our log given an index.
func (e *EPaxos) getInstance(index LogIndex) *Instance {
	// extend this replica, then append to this replica's logs
	for len(e.log[index.Replica]) <= index.Index {
		e.log[index.Replica] = append(e.log[index.Replica], Instance{Valid: false})
	}

	return &e.log[index.Replica][index.Index]
}

// preAcceptPhase executes the pre-accept phase for the EPaxos commit protocol.
func (e *EPaxos) preAcceptPhase(cmd interface{}, ix LogIndex) (commit bool, abort bool) {
	e.Lock()

	abort = false
	commit = false

	assert(ix.Replica == e.me, e.me, "log index %v should have replica %v", ix, replicaName(e.me))

	e.debug(topicInfo, "Starting pre-accept phase for instance %v...", ix)

	// Print out how phase 1 ended
	defer func() {
		e.debug(
			topicInfo,
			"Ending pre-accept phase for instance %v... (commit=%v abort=%v)",
			ix,
			commit,
			abort,
		)
	}()

	// calculate seq and deps
	seq, deps := e.attributes(cmd, ix)

	// put cmd into our log
	instance := e.makeInstance(cmd, ix, deps, seq)

	e.Unlock()

	// broadcast and wait for pre-accept messages to our other peers
	updatedSeq, updatedDeps, abort := e.broadcastPreAccept(instance, e.numFastPath())

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
		e.debug(topicLog, "Updated %v: %v", ix, *instance)
	}

	// only commit if nothing changed
	commit = (seq == updatedSeq) && (mapsEqual(deps, updatedDeps))

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

	// pre-condition: the instance must either be pre-accepted or accepted
	assert(
		instance.Status == PREACCEPTED || instance.Status == ACCEPTED,
		e.me,
		"Expected %v to be pre-accepted or accepted, instead: %v",
		instance.Position,
		instance.Status,
	)

	instance.Status = COMMITTED
	instance.Timer = time.Time{}

	e.Unlock()

	// broadcast and asynchronously wait to commit to other peers
	_ = e.broadcastCommit(*instance)

	return
}

func (e *EPaxos) processRequest(cmd interface{}, ix LogIndex) {
	// run pre-accept phase
	commit, abort := e.preAcceptPhase(cmd, ix)

	if abort || e.killed() {
		return
	}

	// We are not on the fast-path, first execute Paxos-Accept phase
	if !commit {
		e.debug(topicInfo, "On slow path for %v...", ix)
	} else {
		e.debug(topicInfo, "On fast path for %v...", ix)
	}

	e.commitPhase(ix)

	// run commit phase
	// if sameReplies {
	// 	// fmt.Printf("[command %v] all replies are same.. running commit\n", cmd)
	// 	for i := 0; i < e.numPeers(); i++ {
	// 		if i == e.me {
	// 			continue
	// 		}
	// 		e.log[e.me][instanceNum].Status = Committed
	// 		go e.broadcastCommit(i, e.log[e.me][instanceNum])
	// 	}
	// } else {
	// 	// fmt.Printf("[command %v] either not enough replies or not all replies are same.. running accept\n", cmd)
	// 	numAcceptResponses := 0
	// 	acceptFail := make(chan bool)
	// 	acceptResponses := make(map[int]AcceptReply)
	// 	acceptResponsesLock := sync.Mutex{}

	// 	// run paxos-accept phase
	// 	for i := 0; i < e.numPeers(); i++ {
	// 		if i == e.me {
	// 			continue
	// 		}
	// 		e.log[e.me][instanceNum].Status = Accepted
	// 		e.log[e.me][instanceNum].Deps = unionedDeps
	// 		e.log[e.me][instanceNum].Seq = unionedSeq
	// 		// fmt.Printf("broadcasting accept messages...\n")
	// 		go e.broadcastAccept(i, e.log[e.me][instanceNum], &numAcceptResponses, acceptFail, &acceptResponses, &acceptResponsesLock)
	// 	}
	// 	e.Unlock()

	// 	for !e.killed() {
	// 		if numAcceptResponses >= e.numPeers()/2 {
	// 			break
	// 		}
	// 		time.Sleep(10 * time.Millisecond)
	// 	}
	// 	// fmt.Printf("[command %v] reached majority of accepts! all logs: %v\n", cmd, e.log)

	// 	// run commit phase
	// 	e.Lock()
	// 	for i := 0; i < e.numPeers(); i++ {
	// 		if i == e.me {
	// 			continue
	// 		}
	// 		e.log[e.me][instanceNum].Status = Committed
	// 		e.log[e.me][instanceNum].Deps = unionedDeps
	// 		e.log[e.me][instanceNum].Seq = unionedSeq
	// 		// fmt.Printf("broadcasting commit messages...\n")
	// 		go e.broadcastCommit(i, e.log[e.me][instanceNum])
	// 	}
	// 	e.Unlock()
	// }
	// fmt.Printf("[command %v] finished processing request %v! should be committed\n", cmd, instanceNum)
	// fmt.Printf("[command %v] e.log after %v\n", cmd, e.log)
	// send RequestReply to client?
}

// call when holding e.Lock()
// func (e *EPaxos) broadcastPreAccept(peer int, instance Instance, numResponses *int, fail chan bool, responses *map[int]PreAcceptReply, responsesLock *sync.Mutex) {
// 	// fmt.Printf("[peer %v] in pre-accept\n", peer)
// 	e.lock.Lock()
// 	for !e.killed() {
// 		// fmt.Printf("[peer %v, command %v] in loop\n", peer, cmd)
// 		cmd, deps, seq, pos := instance.Command, instance.Deps, instance.Seq, instance.Position
// 		args := PreAcceptArgs{Command: cmd, Deps: deps, Seq: seq, Ballot: Ballot{BallotNum: e.myBallot, ReplicaNum: e.me}, Position: pos}
// 		reply := PreAcceptReply{}
// 		e.lock.Unlock()
// 		ok := e.sendPreAccept(peer, &args, &reply)
// 		// fmt.Printf("[peer %v, command %v] result of sendPreAccept: %v\n", peer, cmd, ok)
// 		if ok {
// 			e.lock.Lock()
// 			if !reply.Success {
// 				fail <- false
// 			}
// 			responsesLock.Lock()
// 			(*responses)[peer] = reply
// 			responsesLock.Unlock()
// 			*numResponses++
// 			// fmt.Printf("[peer %v, command %v] replying to PreAccept, numResponses: %v\n", peer, cmd, *numResponses)
// 			e.lock.Unlock()
// 			return
// 		} else { // keep trying if not ok
// 			time.Sleep(10 * time.Millisecond)
// 		}
// 	}
// }

// broadcastPreAccept sends out PreAccept messages to a fast quorum of nodes.
// [quorum] is the size of the quorum we need to satisfy.
// [abort] means at least one replica rejected our PreAccept message meaning we are not the command leader.
// Returns the union of all the dependencies and the updated sequence number.
func (e *EPaxos) broadcastPreAccept(instance Instance, quorum int) (seq int, deps map[LogIndex]int, abort bool) {
	replyCount := 1
	rejectCount := 0
	unionSeq := instance.Seq
	unionDeps := instance.Deps

	lk := sync.NewCond(new(sync.Mutex))

	args := PreAcceptArgs{
		Command:  instance.Command,
		Deps:     instance.Deps,
		Seq:      instance.Seq,
		Ballot:   instance.Ballot,
		Position: instance.Position,
	}

	// TODO(kosinw): We should implement the thrifty optimization from 6.2
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
			e.debug(topicError, "Stepping down as command leader for %v...", instance.Position)
			abort = true
			break
		}

		// Fast path quorum
		if replyCount >= ((e.numPeers()/2)+1) && instance.Seq == unionSeq && mapsEqual(instance.Deps, unionDeps) {
			e.debug(topicInfo, "Received succesful fast path quorum for instance %v...", instance.Position)
			abort = false
			break
		}

		// Wait for all the replies
		if replyCount >= quorum {
			e.debug(topicInfo, "Received succesful slow path quorum for instance %v...", instance.Position)
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

	e.debug(topicRpc, "Receiving PreAccept from %v for %v", replicaName(args.Ballot.ReplicaNum), args.Position)
	defer e.debug(topicRpc, "Finished PreAccept from %v for %v", replicaName(args.Ballot.ReplicaNum), args.Position)

	// First check if the ballot number is invalid for the RPC request
	// This is done to ensure freshness of messages
	instance := e.getInstance(args.Position)

	if instance.Valid && instance.Ballot.gt(args.Ballot) {
		index := args.Position
		b := e.log[index.Replica][index.Index].Ballot
		e.debug(topicInfo, "Ballot number too low: %v < %v", args.Ballot, b)
		reply.Success = false
		return
	}

	if instance.Valid && instance.Ballot.eq(args.Ballot) && instance.Status > PREACCEPTED {
		e.debug(topicInfo, "Instance in a further phase: %v < %v", PREACCEPTED, instance.Status)
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

	e.debug(topicLog, "Updated %v: %v", args.Position, *instance)

	reply.Deps = instance.Deps
	reply.Seq = instance.Seq
	reply.Success = true
}

func (e *EPaxos) sendPreAccept(server int, args *PreAcceptArgs, reply *PreAcceptReply) bool {
	e.debug(topicWarn, "Calling %v.PreAccept...: %v", replicaName(server), args.Position)

	ok := e.peers[server].Call("EPaxos.PreAccept", args, reply)

	if ok {
		e.debug(topicWarn, "Finishing %v.PreAccept...: %v", replicaName(server), reply.Success)
	} else {
		e.debug(topicWarn, "Dropping %v.PreAccept...", replicaName(server))
	}
	return ok
}

// func (e *EPaxos) broadcastAccept(peer int, instance Instance, numAcceptResponses *int, fail chan bool, responses *map[int]AcceptReply, responsesLock *sync.Mutex) {
// 	e.lock.Lock()
// 	for !e.killed() {
// 		cmd, deps, seq, pos := instance.Command, instance.Deps, instance.Seq, instance.Position
// 		args := AcceptArgs{Command: cmd, Deps: deps, Seq: seq, Ballot: Ballot{BallotNum: e.myBallot, ReplicaNum: e.me}, Position: pos}
// 		reply := AcceptReply{}
// 		e.lock.Unlock()
// 		ok := e.sendAccept(peer, &args, &reply)
// 		// fmt.Printf("[peer %v, command %v] result of sendAccept: %v\n", peer, cmd, ok)
// 		if ok {
// 			e.lock.Lock()
// 			if !reply.Success {
// 				fail <- false
// 			}
// 			responsesLock.Lock()
// 			(*responses)[peer] = reply
// 			responsesLock.Unlock()
// 			*numAcceptResponses++
// 			e.lock.Unlock()
// 			return
// 		} else { // keep trying if not ok
// 			time.Sleep(10 * time.Millisecond)
// 		}
// 	}
// }

func (e *EPaxos) Accept(args *AcceptArgs, reply *AcceptReply) {
	e.lock.Lock()
	defer e.lock.Unlock()

	// ballot := args.Ballot
	// bNum := ballot.BallotNum
	// pos := args.Position
	// instanceInd, replicaInd := pos.Index, pos.Replica

	// // ballot # check
	// if len(e.instanceBallots[e.me]) > instanceInd && bNum < e.instanceBallots[e.me][instanceInd] {
	// 	reply.Success = false
	// 	return
	// }

	// // start timer
	// for len(e.timers[replicaInd]) <= instanceInd {
	// 	e.timers[replicaInd] = append(e.timers[replicaInd], time.Now())
	// }
	// e.timers[replicaInd][instanceInd] = time.Now()

	// e.log[replicaInd][instanceInd].Status = Accepted
	// e.log[replicaInd][instanceInd].Deps = args.Deps
	// e.log[replicaInd][instanceInd].Seq = args.Seq
	// reply.Success = true

}

func (e *EPaxos) sendAccept(server int, args *AcceptArgs, reply *AcceptReply) bool {
	ok := e.peers[server].Call("EPaxos.Accept", args, reply)
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

				lk.L.Lock()
				defer lk.L.Unlock()

				replyCount++

				if !reply.Success {
					rejectCount++
				}

				lk.Broadcast()
			}

		}(i)
	}

	lk.L.Lock()
	defer lk.L.Unlock()

	for !e.killed() {
		lk.Wait()

		if rejectCount > 0 {
			e.debug(topicError, "Stepping down as command leader for %v...", instance.Position)
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
	e.lock.Lock()
	defer e.lock.Unlock()

	e.debug(topicRpc, "Receiving Commit from %v for %v", replicaName(args.Ballot.ReplicaNum), args.Position)
	defer e.debug(topicRpc, "Finished Commit from %v for %v", replicaName(args.Ballot.ReplicaNum), args.Position)

	instance := e.getInstance(args.Position)

	if instance.Valid && instance.Ballot.gt(args.Ballot) {
		index := args.Position
		b := e.log[index.Replica][index.Index].Ballot
		e.debug(topicInfo, "Ballot number too low: %v < %v", args.Ballot, b)
		reply.Success = false
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

	reply.Success = true
}

func (e *EPaxos) sendCommit(server int, args *CommitArgs, reply *CommitReply) bool {
	e.debug(topicWarn, "Calling %v.Commit...: %v", replicaName(server), args.Position)

	ok := e.peers[server].Call("EPaxos.Commit", args, reply)

	if ok {
		e.debug(topicWarn, "Finishing %v.Commit...: %v", replicaName(server), reply.Success)
	} else {
		e.debug(topicWarn, "Dropping %v.Commit...", replicaName(server))
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

	e.lock.Lock()
	defer e.lock.Unlock()

	instanceNum = e.nextIndex
	e.nextIndex++

	li = LogIndex{Replica: e.me, Index: instanceNum}

	e.debug(topicClient, "Starting command at instance %v", li)

	go e.processRequest(command, li)

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
	e.lock.Lock()
	defer e.lock.Unlock()

	atomic.StoreInt32(&e.dead, 1)

	enableLogging()
	e.debug(topicInfo, "Killing replica %v...", e.me)
	disableLogging()
}

func (e *EPaxos) killed() bool {
	z := atomic.LoadInt32(&e.dead)
	return z == 1
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

	// print out what state we are starting at
	e.debug(
		topicStart,
		"Starting at nextIndex: %v",
		e.nextIndex,
	)

	return e
}
