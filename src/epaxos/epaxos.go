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
	// "fmt"
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

// func (e *EPaxos) makeConflictsMap() []map[string]int {
// 	conflicts := make([]map[string]int, e.numPeers())
// 	for i := range conflicts {
// 		conflicts[i] = make(map[string]int, MAX_SIZE)
// 	}
// 	return conflicts
// }

// func (lg *Log) append(term int, command interface{}) int {
// 	*lg = append(*lg, LogEntry{Index: lg.size(), Term: term, Command: command})
// 	return lg.lastLogIndex()
// }

// A Go object implementing a single EPaxos peer.
type EPaxos struct {
	lock      sync.Mutex          // lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	// numPeers  int

	applyCh             chan<- Instance                   // channel used to send client messages
	interferenceChecker func(cmd1, cmd2 interface{}) bool // function passed in by client that checks whether two commands interfere

	log       PaxosLog // log across all replicas; nested array indexed by replica number, instance number
	nextIndex int      // index of next instance to be added to this replica

	instanceBallots [][]int // ballot number of each instance in each replica
	myBallot        int     // ballot number of this replica

	lastApplied []int // index of highest log entry known to be applied to state machine

	// so we can run a background goroutine to check if whether any timer has expired so we can step in and run explicit prepare for that instance
	timers [][]time.Time // time that each instance started processing an RPC

	// conflicts       []map[string]int // a slice of maps where index i represents replica i & maps the key of command
	// to the highest conflicting instance # within that replica
}

func (e *EPaxos) numPeers() int {
	return len(e.peers)
}

// # replicas in fast path quorum
func (e *EPaxos) numFastPath() int {
	return e.numPeers() - 2
}

// func (e *EPaxos) numSlowQuorum() int {
// 	F := (e.numPeers() - 1) / 2
// 	return F + 1
// }

// checks if two maps are equal
func mapsEqual(map1, map2 map[LogIndex]int) bool {
	if len(map1) != len(map2) {
		return false
	}
	for key, value1 := range map1 {
		value2, ok := map2[key]
		if !ok || value1 != value2 {
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

// func (e *EPaxos)

func (e *EPaxos) processRequest(cmd interface{}, instanceNum int) {
	e.Lock()

	// find seq num & deps
	// assuming every instance depends on the instance before it within a replica
	maxSeq := 0
	deps := make(map[LogIndex]int)
	// loop through all instances in replica L's 2D log
	for r, replica := range e.log {
		for i := len(replica) - 1; i >= 0; i-- { // loop through each replica backwards
			// fmt.Printf("replica %v, i %v, len(replica) %v\n", replica, i, len(replica))
			instance := replica[i]
			if e.interferenceChecker(cmd, instance.Command) {
				deps[LogIndex{Replica: r, Index: i}] = 1
				if instance.Seq > maxSeq {
					maxSeq = instance.Seq
				}
				break // only find the latest instance that interferes
			}
		}
	}
	// seq is larger than seq of all interfering commands in deps
	seq := maxSeq + 1
	// extend this replica, then append to this replica's logs
	for len(e.log[e.me]) <= instanceNum {
		e.log[e.me] = append(e.log[e.me], Instance{})
	}
	e.log[e.me][instanceNum] = Instance{
		Deps:    deps,
		Seq:     seq,
		Command: cmd,
		Position: LogIndex{
			Replica: e.me,
			Index:   instanceNum,
		},
		Status: PREACCEPTED,
	}

	// fmt.Printf("[command %v] e.log: %v\n", cmd, e.log)

	numPreAcceptResponses := 0
	fail := make(chan bool)

	// map of responses of RPCs
	responses := make(map[int]PreAcceptReply)
	responsesLock := sync.Mutex{}

	// send PreAccept message to replicas, wait for fast quorum of replies
	for i := 0; i < e.numPeers(); i++ {
		if i == e.me {
			continue
		}
		go e.broadcastPreAccept(i, e.log[e.me][instanceNum], &numPreAcceptResponses, fail, &responses, &responsesLock)
	}
	e.Unlock()

	// fmt.Printf("[command %v] finished broadcasting pre-accept messages!\n", cmd)

	for !e.killed() {
		// fmt.Printf("[command %v] numPreAcceptResponses: %v\n", cmd, numPreAcceptResponses)
		if numPreAcceptResponses >= e.numPeers()/2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// check if channel fail
	select {
	case <-fail:
		return // one of the replicas had a higher ballot # so we return
	default:
	}
	e.Lock()
	// fmt.Printf("[command %v] reached majority of preaccepts! all logs: %v\n", cmd, e.log)

	// check whether all deps & seqs are same
	sameReplies := true
	unionedSeq := seq
	unionedDeps := deps
	// only qualifies for fast path if at least N - 2 responses match
	if len(responses) < e.numFastPath() {
		// fmt.Printf("[command %v] don't have numFastPath %v responses, only have %v\n", cmd, e.numFastPath(), len(responses))
		sameReplies = false
	} else {
		for _, response := range responses {
			// fmt.Printf("[command %v] looping through responses, on response %v\n", cmd, response)
			if !response.Success || !mapsEqual(deps, response.Deps) || seq != response.Seq {
				sameReplies = false
			}
			// find max seq & unioned deps
			unionedSeq = max(unionedSeq, response.Seq)
			unionedDeps = unionMaps(unionedDeps, response.Deps)
		}
	}
	// run commit phase
	if sameReplies {
		// fmt.Printf("[command %v] all replies are same.. running commit\n", cmd)
		for i := 0; i < e.numPeers(); i++ {
			if i == e.me {
				continue
			}
			e.log[e.me][instanceNum].Status = COMMITTED
			go e.broadcastCommit(i, e.log[e.me][instanceNum])
		}
	} else {
		// fmt.Printf("[command %v] either not enough replies or not all replies are same.. running accept\n", cmd)
		numAcceptResponses := 0
		acceptFail := make(chan bool)
		acceptResponses := make(map[int]AcceptReply)
		acceptResponsesLock := sync.Mutex{}

		// run paxos-accept phase
		for i := 0; i < e.numPeers(); i++ {
			if i == e.me {
				continue
			}
			e.log[e.me][instanceNum].Status = ACCEPTED
			e.log[e.me][instanceNum].Deps = unionedDeps
			e.log[e.me][instanceNum].Seq = unionedSeq
			// fmt.Printf("broadcasting accept messages...\n")
			go e.broadcastAccept(i, e.log[e.me][instanceNum], &numAcceptResponses, acceptFail, &acceptResponses, &acceptResponsesLock)
		}
		e.Unlock()

		for !e.killed() {
			if numAcceptResponses >= e.numPeers()/2 {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		// fmt.Printf("[command %v] reached majority of accepts! all logs: %v\n", cmd, e.log)

		// run commit phase
		e.Lock()
		for i := 0; i < e.numPeers(); i++ {
			if i == e.me {
				continue
			}
			e.log[e.me][instanceNum].Status = COMMITTED
			e.log[e.me][instanceNum].Deps = unionedDeps
			e.log[e.me][instanceNum].Seq = unionedSeq
			// fmt.Printf("broadcasting commit messages...\n")
			go e.broadcastCommit(i, e.log[e.me][instanceNum])
		}
		e.Unlock()
	}
	// fmt.Printf("[command %v] finished processing request %v! should be committed\n", cmd, instanceNum)
	// fmt.Printf("[command %v] e.log after %v\n", cmd, e.log)
	// send RequestReply to client?
}

// call when holding e.Lock()
func (e *EPaxos) broadcastPreAccept(peer int, instance Instance, numResponses *int, fail chan bool, responses *map[int]PreAcceptReply, responsesLock *sync.Mutex) {
	// fmt.Printf("[peer %v] in pre-accept\n", peer)
	e.lock.Lock()
	for !e.killed() {
		// fmt.Printf("[peer %v, command %v] in loop\n", peer, cmd)
		cmd, deps, seq, pos := instance.Command, instance.Deps, instance.Seq, instance.Position
		args := PreAcceptArgs{Command: cmd, Deps: deps, Seq: seq, Ballot: Ballot{BallotNum: e.myBallot, ReplicaNum: e.me}, Position: pos}
		reply := PreAcceptReply{}
		e.lock.Unlock()
		ok := e.sendPreAccept(peer, &args, &reply)
		// fmt.Printf("[peer %v, command %v] result of sendPreAccept: %v\n", peer, cmd, ok)
		if ok {
			e.lock.Lock()
			if !reply.Success {
				fail <- false
			}
			responsesLock.Lock()
			(*responses)[peer] = reply
			responsesLock.Unlock()
			*numResponses++
			// fmt.Printf("[peer %v, command %v] replying to PreAccept, numResponses: %v\n", peer, cmd, *numResponses)
			e.lock.Unlock()
			return
		} else { // keep trying if not ok
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// PreAccept RPC handler.
func (e *EPaxos) PreAccept(args *PreAcceptArgs, reply *PreAcceptReply) {
	// fmt.Printf("[peer %v, command %v] IN PREACCEPT RPC HANDLER \n", e.me, args.Command)
	ballot := args.Ballot
	bNum := ballot.BallotNum
	pos := args.Position
	instanceInd, replicaInd := pos.Index, pos.Replica

	e.lock.Lock()
	// [insert helper] if ballot number i receive is smaller than the largest ballot number i've seen so far, reply false
	if len(e.instanceBallots[e.me]) > instanceInd && bNum < e.instanceBallots[e.me][instanceInd] {
		reply.Success = false
		return
	}

	// [insert helper] start timer -- add extra entries
	for len(e.timers[replicaInd]) <= instanceInd {
		e.timers[replicaInd] = append(e.timers[replicaInd], time.Now())
	}
	e.timers[replicaInd][instanceInd] = time.Now()

	maxSeq := args.Seq
	cmd := args.Command
	depsL := args.Deps
	depsR := make(map[LogIndex]int) // construct this replica's dependencies map
	for r, replica := range e.log {
		for i := len(replica) - 1; i >= 0; i-- { // loop through instances of each replica backwards
			instance := replica[i]
			if e.interferenceChecker(cmd, instance.Command) {
				depsR[LogIndex{Replica: r, Index: i}] = 1
				if instance.Seq > maxSeq {
					maxSeq = instance.Seq
				}
				break // only find the latest instance that interferes
			}
		}
	}
	e.lock.Unlock()
	maxSeq += 1

	// fmt.Printf("[peer %v preaccept, command %v] e.log %v replicaInd %v instanceInd %v \n", e.me, args.Command, e.log, replicaInd, instanceInd)

	unionedDeps := unionMaps(depsL, depsR)
	// extend replica L's logs within this view, then append to its logs
	for len(e.log[replicaInd]) <= instanceInd {
		e.log[replicaInd] = append(e.log[replicaInd], Instance{})
	}
	e.log[replicaInd][instanceInd] = Instance{
		Deps:    unionedDeps,
		Seq:     maxSeq,
		Command: cmd,
		Position: LogIndex{
			Replica: replicaInd,
			Index:   instanceInd,
		},
		Status: PREACCEPTED,
	}
	// fmt.Printf("[peer %v preaccept END, command %v] e.log %v\n", e.me, args.Command, e.log)
	// reply with union of dependencies & new max seq #
	reply.Deps = unionedDeps
	reply.Seq = maxSeq
	reply.Success = true
}

func (e *EPaxos) sendPreAccept(server int, args *PreAcceptArgs, reply *PreAcceptReply) bool {
	// fmt.Printf("[command %v] sending PreAccept from %v to %v\n", args.Command, e.me, server)
	ok := e.peers[server].Call("EPaxos.PreAccept", args, reply)
	return ok
}

func (e *EPaxos) broadcastAccept(peer int, instance Instance, numAcceptResponses *int, fail chan bool, responses *map[int]AcceptReply, responsesLock *sync.Mutex) {
	e.lock.Lock()
	for !e.killed() {
		cmd, deps, seq, pos := instance.Command, instance.Deps, instance.Seq, instance.Position
		args := AcceptArgs{Command: cmd, Deps: deps, Seq: seq, Ballot: Ballot{BallotNum: e.myBallot, ReplicaNum: e.me}, Position: pos}
		reply := AcceptReply{}
		e.lock.Unlock()
		ok := e.sendAccept(peer, &args, &reply)
		// fmt.Printf("[peer %v, command %v] result of sendAccept: %v\n", peer, cmd, ok)
		if ok {
			e.lock.Lock()
			if !reply.Success {
				fail <- false
			}
			responsesLock.Lock()
			(*responses)[peer] = reply
			responsesLock.Unlock()
			*numAcceptResponses++
			e.lock.Unlock()
			return
		} else { // keep trying if not ok
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (e *EPaxos) Accept(args *AcceptArgs, reply *AcceptReply) {
	e.lock.Lock()
	defer e.lock.Unlock()

	ballot := args.Ballot
	bNum := ballot.BallotNum
	pos := args.Position
	instanceInd, replicaInd := pos.Index, pos.Replica

	// ballot # check
	if len(e.instanceBallots[e.me]) > instanceInd && bNum < e.instanceBallots[e.me][instanceInd] {
		reply.Success = false
		return
	}

	// start timer
	for len(e.timers[replicaInd]) <= instanceInd {
		e.timers[replicaInd] = append(e.timers[replicaInd], time.Now())
	}
	e.timers[replicaInd][instanceInd] = time.Now()

	e.log[replicaInd][instanceInd].Status = ACCEPTED
	e.log[replicaInd][instanceInd].Deps = args.Deps
	e.log[replicaInd][instanceInd].Seq = args.Seq
	reply.Success = true

}

func (e *EPaxos) sendAccept(server int, args *AcceptArgs, reply *AcceptReply) bool {
	ok := e.peers[server].Call("EPaxos.Accept", args, reply)
	return ok
}

func (e *EPaxos) broadcastCommit(peer int, instance Instance) {
	e.lock.Lock()
	for !e.killed() {
		cmd, deps, seq, pos := instance.Command, instance.Deps, instance.Seq, instance.Position
		// fmt.Printf("[peer %v, command %v] inBroadcastCommit\n", peer, cmd)
		args := CommitArgs{Command: cmd, Deps: deps, Seq: seq, Ballot: Ballot{BallotNum: e.myBallot, ReplicaNum: e.me}, Position: pos}
		reply := CommitReply{}
		e.lock.Unlock()
		ok := e.sendCommit(peer, &args, &reply)
		// fmt.Printf("[peer %v, command %v] result of Commit: %v\n", peer, cmd, ok)
		if ok {
			return
		} else { // keep trying if not ok
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (e *EPaxos) Commit(args *CommitArgs, reply *CommitReply) {
	e.lock.Lock()
	defer e.lock.Unlock()

	ballot := args.Ballot
	bNum := ballot.BallotNum
	pos := args.Position
	instanceInd, replicaInd := pos.Index, pos.Replica

	// ballot # check
	if len(e.instanceBallots[e.me]) > instanceInd && bNum < e.instanceBallots[e.me][instanceInd] {
		reply.Success = false
		return
	}

	// start timer
	for len(e.timers[replicaInd]) <= instanceInd {
		e.timers[replicaInd] = append(e.timers[replicaInd], time.Now())
	}
	e.timers[replicaInd][instanceInd] = time.Now()

	e.log[replicaInd][instanceInd].Status = COMMITTED
	reply.Success = true
}

func (e *EPaxos) sendCommit(server int, args *CommitArgs, reply *CommitReply) bool {
	ok := e.peers[server].Call("EPaxos.Commit", args, reply)
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

	go e.processRequest(command, instanceNum)

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
	e.interferenceChecker = interferes
	e.log = e.makeLog()
	e.nextIndex = 0

	e.instanceBallots = make([][]int, len(peers))
	e.myBallot = 0

	e.lastApplied = make([]int, len(peers))
	e.timers = make([][]time.Time, len(peers))

	// print out what state we are starting at
	e.debug(
		topicStart,
		"Starting at ballot: %v, nextIndex: %v",
		e.myBallot,
		e.nextIndex,
	)

	return e
}
