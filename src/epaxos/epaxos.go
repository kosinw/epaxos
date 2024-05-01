package epaxos

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labrpc"
)

// func (le LogEntry) String() string {
// 	return fmt.Sprintf("{T:%v I:%v}", le.Term, le.Index)
// }

// A Go type representing the persistent log.
type PaxosLog [][]Instance

func (e *EPaxos) makeLog() PaxosLog { 
	log := make([][]Instance, e.numPeers())
	for i := range log {
		log[i] = make([]Instance, 0)
	}
	return log
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
	lock      			sync.Mutex          				// lock to protect shared access to this peer's state
	peers     			[]*labrpc.ClientEnd 				// RPC end points of all peers
	persister 			*Persister          				// Object to hold this peer's persisted state
	me        			int                 				// this peer's index into peers[]
	dead      			int32               				// set by Kill()
	// numPeers  int

	applyCh 			chan<-Instance 						// channel used to send client messages
	interferenceChecker func(cmd1, cmd2 interface{}) bool	// function passed in by client that checks whether two commands interfere
	
	log         		PaxosLog   							// log across all replicas; nested array indexed by replica number, instance number
	nextIndex 			int	 								// index of next instance to be added to this replica

	instanceBallots 	[][]int								// ballot number of each instance in each replica
	myBallot			int									// ballot number of this replica

	lastApplied 		[]int      							// index of highest log entry known to be applied to state machine
	
	// so we can run a background goroutine to check if whether any timer has expired so we can step in and run explicit prepare for that instance
	timers 				[][]time.Time						// time that each instance started processing an RPC

	// conflicts       []map[string]int // a slice of maps where index i represents replica i & maps the key of command 
										// to the highest conflicting instance # within that replica
}

func (e *EPaxos) majority() int {
	return (e.numPeers() / 2) + 1
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

// return currentTerm and whether this server
// believes it is the leader.
// [TODO] what should GetState return now? maybe don't need
func (e *EPaxos) GetState() (int, bool) {
	// Your code here (3A).
	e.lock.Lock()
	defer e.lock.Unlock()

	// return term, isleader
	return 0, false
}

func (e *EPaxos) persist() {
}

func (e *EPaxos) readPersist() {
}

func (e *EPaxos) processRequest(cmd interface{}) {
	// increment instance # for this replica, the leader of command cmd
	instanceNum := e.nextIndex
	e.nextIndex++

	// find seq num & deps
	// assuming every instance depends on the instance before it within a replica
	maxSeq := 0
    deps := make(map[LogIndex]int)
	instances := e.log[e.me]
	// loop through all instances in replica L
	for i := len(instances) - 1; i >= 0; i-- {
		if e.interferenceChecker(cmd, instances[i].Command) {
			deps[LogIndex{ Replica: e.me, Index: i }] = 1
			if instances[i].Seq > maxSeq {
				maxSeq = instances[i].Seq
			}
			break // we only need to find the latest instance that interferes
		}
	}
	// seq is larger than seq of all interfering commands in deps
    seq := maxSeq + 1 
	// append to this replica's logs
	e.log[e.me][instanceNum] = Instance{
		Deps: deps,
		Seq: seq,
		Command: cmd,
		Position: LogIndex{
			Replica: e.me,
			Index: instanceNum,
		},
		Status: PREACCEPTED,
	}

	var wg sync.WaitGroup
	wg.Add(e.numPeers() / 2) // start with N/2
	fail := make(chan bool)

	// map of responses of RPCs
	responses := make(map[int]PreAcceptReply)
    responsesLock := sync.Mutex{}

	// send PreAccept message to fast quorum of replicas (that includes L)
	for i := 0; i < e.numPeers(); i++ {
		if i == e.me {
			continue
		}
		go e.broadcastPreAccept(i, cmd, deps, seq, &wg, fail, &responses, &responsesLock)
	}

	wg.Wait() // gotten replies from N/2 + 1 (including self) replicas
	// check if channel fail
	e.lock.Unlock()
	select {
    case <-fail:
		return // one of the replicas had a higher ballot # so we return
    default:
    }
	e.lock.Lock()

	// check whether all deps & seqs are same
	sameReplies := true
	unionedSeq := seq
	unionedDeps := deps
	// only qualifies for fast path if at least N - 2 responses match
	if len(responses) < e.numFastPath() {
		sameReplies = false
	}
	for _, response := range responses {
		if !response.Success || !mapsEqual(deps, response.Deps) || seq != response.Seq {
			sameReplies = false
		}
		// find max seq & unioned deps
		unionedSeq = max(unionedSeq, response.Seq)
		unionedDeps = unionMaps(unionedDeps, response.Deps)
	}
	// run commit phase
	if sameReplies {
		for i := 0; i < e.numPeers(); i++ {
			if i == e.me {
				continue
			}
			e.log[e.me][instanceNum].Status = COMMITTED
			go e.broadcastCommit(i, cmd, deps, seq)
		}
	} else {
		var acceptWg sync.WaitGroup
		acceptWg.Add(e.numPeers() / 2) // start with N/2
		acceptFail := make(chan bool)
		acceptResponses := make(map[int]AcceptReply)
		acceptResponsesLock := sync.Mutex{}

		// run paxos-accept phase
		for i := 0; i < e.numPeers(); i++ {
			if i == e.me {
				continue
			}
			e.log[e.me][instanceNum].Status = ACCEPTED
			go e.broadcastAccept(i, cmd, unionedDeps, unionedSeq, &acceptWg, acceptFail, &acceptResponses, &acceptResponsesLock)
		}

		acceptWg.Wait()
		// run commit phase
		for i := 0; i < e.numPeers(); i++ {
			if i == e.me {
				continue
			}
			e.log[e.me][instanceNum].Status = COMMITTED
			go e.broadcastCommit(i, cmd, unionedDeps, unionedSeq)
		}
	}
	// [TODO] send RequestReply to client?
}

// call when holding e.Lock()
func (e *EPaxos) broadcastPreAccept(peer int, cmd interface{}, deps map[LogIndex]int, seq int, wg *sync.WaitGroup, fail chan bool, responses *map[int]PreAcceptReply, responsesLock *sync.Mutex) {
	defer wg.Add(-1) // decrement wg
	for !e.killed() {
		args := PreAcceptArgs{ Command: cmd, Deps: deps, Seq: seq, Ballot: Ballot{ BallotNum: e.myBallot, ReplicaNum: e.me } }
		reply := PreAcceptReply{}
		e.lock.Unlock()
		ok := e.sendPreAccept(peer, &args, &reply)
		e.lock.Lock()
		if ok {
			if reply.Success == false {
				fail <- false
			}
			responsesLock.Lock()
			(*responses)[peer] = reply
			responsesLock.Unlock()
			return
		} else { // keep trying if not ok
			e.lock.Unlock()
			time.Sleep(10 * time.Millisecond)
			e.lock.Lock()
		}
	}
}

// PreAccept RPC handler.
func (e *EPaxos) PreAccept(args *PreAcceptArgs, reply *PreAcceptReply) {
	e.lock.Lock()
	defer e.lock.Unlock()
	
	ballot := args.Ballot
	bNum := ballot.BallotNum
	pos := args.Position
	instanceInd, replicaInd := pos.Index, pos.Replica

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
	instances := e.log[e.me]
	for i := len(instances) - 1; i >= 0; i-- { // loop through instances of replica R (backwards)
		if e.interferenceChecker(cmd, instances[i].Command) {
			depsR[LogIndex{ Replica: e.me, Index: i }] = 1
			if instances[i].Seq > maxSeq {
				maxSeq = instances[i].Seq
			}
			break // only find latest instance that interferes
		}
	}
	maxSeq += 1
	// reply with union of dependencies & new max seq #
	reply.Deps = unionMaps(depsL, depsR)
	reply.Seq = maxSeq
	reply.Success = true
}

func (e *EPaxos) sendPreAccept(server int, args *PreAcceptArgs, reply *PreAcceptReply) bool {
	ok := e.peers[server].Call("EPaxos.PreAccept", args, reply)
	return ok
}

func (e *EPaxos) broadcastAccept(peer int, cmd interface{}, deps map[LogIndex]int, seq int, wg *sync.WaitGroup, fail chan bool, responses *map[int]AcceptReply, responsesLock *sync.Mutex)  {
	e.lock.Lock()
	defer e.lock.Unlock()
	for !e.killed() {
		args := AcceptArgs{ Command: cmd, Deps: deps, Seq: seq, Ballot: Ballot{ BallotNum: e.myBallot, ReplicaNum: e.me } }
		reply := AcceptReply{}
		e.lock.Unlock()
		ok := e.sendAccept(peer, &args, &reply)
		e.lock.Lock()
		if ok {
			if reply.Success == false {
				fail <- false
			}
			responsesLock.Lock()
			(*responses)[peer] = reply
			responsesLock.Unlock()
			return
		} else { // keep trying if not ok
			e.lock.Unlock()
			time.Sleep(10 * time.Millisecond)
			e.lock.Lock()
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
	
	e.log[e.me][instanceInd].Status = ACCEPTED
	reply.Success = true
	
}

func (e *EPaxos) sendAccept(server int, args *AcceptArgs, reply *AcceptReply) bool {
	ok := e.peers[server].Call("EPaxos.Accept", args, reply)
	return ok
}

func (e *EPaxos) broadcastCommit(peer int, cmd interface{}, deps map[LogIndex]int, seq int) {
	e.lock.Lock()
	defer e.lock.Unlock()
	for !e.killed() {
		args := CommitArgs{ Command: cmd, Deps: deps, Seq: seq, Ballot: Ballot{ BallotNum: e.myBallot, ReplicaNum: e.me } }
		reply := CommitReply{}
		e.lock.Unlock()
		ok := e.sendCommit(peer, &args, &reply)
		e.lock.Lock()
		if ok {
			return
		} else { // keep trying if not ok
			e.lock.Unlock()
			time.Sleep(10 * time.Millisecond)
			e.lock.Lock()
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
	
	e.log[e.me][instanceInd].Status = COMMITTED
	reply.Success = true
}

func (e *EPaxos) sendCommit(server int, args *CommitArgs, reply *CommitReply) bool {
	ok := e.peers[server].Call("EPaxos.Commit", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (e *EPaxos) Start(command interface{}) int {
	instanceNum := -1

	// Your code here (3B).
	if e.killed() == true {
		return instanceNum
	}

	e.lock.Lock()
	defer e.lock.Unlock()

	instanceNum = e.nextIndex
	go e.processRequest(command)

	return instanceNum
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

	// enableLogging()
	// debug(topicInfo, rf.me, "Killing peer, T%v", rf.currentTerm)
	// disableLogging()
}

// func (e *EPaxos) killed() bool {
// 	z := atomic.LoadInt32(&e.dead)
// 	return z == 1
// }

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
	// enableLogging()

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

	// e.conflicts = e.makeConflictsMap()

	// print out what state we are starting at
	// debug(
	// 	topicInfo,
	// 	me,
	// 	"Starting at T%v, LLI: %v, CI: %v",
	// 	rf.currentTerm,
	// 	rf.log.lastLogIndex(),
	// 	rf.commitIndex,
	// )

	return e
}
