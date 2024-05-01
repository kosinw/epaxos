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

	"6.5840/labrpc"
)

// func (le LogEntry) String() string {
// 	return fmt.Sprintf("{T:%v I:%v}", le.Term, le.Index)
// }

// A Go type representing the persistent log.
//
// The zeroth index in the log actually tells us the last included index
// and last included term of the snapshot and is not included as part of the log.
// type Log []LogEntry

type PaxosLog [][]Instance

// func makeLog() Log { return Log{LogEntry{Term: 0, Index: 0, Command: 0}} } // [TODO] MODIFY THIS FUNC

// func (lg Log) lastIncludedIndex() int { return lg[0].Index }
// func (lg Log) lastIncludedTerm() int  { return lg[0].Term }
// func (lg Log) lastIndex() int         { return len(lg) - 1 }
// func (lg Log) lastLogIndex() int      { return lg.logIndex(lg.lastIndex()) }
// func (lg Log) lastLogTerm() int       { return lg[lg.lastIndex()].Term }
// func (lg Log) logIndex(index int) int { return index + lg.lastIncludedIndex() }
// func (lg Log) index(logIndex int) int { return logIndex - lg.lastIncludedIndex() }

// func (lg Log) entry(logIndex int) *LogEntry { return &lg[lg.index(logIndex)] }
// func (lg Log) size() int                    { return lg.lastLogIndex() + 1 }

// func (lg *Log) drop(logIndex int) { *lg = (*lg)[:lg.index(logIndex)] }

// // Compacts the log given a snapshot's last included index and term.
// func (lg *Log) compact(lastIncludedIndex int, lastIncludedTerm int) {
// 	if lastIncludedIndex > lg.lastLogIndex() || lg.entry(lastIncludedIndex).Term != lastIncludedTerm {
// 		*lg = (*lg)[:1]
// 		(*lg)[0].Index = lastIncludedIndex
// 		(*lg)[0].Term = lastIncludedTerm
// 	} else {
// 		*lg = (*lg)[lg.index(lastIncludedIndex):]
// 		(*lg)[0].Term = lastIncludedTerm
// 	}
// }

// func (lg *Log) append(term int, command interface{}) int {
// 	*lg = append(*lg, LogEntry{Index: lg.size(), Term: term, Command: command})
// 	return lg.lastLogIndex()
// }

// // Finds the most recent log entry with given term.
// // ok=true if the log contains that term, otherwise ok=false
// // logIndex is only valid when ok=true
// func (lg Log) mostRecent(term int) (ok bool, logIndex int) {
// 	ok = false
// 	logIndex = -1

// 	for i := lg.lastIndex(); i > 0; i-- {
// 		if lg[i].Term == term {
// 			ok = true
// 			logIndex = lg.logIndex(i)
// 			return
// 		}
// 	}

// 	return
// }

// A Go object implementing a single EPaxos peer.
type EPaxos struct {
	lock  sync.Mutex          // Lock to protect shared access to this peer's state
	peers []*labrpc.ClientEnd // RPC end points of all peers
	me    int                 // this peer's index into peers[]
	dead  int32               // set by Kill()
	// numPeers  int

	applyCh       chan<- Instance // used to send client messages
	log           PaxosLog        // log -- nested array indexed by replica number, instance number
	numInstances  int             // number of instances per replica? maybe unneeded
	instanceIndex []int           // instance numbers of each replica

	lastApplied []int      // index of highest log entry known to be applied to state machine
	status      [][]Status // status of every instance (see common.go)

}

func (e *EPaxos) majority() int {
	return (e.numPeers() / 2) + 1
}

func (e *EPaxos) numPeers() int {
	return len(e.peers)
}

// return currentTerm and whether this server
// believes it is the leader.
// [TODO] what should GetState return now?
func (e *EPaxos) GetState() (int, bool) {
	// Your code here (3A).
	e.lock.Lock()
	defer e.lock.Unlock()

	// return term, isleader
	return 0, false
}

func (e *EPaxos) processRequest(cmd interface{}) {
	// increment instance # for replica L (this replica)
	e.instanceIndex[e.me]++

}

// PreAccept RPC handler.
func (e *EPaxos) PreAccept(args *PreAcceptArgs, reply *PreAcceptReply) {

}

func (e *EPaxos) sendPreAccept(server int, args *PreAcceptArgs, reply *PreAcceptReply) bool {
	ok := e.peers[server].Call("EPaxos.PreAccept", args, reply)
	return ok
}

func (e *EPaxos) Accept(args *AcceptArgs, reply *AcceptReply) {

}

func (e *EPaxos) sendAccept(server int, args *AcceptArgs, reply *AcceptReply) bool {
	ok := e.peers[server].Call("EPaxos.Accept", args, reply)
	return ok
}

func (e *EPaxos) Commit(args *CommitArgs, reply *CommitReply) {

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
func (e *EPaxos) Start(command interface{}) LogIndex {
	e.lock.Lock()
	defer e.lock.Unlock()

	return LogIndex{0, 0}
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
	enableLogging()

	e := new(EPaxos)

	e.lock = sync.Mutex{}
	e.peers = peers
	e.me = me
	e.dead = 0

	e.applyCh = applyCh
	// e.log = makeLog()

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
