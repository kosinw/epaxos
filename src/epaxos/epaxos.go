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
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
)

// Constants for important timeout values.
const (
	ElectionTimeoutMin = 400
	ElectionTimeoutMax = 600
	HeartbeatTimeout   = 50
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// REPLACE WITH INSTANCe
type LogEntry struct {
	Term    int         // term when entry was received by leader
	Index   int         // index that entry will appear at if it's ever committed
	Command interface{} // state machine command
}

func (le LogEntry) String() string {
	return fmt.Sprintf("{T:%v I:%v}", le.Term, le.Index)
}

// A Go type representing the persistent log.
//
// The zeroth index in the log actually tells us the last included index
// and last included term of the snapshot and is not included as part of the log.
type Log []LogEntry

type PaxosLog [][]Instance

func makeLog() Log { return Log{LogEntry{Term: 0, Index: 0, Command: 0}} }

func (lg Log) lastIncludedIndex() int { return lg[0].Index }
func (lg Log) lastIncludedTerm() int  { return lg[0].Term }
func (lg Log) lastIndex() int         { return len(lg) - 1 }
func (lg Log) lastLogIndex() int      { return lg.logIndex(lg.lastIndex()) }
func (lg Log) lastLogTerm() int       { return lg[lg.lastIndex()].Term }
func (lg Log) logIndex(index int) int { return index + lg.lastIncludedIndex() }
func (lg Log) index(logIndex int) int { return logIndex - lg.lastIncludedIndex() }

func (lg Log) entry(logIndex int) *LogEntry { return &lg[lg.index(logIndex)] }
func (lg Log) size() int                    { return lg.lastLogIndex() + 1 }

func (lg *Log) drop(logIndex int) { *lg = (*lg)[:lg.index(logIndex)] }

// Compacts the log given a snapshot's last included index and term.
func (lg *Log) compact(lastIncludedIndex int, lastIncludedTerm int) {
	if lastIncludedIndex > lg.lastLogIndex() || lg.entry(lastIncludedIndex).Term != lastIncludedTerm {
		*lg = (*lg)[:1]
		(*lg)[0].Index = lastIncludedIndex
		(*lg)[0].Term = lastIncludedTerm
	} else {
		*lg = (*lg)[lg.index(lastIncludedIndex):]
		(*lg)[0].Term = lastIncludedTerm
	}
}

func (lg *Log) append(term int, command interface{}) int {
	*lg = append(*lg, LogEntry{Index: lg.size(), Term: term, Command: command})
	return lg.lastLogIndex()
}

// Finds the most recent log entry with given term.
// ok=true if the log contains that term, otherwise ok=false
// logIndex is only valid when ok=true
func (lg Log) mostRecent(term int) (ok bool, logIndex int) {
	ok = false
	logIndex = -1

	for i := lg.lastIndex(); i > 0; i-- {
		if lg[i].Term == term {
			ok = true
			logIndex = lg.logIndex(i)
			return
		}
	}

	return
}

// Current state the Raft peer is in.
type state string

const (
	stateFollower  state = "FOLLOWER"
	stateCandidate state = "CANDIDATE"
	stateLeader    state = "LEADER"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	lock      sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// State not specified by paper.
	state              state           // current status of peer (one of {follower, leader, candidate})
	electionTimestamp  time.Time       // starting timestamp for next election timeout
	broadcastTimestamp time.Time       // starting timestamp for next log replication broadcast
	commitLock         *sync.Cond      // lock to protect commitIndex
	applyCh            chan<- ApplyMsg // used to send client messages

	// Figure 2 state.
	currentTerm int   // latest term server has seen (persistent)
	votedFor    int   // candidateId that received in current vote (persistent)
	log         Log   // log (persistent)
	commitIndex int   // index of highest log entry known to be committed
	lastApplied int   // index of highest log entry known to be applied to state machine
	nextIndex   []int // for each peer, index of next log entry which must be sent
	matchIndex  []int // for each peer, index of highest log entry known to be replicated

}

type EPaxos struct {
	lock      sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	numPeers  int
	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// State not specified by paper.
	state              state           // current status of peer (one of {follower, leader, candidate})
	electionTimestamp  time.Time       // starting timestamp for next election timeout
	broadcastTimestamp time.Time       // starting timestamp for next log replication broadcast
	commitLock         *sync.Cond      // lock to protect commitIndex
	applyCh            chan<- ApplyMsg // used to send client messages

	// Figure 2 state.
	currentTerm int        // latest term server has seen (persistent)
	votedFor    int        // candidateId that received in current vote (persistent)
	log         PaxosLog   // log (persistent)
	commitIndex int        // index of highest log entry known to be committed
	lastApplied []int      // index of highest log entry known to be applied to state machine
	status      [][]Status //Status of every instance (see common.go)
	nextIndex   []int      // for each peer, index of next log entry which must be sent
	matchIndex  []int      // for each peer, index of highest log entry known to be replicated

}

func (rf *Raft) majority() int {
	return (rf.numPeers() / 2) + 1
}

func (rf *Raft) numPeers() int {
	return len(rf.peers)
}

// Starts election on peer. Must be called in critical section after `canStartElection`.
//
// rf.mu must be held.
func (rf *Raft) becomeCandidate() {
	debug(topicVote, rf.me, "Becoming candidate for T%v", rf.currentTerm+1)

	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.state = stateCandidate

	rf.persist(nil)

	go rf.broadcastRequestVotes(rf.currentTerm, rf.log.lastLogIndex(), rf.log.lastLogTerm())

	return
}

// Converts peer to a follower with newTerm, also resets election timer.
func (rf *Raft) becomeFollower(newTerm int) {
	assert(
		rf.currentTerm < newTerm,
		rf.me,
		"Expected currentTerm (%v) < newTerm (%v)",
		rf.currentTerm,
		newTerm,
	)

	debug(topicInfo, rf.me, "Becoming follower, moving from T%v -> T%v", rf.currentTerm, newTerm)

	rf.state = stateFollower
	rf.currentTerm = newTerm
	rf.votedFor = -1

	rf.persist(nil)
}

func (rf *Raft) resetElectionTimer() {
	debug(topicTimer, rf.me, "Reset election timer")
	rf.electionTimestamp = time.Now()
}

func (rf *Raft) resetBroadcastTimer() {
	debug(topicTimer, rf.me, "Reset heartbeat timer")
	rf.broadcastTimestamp = time.Now()
}

func (rf *Raft) chooseElectionTimeout() time.Duration {
	v := ElectionTimeoutMin + (rand.Int63() % (ElectionTimeoutMax - ElectionTimeoutMin))
	timeout := time.Duration(v) * time.Millisecond
	debug(topicTimer, rf.me, "Election timeout is now %v", timeout)
	rf.resetElectionTimer()
	return timeout
}

// Checks to see if election can be started.
// Resets election timeout stamp if timeout has happened.
func (rf *Raft) canStartElection(timeout time.Duration) bool {
	// 1. Check if the peer is not currently a leader (otherwise bail).
	// 2. Check if the election timeout has elapsed (otherwise bail).
	if rf.state == stateLeader {
		return false
	}

	if time.Since(rf.electionTimestamp) < timeout {
		return false
	}

	return true
}

func (rf *Raft) canStartBroadcast() bool {
	if rf.state != stateLeader {
		return false
	}

	if time.Since(rf.broadcastTimestamp) < (HeartbeatTimeout * time.Millisecond) {
		return false
	}

	return true
}

// Converts peer to leader status (and does a bunch of leader initialization) code.
//
// rf.mu must be held before calling this procedure.
func (rf *Raft) becomeLeader() {
	debug(topicLeader, rf.me, "Becoming leader for T%v", rf.currentTerm)

	rf.state = stateLeader

	// Reset state after election.
	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.log.lastLogIndex() + 1
	}

	for i := range rf.matchIndex {
		rf.matchIndex[i] = 0
	}

	// Broadcast AppendEntries RPC to all peers.
	rf.broadcastAppendEntries()
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (3A).
	rf.lock.Lock()
	defer rf.lock.Unlock()

	term := rf.currentTerm
	isleader := rf.state == stateLeader

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist(snapshot []byte) {
	// Your code here (3C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	if err := e.Encode(rf.currentTerm); err != nil {
		panic(err)
	}

	if err := e.Encode(rf.votedFor); err != nil {
		panic(err)
	}

	if err := e.Encode(rf.log); err != nil {
		panic(err)
	}

	raftstate := w.Bytes()

	if snapshot == nil {
		rf.persister.Save(raftstate, rf.persister.ReadSnapshot())
	} else {
		rf.persister.Save(raftstate, snapshot)
	}

	debug(topicPersist, rf.me, "Write state, T%v (VF: %v N: %v)", rf.currentTerm, rf.votedFor, rf.log.size())

	if snapshot != nil {
		debug(topicSnap, rf.me, "Write snapshot, T%v (total bytes: %v)", rf.currentTerm, len(snapshot))
	}
}

// restore previously persisted state.
func (rf *Raft) readPersist() {
	state := rf.persister.ReadRaftState()

	if state == nil || len(state) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	r := bytes.NewBuffer(state)
	d := labgob.NewDecoder(r)

	var currentTerm int
	var votedFor int
	var log []LogEntry

	if err := d.Decode(&currentTerm); err != nil {
		panic(err)
	}

	if err := d.Decode(&votedFor); err != nil {
		panic(err)
	}

	if err := d.Decode(&log); err != nil {
		panic(err)
	}

	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.log = log
	rf.commitIndex = rf.log.lastIncludedIndex()
	rf.lastApplied = rf.log.lastIncludedIndex()

	debug(topicPersist, rf.me, "Read state (T%v VF: %v L: %v)", rf.currentTerm, rf.votedFor, rf.log)
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.lock.Lock()
	defer rf.lock.Unlock()

	debug(topicClient, rf.me, "Snapshot at log index %v", index)

	if index <= rf.log.lastIncludedIndex() {
		debug(topicClient, rf.me, "Ignoring snapshot, reason: we have a better one")
		return
	}

	rf.log.compact(index, rf.log.entry(index).Term)
	debug(topicClient, rf.me, "Compacting log (LII: %v)", rf.log.lastIncludedIndex())
	debug(topicClient, rf.me, "Log: %v", rf.log)
	rf.persist(snapshot)
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// Checks if candidate (requester) log is at least as up-to-date as the peer's log.
func (rf *Raft) candidateUpToDate(lastLogIndex int, lastLogTerm int) bool {
	// Raft determines which of two logs is more up-to-date
	// by comparing the index and term of the last entries in the
	// logs. If the logs have last entries with different terms, then
	// the log with the later term is more up-to-date. If the logs
	// end with the same term, then whichever log is longer is
	// more up-to-date.

	if lastLogTerm == rf.log.lastLogTerm() {
		return lastLogIndex >= rf.log.lastLogIndex()
	} else {
		return lastLogTerm > rf.log.lastLogTerm()
	}
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.lock.Lock()
	defer rf.lock.Unlock()

	debug(
		topicVote,
		rf.me,
		"Request vote S%v <- S%v, T%v (LLI: %v LLT: %v)",
		rf.me,
		args.CandidateId,
		args.Term,
		args.LastLogIndex,
		args.LastLogTerm,
	)

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower (§5.1)
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	// Receiver implementation:
	// 1. Reply false if term < currentTerm (§5.1)
	// 2. If votedFor is null or candidateId, and candidate’s log is at
	// 	  least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		debug(topicWarn, rf.me, "Responding with vote NO, reason: T%v < T%v", args.Term, rf.currentTerm)
		return
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		reply.VoteGranted = rf.candidateUpToDate(args.LastLogIndex, args.LastLogTerm)
		if reply.VoteGranted {
			// NOTE(kosinw): Voting for a candidate should reset your election timer
			// 			     This covers the edge case where a peer updates its term BUT rejects
			//				 a vote from peer B. Then this peer votes for peer C but still has
			//				 a short election timer.

			rf.votedFor = args.CandidateId

			rf.persist(nil)

			rf.resetElectionTimer()

			debug(topicVote, rf.me, "Responding with vote YES")
		} else {
			debug(topicVote, rf.me, "Responsding with vote NO (voted for S%v)", rf.votedFor)
		}

		return
	}

	reply.VoteGranted = false
	debug(topicVote, rf.me, "Responding with vote NO")
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	debug(
		topicVote,
		rf.me,
		"Request vote S%v -> S%v, T%v (LLI: %v LLT: %v)",
		rf.me,
		server,
		args.Term,
		args.LastLogIndex,
		args.LastLogTerm,
	)

	start := debugTimestamp()
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower (§5.1)
	if ok {
		rf.lock.Lock()
		if reply.Term > rf.currentTerm {
			rf.becomeFollower(reply.Term)
		}
		rf.lock.Unlock()
	} else {
		debug(topicWarn, rf.me, "Network timeout for request vote S%v -> S%v (%v)", rf.me, server, start)
	}

	return ok
}

// AppendEntries RPC arguments structure.
type AppendEntriesArgs struct {
	Term         int        // leader's term
	LeaderId     int        // so followers can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of PrevLogIndex entry
	Entries      []LogEntry // log entries to store
	LeaderCommit int        // leader's commit index
}

// AppendEntries RPC reply structure.
type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching PrevLogIndex and PrevLogTerm

	// Log inconsistency optimization (3C)
	XTerm  int // term in the conflicting log entry
	XIndex int // index of the first entry with that term
	XLen   int // log length
}

// Given a previous log index and term, determine if log is inconsitent.
func (rf *Raft) hasInconsistentLog(prevLogIndex int, prevLogTerm int) bool {
	return prevLogIndex > rf.log.lastLogIndex() || rf.log.entry(prevLogIndex).Term != prevLogTerm
}

// AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (3B)
	rf.lock.Lock()
	defer rf.lock.Unlock()

	debug(topicLog, rf.me, "Append entries S%v <- S%v, T%v", rf.me, args.LeaderId, args.Term)

	if len(args.Entries) > 0 {
		debug(topicLog, rf.me, "From %v to %v", args.Entries[0], args.Entries[len(args.Entries)-1])
	}

	reply.Term = rf.currentTerm
	reply.Success = false

	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower (§5.1)
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	// Receiver implementation:
	// 1. Reply false if term < currentTerm (§5.1)
	// 2. Reply false if log doesn’t contain an entry at prevLogIndex
	// 	  whose term matches prevLogTerm (§5.3)
	// 3. If an existing entry conflicts with a new one (same index
	//    but different terms), delete the existing entry and all that
	//    follow it (§5.3)
	// 4. Append any new entries not already in the log
	// 5. If leaderCommit > commitIndex, set commitIndex =
	//    min(leaderCommit, index of last new entry)

	if args.Term < rf.currentTerm {
		reply.Success = false
		debug(topicLog, rf.me, "Append entries DENY, reason: T%v < T%v", args.Term, rf.currentTerm)
		return
	}

	// We are dealing with current leader so we should reset election timer
	rf.resetElectionTimer()

	// NOTE(kosinw): Stupid edge case where we receive old requests for AppendEntries
	// after we have received an InstallSnapshot and compacted our log.
	if args.PrevLogIndex < rf.log.lastIncludedIndex() {
		reply.Success = true
		return
	}

	if rf.hasInconsistentLog(args.PrevLogIndex, args.PrevLogTerm) {
		reply.Success = false

		if args.PrevLogIndex > rf.log.lastLogIndex() {
			reply.XTerm = -1
			reply.XIndex = -1
			reply.XLen = rf.log.size()
		} else {
			var logIndex int

			term := rf.log.entry(args.PrevLogIndex).Term

			for logIndex = args.PrevLogIndex; logIndex >= rf.log.lastIncludedIndex(); logIndex-- {
				if rf.log.entry(logIndex).Term != term {
					logIndex++
					break
				}
			}

			reply.XTerm = term
			reply.XIndex = logIndex
			reply.XLen = -1
		}

		debug(
			topicLog,
			rf.me,
			"Append entries DENY, reason: LI T%v (XT: %v XI: %v XL: %v)",
			rf.currentTerm,
			reply.XTerm,
			reply.XIndex,
			reply.XLen,
		)
		return
	}

	// Drop log entries if necessary.
	dropIndex := rf.log.size()

	for i := range args.Entries {
		logIndex := args.Entries[i].Index
		logTerm := args.Entries[i].Term

		if logIndex > rf.log.lastLogIndex() {
			continue
		}

		if logTerm != rf.log.entry(logIndex).Term {
			dropIndex = min(dropIndex, logIndex)
		}
	}

	// Truncate log
	if dropIndex < rf.log.size() {
		debug(topicDrop, rf.me, "Dropping log entries %v to %v", dropIndex, rf.log.lastLogIndex())
		rf.log.drop(dropIndex)
	}

	// Append new log entries not already in log.
	for _, entry := range args.Entries {
		if entry.Index <= rf.log.lastLogIndex() {
			continue
		}

		debug(topicLog, rf.me, "Appending log entry %v at index %v, T%v", entry.Command, entry.Index, entry.Term)
		rf.log.append(entry.Term, entry.Command)
	}

	// Only persist if new entries were added
	if len(args.Entries) > 0 {
		rf.persist(nil)
	}

	if args.LeaderCommit > rf.commitIndex {
		commitIndex := args.LeaderCommit

		if len(args.Entries) > 0 {
			lastEntryIndex := args.Entries[len(args.Entries)-1].Index
			commitIndex = min(commitIndex, lastEntryIndex)
		}

		debug(topicCommit, rf.me, "Updated commit index %v -> %v", rf.commitIndex, commitIndex)
		rf.commitIndex = commitIndex

		// Wake up waiting goroutines.
		rf.commitLock.Broadcast()
	}

	reply.Success = true
	debug(topicLog, rf.me, "Append entries OK")
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	debug(
		topicLeader,
		rf.me,
		"Append entries S%v -> S%v, T%v (CI: %v, NI: %v, N: %v)",
		args.LeaderId,
		server,
		args.Term,
		args.LeaderCommit,
		args.PrevLogIndex+1,
		len(args.Entries),
	)

	start := debugTimestamp()

	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower (§5.1)
	if ok {
		rf.lock.Lock()
		if reply.Term > rf.currentTerm {
			rf.becomeFollower(reply.Term)
		}
		rf.lock.Unlock()
	} else {
		debug(topicWarn, rf.me, "Network timeout for append entries S%v -> S%v (%v)", args.LeaderId, server, start)
	}

	return ok
}

// InstallSnapshot RPC arguments structure.
type InstallSnapshotArgs struct {
	Term              int    // leader's term
	LeaderId          int    // so followers can redirect clients
	LastIncludedIndex int    // the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int    // term of lastIncludedIndex
	Data              []byte // raw bytes of the snapshot
}

// InstallSnapshot RPC reply structure.
type InstallSnapshotReply struct {
	Term int // currentTerm, for leader to update itself
}

// InstallSnapshot RPC handler.
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	// Your code here (3D)
	rf.lock.Lock()
	defer rf.lock.Unlock()

	debug(topicSnap, rf.me, "Install snapshot S%v <- S%v, T%v", rf.me, args.LeaderId, args.Term)

	reply.Term = rf.currentTerm

	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower (§5.1)
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	// Receiver implementation:
	// 1. Reply immediately if term < currentTerm
	// 2. Create new snapshot file if first chunk (offset is 0)
	// 3. Write data into snapshot file at given offset
	// 4. Reply and wait for more data chunks if done is false
	// 5. Save snapshot file, discard any existing or partial snapshot
	// 	  with a smaller index
	// 6. If existing log entry has same index and term as snapshot’s
	// 	  last included entry, retain log entries following it and reply
	// 7. Discard the entire log
	// 8. Reset state machine using snapshot contents (and load
	// 	  snapshot’s cluster configuration)
	if args.Term < rf.currentTerm {
		debug(topicSnap, rf.me, "Install snapshot DENY, reason: T%v < T%v", args.Term, rf.currentTerm)
		return
	}

	// We are dealing with current leader so we should reset election timer
	rf.resetElectionTimer()

	// NOTE(kosinw): Another really stupid stupid bug, can be cause if InstallSnapshot
	// was sent multiple times, just check if the current snapshot we have is better
	// than the one we are about to install
	if args.LastIncludedIndex <= rf.log.lastIncludedIndex() {
		debug(topicSnap, rf.me, "Install snapshot DENY, reason: we have a better one")
		return
	}

	// Comapct log and persist snapshot.
	rf.log.compact(args.LastIncludedIndex, args.LastIncludedTerm)
	debug(topicSnap, rf.me, "Compacting log (LII: %v)", rf.log.lastIncludedIndex())
	debug(topicSnap, rf.me, "Log: %v", rf.log)

	rf.persist(args.Data)

	// Restore commitIndex and lastApplied.
	rf.commitIndex = max(rf.commitIndex, rf.log.lastIncludedIndex())
	rf.lastApplied = max(rf.lastApplied, rf.log.lastIncludedIndex())

	if rf.lastApplied > rf.log.lastIncludedIndex() {
		debug(topicSnap, rf.me, "Wont send snapshot to client, LA: %v > LII: %v", rf.lastApplied, rf.log.lastIncludedIndex())
		return // this snapshot is old

	}
	debug(topicSnap, rf.me, "Sending snapshot to client, term: %v index: %v", args.LastIncludedTerm, args.LastIncludedIndex)

	// Send snapshot to client.
	rf.lock.Unlock()
	rf.applyCh <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}

	debug(topicSnap, rf.me, "Install snapshot finished, replying to S%v", args.LeaderId)
	rf.lock.Lock()
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	debug(
		topicLeader,
		rf.me,
		"Install snapshot S%v -> S%v, T%v (LII: %v, LIT: %v, total bytes: %v)",
		args.LeaderId,
		server,
		args.Term,
		args.LastIncludedIndex,
		args.LastIncludedTerm,
		len(args.Data),
	)

	start := debugTimestamp()

	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)

	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower (§5.1)
	if ok {
		rf.lock.Lock()
		if reply.Term > rf.currentTerm {
			rf.becomeFollower(reply.Term)
		}
		rf.lock.Unlock()
	} else {
		debug(topicWarn, rf.me, "Network timeout for install snapshot S%v -> S%v (%v)", args.LeaderId, server, start)
	}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false

	// Your code here (3B).
	if rf.killed() == true {
		return index, term, isLeader
	}

	rf.lock.Lock()
	defer rf.lock.Unlock()

	isLeader = rf.state == stateLeader
	term = rf.currentTerm

	if !isLeader {
		return index, term, isLeader
	}

	index = rf.log.append(term, command)
	debug(topicClient, rf.me, "Appending log entry %v at index %v, T%v", command, index, term)

	rf.persist(nil)

	// NOTE(kosinw): Broadcast append entries here to potentially increase throughput?
	rf.broadcastAppendEntries()

	return index, term, isLeader
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
func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.lock.Lock()
	defer rf.lock.Unlock()

	atomic.StoreInt32(&rf.dead, 1)

	enableLogging()
	debug(topicInfo, rf.me, "Killing peer, T%v", rf.currentTerm)
	disableLogging()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// Must be ran in a goroutine, will try to acquire rf.mu for critical sections.
func (rf *Raft) replicateLog(peer int) {
	rf.lock.Lock()

	// Check if we are still currently the leader.
	if rf.state != stateLeader {
		debug(topicTrace, rf.me, "No longer leader, cancelling append entries to S%v", peer)
		rf.lock.Unlock()
		return
	}

	nextIndex := rf.nextIndex[peer]

	term := rf.currentTerm
	leaderId := rf.me
	prevLogIndex := nextIndex - 1

	// If prevLogIndex is too far behind, we have to send a snapshot instead
	if nextIndex < rf.log.lastIncludedIndex()+1 {
		debug(
			topicLeader,
			rf.me,
			"Too far behind S%v -> S%v, T%v (NI: %v, LII: %v)",
			rf.me,
			peer,
			term,
			nextIndex,
			rf.log.lastIncludedIndex(),
		)

		args := InstallSnapshotArgs{
			Term:              term,
			LeaderId:          leaderId,
			LastIncludedIndex: rf.log.lastIncludedIndex(),
			LastIncludedTerm:  rf.log.lastIncludedTerm(),
			Data:              rf.persister.ReadSnapshot(),
		}

		reply := InstallSnapshotReply{}

		rf.lock.Unlock()

		// If network failure, we should terminate.
		if !rf.sendInstallSnapshot(peer, &args, &reply) {
			return
		}

		rf.lock.Lock()
		defer rf.lock.Unlock()

		// If successful: update nextIndex for follower
		if term == rf.currentTerm {
			rf.nextIndex[peer] = rf.log.lastIncludedIndex() + 1
			rf.matchIndex[peer] = rf.log.lastIncludedIndex()

			debug(
				topicLeader,
				rf.me,
				"Install snapshot OK from S%v (NI: %v MI: %v)",
				peer,
				rf.nextIndex[peer],
				rf.matchIndex[peer],
			)
		}

		return
	}

	prevLogTerm := rf.log.entry(prevLogIndex).Term
	entries := make([]LogEntry, 0)
	leaderCommit := rf.commitIndex

	// If last log index ≥ nextIndex for a follower: send
	// AppendEntries RPC with log entries starting at nextIndex
	if rf.log.lastLogIndex() >= nextIndex {
		for i := nextIndex; i < rf.log.size(); i++ {
			entries = append(entries, *rf.log.entry(i))
		}
	}

	rf.lock.Unlock()

	args := AppendEntriesArgs{
		Term:         term,
		LeaderId:     leaderId,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: leaderCommit,
	}

	reply := AppendEntriesReply{}

	// If network failure, we should terminate.
	if !rf.sendAppendEntries(peer, &args, &reply) {
		return
	}

	// If successful: update nextIndex and matchIndex for follower (§5.3)
	// If failure because of log inconsistency: decrement nextIndex and retry (§5.3)
	rf.lock.Lock()
	defer rf.lock.Unlock()

	// Check for term confusion.
	if term != rf.currentTerm {
		debug(topicTrace, rf.me, "Term confusion for append entries to S%v", peer)
		return
	}

	if reply.Success {
		rf.nextIndex[peer] = nextIndex + len(entries)
		rf.matchIndex[peer] = prevLogIndex + len(entries)

		debug(
			topicLeader,
			rf.me,
			"Append entries OK from S%v (NI: %v MI: %v)",
			peer,
			rf.nextIndex[peer],
			rf.matchIndex[peer],
		)

		// If there exists an N such that N > commitIndex, a majority
		// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
		// set commitIndex = N (§5.3, §5.4).
		for _, entry := range entries {
			N := entry.Index

			if N <= rf.commitIndex {
				continue
			}

			count := 1

			for _, matchIndex := range rf.matchIndex {
				if matchIndex >= N {
					count += 1
				}
			}

			if count >= rf.majority() && rf.log.entry(N).Term == rf.currentTerm {
				debug(topicCommit, rf.me, "Updated commit index %v -> %v", rf.commitIndex, N)
				rf.commitIndex = N

				// Wake up waiting goroutines.
				rf.commitLock.Broadcast()
			}
		}

		return
	} else {
		// Check if args.Term != reply.Term for invalid terms
		if args.Term != reply.Term {
			debug(topicLeader, rf.me, "Append entries DENY from S%v (bad terms)", peer)
			return
		}

		// Log inconsistency case.
		// Case 1: leader doesn't have XTerm:
		//   nextIndex = XIndex
		// Case 2: leader has XTerm:
		//   nextIndex = leader's last entry for XTerm
		// Case 3: follower's log is too short:
		//   nextIndex = XLen

		hasTerm, logIndex := rf.log.mostRecent(reply.XTerm)

		if reply.XLen != -1 {
			rf.nextIndex[peer] = reply.XLen
		} else if hasTerm {
			rf.nextIndex[peer] = logIndex
		} else {
			rf.nextIndex[peer] = reply.XIndex
		}
	}
}

// Spawns a goroutine for each peer to append entries.
//
// rf.mu must be held before calling this procedure.
func (rf *Raft) broadcastAppendEntries() {
	rf.resetBroadcastTimer()

	for i := 0; i < rf.numPeers(); i++ {
		if i == rf.me {
			continue
		}

		go rf.replicateLog(i)
	}
}

// Spawns a goroutine for each peer to request a vote for the `currentTerm`.
// Then waits for a majority of peers to give a yes vote for this candidate.
// If majority votes are received in this term then the peer is promoted to leader.
//
// This procedure must be started in its own separate goroutine.
func (rf *Raft) broadcastRequestVotes(currentTerm int, lastLogIndex int, lastLogTerm int) {
	votes := 1
	yesVotes := 1
	votesCond := sync.NewCond(new(sync.Mutex))

	// Spawn a bunch of goroutines that will request for votes
	for i := 0; i < rf.numPeers(); i++ {
		if i == rf.me {
			continue
		}

		go func(i int) {
			defer votesCond.Broadcast()

			args := RequestVoteArgs{
				Term:         currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			reply := RequestVoteReply{}

			// Keep resending RequestVote RPC until it goes through network.
			for !rf.sendRequestVote(i, &args, &reply) {
				if rf.killed() {
					return
				}
			}

			votesCond.L.Lock()

			if reply.VoteGranted {
				debug(topicVote, rf.me, "Request vote YES from S%v", i)
				yesVotes += 1
			} else {
				debug(topicVote, rf.me, "Request vote NO from S%v", i)
			}

			votes += 1
			votesCond.L.Unlock()

		}(i)
	}

	// Keep waiting until this thread is killed or we receive majority vote
	votesCond.L.Lock()
	defer votesCond.L.Unlock()

	for rf.killed() == false {
		debug(topicVote, rf.me, "Currently received %v YES votes for T%v", yesVotes, currentTerm)

		noVotes := votes - yesVotes

		// If we have majority no votes, we are done.
		if noVotes >= rf.majority() {
			break
		}

		// If we have majority yes votes, we are done.
		if yesVotes >= rf.majority() {
			break
		}

		// If we have all the votes in, we are done.
		if votes >= rf.numPeers() {
			break
		}

		votesCond.Wait()
	}

	// Check if we won the majority vote
	wonMajority := yesVotes >= rf.majority()
	debug(topicVote, rf.me, "Finished waiting for votes (total %v) (yes %v)", votes, yesVotes)

	// Now check if we are still in the correct term + have candidacy
	rf.lock.Lock()
	isCorrectTerm := (currentTerm == rf.currentTerm)
	isCandidate := (stateCandidate == rf.state)

	if isCorrectTerm && isCandidate && wonMajority {
		// We are leader now!
		rf.becomeLeader()
	}
	rf.lock.Unlock()
}

// Long-running routine that periodically checks if election timeout has elapsed.
// Once timeout has elapsed, peer is promoted to a candidate and begins an election.
func (rf *Raft) electionTicker() {
	// Choose a random election timeout
	rf.lock.Lock()
	timeout := rf.chooseElectionTimeout()
	rf.lock.Unlock()

	for rf.killed() == false {
		// Pause for a little bit (1ms) before checking if timeout has elapsed.
		time.Sleep(1 * time.Millisecond)

		// This is a critical section since the timestamp could be reset
		// and the state of the peer can be interleaved between statements
		// causing race conditions.
		rf.lock.Lock()
		if rf.canStartElection(timeout) {
			rf.becomeCandidate()
			timeout = rf.chooseElectionTimeout()
		}
		rf.lock.Unlock()
	}
}

// Long-running routine that periodically checks if heartbeast timeout has elapsed.
// Once heartbeat has elapsed, peer broadcasts AppendEntries RPCs if the current leader.
func (rf *Raft) heartbeatTicker() {
	for rf.killed() == false {
		// Pause for a little bit (1ms) before checking if timeout has elapsed.
		time.Sleep(1 * time.Millisecond)

		// Broadcast entries if current leader
		rf.lock.Lock()
		if rf.canStartBroadcast() {
			debug(topicTimer, rf.me, "Heartbeat timeout elapsed")
			rf.broadcastAppendEntries()
		}
		rf.lock.Unlock()
	}
}

// Long-running routine that spins in a busy loop waiting for updates to commitIndex.
// Once commitIndex is updated, it sequentially applies commited, but unapplied commands
// to the state machine through a channel.
func (rf *Raft) commitTicker() {
	rf.lock.Lock()
	defer rf.lock.Unlock()

	for rf.killed() == false {
		for rf.lastApplied == rf.commitIndex {
			rf.commitLock.Wait()
		}

		assert(
			rf.lastApplied <= rf.commitIndex,
			rf.me,
			"lastApplied (%v) > commitIndex (%v)",
			rf.lastApplied,
			rf.commitIndex,
		)

		nextApply := rf.lastApplied + 1
		rf.lastApplied++

		if nextApply <= rf.log.lastIncludedIndex() {
			continue
		}

		entry := *rf.log.entry(nextApply)

		rf.lock.Unlock()
		// This is a blocking operation, so no locks can be held here.
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      entry.Command,
			CommandIndex: entry.Index,
		}
		debug(topicCommit, rf.me, "Applied command %v at index %v, T%v", entry.Command, entry.Index, entry.Term)
		rf.lock.Lock()
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
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	enableLogging()

	rf := new(Raft)

	rf.lock = sync.Mutex{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.dead = 0

	// Your initialization code here (3A, 3B, 3C).
	// initializing custom state
	rf.state = stateFollower
	rf.electionTimestamp = time.Now()
	rf.broadcastTimestamp = time.Now()
	rf.commitLock = sync.NewCond(&rf.lock)
	rf.applyCh = applyCh

	// initializing state from figure 2
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = makeLog()
	rf.lastApplied = 0
	rf.commitIndex = 0
	rf.nextIndex = make([]int, rf.numPeers())
	rf.matchIndex = make([]int, rf.numPeers())

	// initialize from state persisted before a crash
	rf.readPersist()

	// print out what state we are starting at
	debug(
		topicInfo,
		me,
		"Starting at T%v, LLI: %v, CI: %v",
		rf.currentTerm,
		rf.log.lastLogIndex(),
		rf.commitIndex,
	)

	// start long running goroutine for election timer
	go rf.electionTicker()

	// start long running goroutine for leader heartbeats
	go rf.heartbeatTicker()

	// start long running goroutine for applying commited entries
	go rf.commitTicker()

	return rf
}
