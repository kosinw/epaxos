package epaxos

import "fmt"

type LogIndex struct {
	Replica int // # of replica that this log belongs to
	Index   int // index # of this entry within the log
}

const ReplicaAlphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"

func (li LogIndex) String() string {
	return fmt.Sprintf("%c.%v", ReplicaAlphabet[li.Replica], li.Index)
}

type Instance struct {
	Command interface{}      // state machine command
	Deps    map[LogIndex]int // for all instances that this command depends on, maps its LogIndex to 1
	Seq     int              // index that entry will appear at if it's ever committed

	Position LogIndex
	Status   Status // current status of instance
}

// possible statuses of an instance
type Status int

const (
	PREACCEPTED Status = iota
	ACCEPTED
	COMMITTED
	EXECUTED
)

type Ballot struct {
	BallotNum  int // ballot number
	ReplicaNum int // replica that sent the ballot (necessary for explicit prepare)
}

// Pre-Accept RPC arguments structure
type PreAcceptArgs struct {
	Command interface{}      // command
	Deps    map[LogIndex]int // list of all instances that contain commands that interfere with this command
	Seq     int              // sequence number used to break dependencies

	Ballot   Ballot   // ballot number + replica number
	Position LogIndex // position of original command leader
}

// Pre-Accept RPC reply structure
type PreAcceptReply struct {
	Deps    map[LogIndex]int // updated list of dependencies
	Seq     int              // updated sequence number
	Success bool             // true if pre-accept went through
}

// Accept RPC arguments structure
type AcceptArgs struct {
	Command interface{}      // command
	Deps    map[LogIndex]int // list of all instances that contain commands that interfere with this command
	Seq     int              // sequence number used to break dependencies

	Ballot   Ballot   // ballot number
	Position LogIndex // position of original command leader
}

// Accept RPC reply structure
type AcceptReply struct {
	Success bool // true if replica accepted command

}

// Commit RPC arguments structure
type CommitArgs struct {
	Command interface{}      // command
	Deps    map[LogIndex]int // list of all instances that contain commands that interfere with this command
	Seq     int              // sequence number used to break dependencies

	Ballot   Ballot   // ballot number
	Position LogIndex // position of original command leader
}

// Commit RPC reply structure
type CommitReply struct {
	Success bool // true if replica committed command
}
