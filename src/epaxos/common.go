package epaxos

import (
	"fmt"
	"time"
)

type LogIndex struct {
	Replica int // # of replica that this log belongs to
	Index   int // index # of this entry within the log
}

const ReplicaAlphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"

func replicaName(replica int) string {
	return string(ReplicaAlphabet[replica])
}

func (li LogIndex) String() string {
	return fmt.Sprintf("%v.%v", replicaName(li.Replica), li.Index)
}

type Instance struct {
	Command  interface{}      // state machine command
	Deps     map[LogIndex]int // for all instances that this command depends on, maps its LogIndex to 1
	Seq      int              // index that entry will appear at if it's ever committed
	Position LogIndex         // instance number
	Status   Status           // current status of instance
	Ballot   Ballot           // ballot number
	Valid    bool             // true if instance has been initialized
	Timer    time.Time        // timestamp of when to restart instance
}

// possible statuses of an instance
type Status int

const (
	PreAccepted Status = iota
	Accepted
	Committed
	Executed
)

func (s Status) String() string {
	switch s {
	case PreAccepted:
		return "pre-accepted"
	case Accepted:
		return "accepted"
	case Committed:
		return "committed"
	case Executed:
		return "executed"
	}

	return "unknown"
}

type Ballot struct {
	BallotNum  int // ballot number
	ReplicaNum int // replica that sent the ballot (necessary for explicit prepare)
}

func (b Ballot) lt(o Ballot) bool {
	if b.BallotNum == o.BallotNum {
		return b.ReplicaNum < o.ReplicaNum
	}

	return b.BallotNum < o.BallotNum
}

func (b Ballot) eq(o Ballot) bool {
	if b.BallotNum == o.BallotNum {
		return b.ReplicaNum == o.ReplicaNum
	}

	return false
}

func (b Ballot) le(o Ballot) bool {
	return b.lt(o) || b.eq(o)
}

func (b Ballot) gt(o Ballot) bool {
	return !b.le(o)
}

func (b Ballot) ge(o Ballot) bool {
	return b.gt(o) || b.eq(o)
}

func (b Ballot) String() string {
	return fmt.Sprintf("epoch.%v.%v", replicaName(b.ReplicaNum), b.BallotNum)
}

// Pre-Accept RPC arguments structure
type PreAcceptArgs struct {
	Command  interface{}      // command
	Deps     map[LogIndex]int // list of all instances that contain commands that interfere with this command
	Seq      int              // sequence number used to break dependencies
	Ballot   Ballot           // ballot number + replica number
	Position LogIndex         // position of original command leader
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
	Command  interface{}      // command
	Deps     map[LogIndex]int // list of all instances that contain commands that interfere with this command
	Seq      int              // sequence number used to break dependencies
	Ballot   Ballot           // ballot number
	Position LogIndex         // position of original command leader
}

// Commit RPC reply structure
type CommitReply struct {
	Success bool // true if replica committed command
}
