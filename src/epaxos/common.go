package epaxos

type LogIndex struct {
	Replica int
	Index   int
}
type Instance struct {
	Deps     []LogIndex  // dependencies
	Seq      int         // index that entry will appear at if it's ever committed
	Command  interface{} // state machine command
	Position LogIndex
}

type Status int

// Enum values
const (
	PREACCEPTED Status = iota
	ACCEPTED
	COMMITTED
	EXECUTED
)

// Pre-Accept RPC arguments structure
type PreAcceptArgs struct {
	Command interface{} // command
	Deps    []Instance  // list of all instances that contain commands that interfere with this command
	Seq     int         // sequence number used to break dependencies

	Ballot   int // ballot number
	Replica  int // replica number
	Instance int // instance number of leader replica
	Leader   int // command leader
}

// Pre-Accept RPC reply structure
type PreAcceptReply struct {
	Deps []Instance // updated list of dependencies
	Seq  int        // updated sequence number
}

// Accept RPC arguments structure
type AcceptArgs struct {
	Command interface{} // command
	Deps    []Instance  // list of all instances that contain commands that interfere with this command
	Seq     int         // sequence number used to break dependencies

	Ballot   int // ballot number
	Replica  int // replica number
	Instance int // instance number of leader replica
	Leader   int // command leader
}

// Accept RPC reply structure
type AcceptReply struct {
	Success bool // true if replica accepted command

}

// Commit RPC arguments structure
type CommitArgs struct {
	Command interface{} // command
	Deps    []Instance  // list of all instances that contain commands that interfere with this command
	Seq     int         // sequence number used to break dependencies

	Ballot   int // ballot number
	Replica  int // replica number
	Instance int // instance number of leader replica
	Leader   int // command leader
}

// Commit RPC reply structure
type CommitReply struct {
	Success bool // true if replica committed command
}
