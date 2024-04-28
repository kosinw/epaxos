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
