package epaxos

type Instance struct {
	Deps    []Instance  // term when entry was received by leader
	Seq     int         // index that entry will appear at if it's ever committed
	Command interface{} // state machine command
	Replica int
	Index   int
}
type Status int

// Enum values
const (
	PREACCEPTED Status = iota
	ACCEPTED
	COMMITTED
	EXECUTED
)
