package kvraft

const (
	OK                = "OK"
	ErrNoKey          = "no key found"
	ErrWrongLeader    = "wrong leader"
	ErrNotCommitted   = "command could not be committed"
	ErrNetworkTimeout = "network timeout"
)

type Err string

const (
	OpGet    = "GET"
	OpPut    = "PUT"
	OpAppend = "APPEND"
)

// Put or Append
type PutAppendArgs struct {
	Key     string
	Value   string
	Op      string
	ClerkId string // unique ID for clerk
	SeqNum  int    // monotonically increasing sequence number
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key     string
	ClerkId string // unique ID for clerk
	SeqNum  int    // monotonically increasing sequence number
}

type GetReply struct {
	Err   Err
	Value string
}
