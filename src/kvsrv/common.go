package kvsrv

import (
	"crypto/rand"
	"math/big"
)

type PutAppendArgs struct {
	Type    string
	Key     string
	Value   string
	ClerkId int
	SeqNum  int
}

type PutAppendReply struct {
	Value string
}

type GetArgs struct {
	Key     string
	ClerkId int
	SeqNum  int
}

type GetReply struct {
	Value string
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func id() int {
	return int(nrand())
}
