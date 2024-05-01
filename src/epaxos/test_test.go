package epaxos

//
// EPaxos tests.
//
// these tests are originally based on the lab tests
// from lab 3 of the 2024 version of 6.5840
//

import "testing"

// import "fmt"
// import "time"
// import "math/rand"
// import "sync/atomic"
// import "sync"

type testCommand struct {
	Key string
	Op  string
}

func makePutCommand(key string) testCommand {
	return testCommand{
		Key: key,
		Op:  "PUT",
	}
}

func makeGetCommand(key string) testCommand {
	return testCommand{
		Key: key,
		Op:  "GET",
	}
}

func interferes(cmd1, cmd2 interface{}) bool {
	if v1, ok1 := cmd1.(testCommand); ok1 {
		if v2, ok2 := cmd2.(testCommand); ok2 {
			return (v1.Op == "PUT" || v2.Op == "PUT") && v1.Key == v2.Key
		}
	}
	return false
}

// check to see if we can successfully replicate
// 3 entries proposed by 3 different replicas each
func TestBasicAgree3B(t *testing.T) {
	const (
		servers = 3
		iters   = 3
	)

	cfg := make_config(t, servers, false, interferes)
	defer cfg.cleanup()

	cfg.begin("Test (3B): basic agreement")

	key := randstring(5000)

	for r := 0; r < servers; r++ {
		for i := 0; i < iters; i++ {
			index := LogIndex{Replica: r, Index: i}
			nd, _ := cfg.nExecuted(index)

			if nd > 0 {
				t.Fatalf("some have committed before Start()")
			}

			xindex := cfg.one(r, makePutCommand(key), servers, false)
			if xindex != index {
				t.Fatalf("got instance number %v but expected %v", xindex, index)
			}
		}
	}

	cfg.end()
}

// check, based on counting RPC bytes, that
// each command is sent to each peer just once
func TestRPCBytes3B(t *testing.T) {
	const (
		servers = 3
		iters   = 10
		leader  = 0
	)

	cfg := make_config(t, servers, false, interferes)
	defer cfg.cleanup()

	cfg.begin("Test (3B): RPC byte count")

	cfg.one(leader, makePutCommand("first"), servers, false)
	bytes0 := cfg.bytesTotal()

	var sent int64 = 0

	for i := 1; i < iters+1; i++ {
		cmd := randstring(5000)
		index := LogIndex{Replica: leader, Index: i}
		xindex := cfg.one(leader, makeGetCommand(cmd), servers, false)

		if xindex != index {
			t.Fatalf("got instance %v but expected %v", xindex, index)
		}

		sent += int64(len(cmd))
	}

	bytes1 := cfg.bytesTotal()
	got := bytes1 - bytes0

	// TODO(kosinw): I don't know if this is a good number since its based on Raft?
	expected := int64(servers) * sent + 50000

	if got > expected+50000 {
		t.Fatalf("too many RPC bytes; got %v, expected %v", got, expected)
	}

	cfg.end()
}
