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

func interferes(cmd1, cmd2 interface{}) bool {
	if v1, ok1 := cmd1.(testCommand); ok1 {
		if v2, ok2 := cmd2.(testCommand); ok2 {
			return v1.Key == v2.Key
		}
	}
	return false
}

// check to see if we can successfully replicate
// 3 entries proposed by 3 different replicas each
func TestBasicAgreeEPaxos(t *testing.T) {
	const (
		servers = 3
		iters   = 3
	)

	cfg := make_config(t, servers, false, interferes)
	defer cfg.cleanup()

	cfg.begin("Test: basic agreement")

	for r := 0; r < servers; r++ {
		for i := 0; i < iters; i++ {
			index := LogIndex{Replica: r, Index: i}
			nd, _ := cfg.nCommitted(index)

			if nd > 0 {
				t.Fatalf("some have committed before Start()")
			}

			xindex := cfg.one(r, "hello", servers, false)
			if xindex != index {
				t.Fatalf("got instance number %v but expected %v", xindex, index)
			}
		}
	}

	cfg.end()
}
