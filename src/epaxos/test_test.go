package epaxos

//
// EPaxos tests.
//
// these tests are originally based on the lab tests
// from lab 3 of the 2024 version of 6.5840
//

import "testing"
import "time"
import "sync"
import "math/rand"

// import "fmt"
// import "sync/atomic"
// import "sync"

type testCommand struct {
	Key interface{}
	Op  string
}

func makePutCommand(key interface{}) testCommand {
	return testCommand{
		Key: key,
		Op:  "PUT",
	}
}

func makeGetCommand(key interface{}) testCommand {
	return testCommand{
		Key: key,
		Op:  "GET",
	}
}

func interferesTestCommand(cmd1, cmd2 interface{}) bool {
	if v1, ok1 := cmd1.(testCommand); ok1 {
		if v2, ok2 := cmd2.(testCommand); ok2 {
			return (v1.Op == "PUT" || v2.Op == "PUT") && v1.Key == v2.Key
		}
	}
	return false
}

func interferesInt(cmd1, cmd2 interface{}) bool {
	if v1, ok1 := cmd1.(int); ok1 {
		if v2, ok2 := cmd2.(int); ok2 {
			return v1 == v2
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

	cfg := make_config(t, servers, false, interferesTestCommand)
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

	cfg := make_config(t, servers, false, interferesTestCommand)
	defer cfg.cleanup()

	cfg.begin("Test (3B): RPC byte count")

	cfg.one(leader, makePutCommand(99), servers, false)
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
	expected := int64(servers)*sent + 50000

	if got > expected+50000 {
		t.Fatalf("too many RPC bytes; got %v, expected %v", got, expected)
	}

	cfg.end()
}

func TestReplicaFailure3B(t *testing.T) {
	const (
		servers = 3
		leader  = 0
	)

	cfg := make_config(t, servers, false, interferesTestCommand)
	defer cfg.cleanup()

	cfg.begin("Test (3B): test progressive failure of replicas")

	if x := cfg.one(leader, makePutCommand(101), servers, false); x != (LogIndex{leader, 0}) {
		t.Fatalf("expected instance %v.0, instead got %v", leader, x)
	}

	// disconnect last command leader
	cfg.disconnect(leader)

	// the two remaining replicas should be able to agree
	cfg.one((leader+2)%servers, makePutCommand(102), servers-1, false)
	cfg.one((leader+1)%servers, makePutCommand(103), servers-1, false)

	// disconnect another replica
	cfg.disconnect((leader + 2))

	// submit a command to each server, make sure their instances
	index := cfg.peers[leader+1].Start(makePutCommand(104))

	if index.Replica != leader+1 && index.Index != 1 {
		t.Fatalf("expected instance %v.1, instead got %v", leader+1, index)
	}

	time.Sleep(2000 * time.Second)

	// Check that the command did not execute.
	n, _ := cfg.nExecuted(LogIndex{Replica: leader + 1, Index: 1})

	if n > 0 {
		t.Fatalf("%v executed, but no quorum", n)
	}

	cfg.end()
}

// test that a replica participates after
// disconnect and re-connect.
func TestFailAgree3B(t *testing.T) {
	const (
		servers = 3
		leader  = 2
	)

	cfg := make_config(t, servers, false, interferesTestCommand)
	defer cfg.cleanup()

	cfg.begin("Test (3B): agreement after replica reconnects")

	cfg.one(leader, makePutCommand(101), servers, false)

	cfg.disconnect((leader + 1) % servers)

	cfg.one(leader, makePutCommand(102), servers-1, false)
	cfg.one(leader, makePutCommand(103), servers-1, false)
	cfg.one(leader, makePutCommand(104), servers-1, false)
	cfg.one(leader, makePutCommand(105), servers-1, false)

	cfg.connect((leader + 1) % servers)

	cfg.one(leader, makePutCommand(106), servers, true)
	cfg.one(leader, makePutCommand(107), servers, true)

	cfg.end()
}

func TestFailNoAgree3B(t *testing.T) {
	const (
		servers = 5
		leader  = 3
	)

	cfg := make_config(t, servers, false, interferesTestCommand)
	defer cfg.cleanup()

	cfg.begin("Test (3B): no agreement if too many replicas disconnect")

	cfg.one(leader, makePutCommand(10), servers, false)

	cfg.disconnect((leader + 1) % servers)
	cfg.disconnect((leader + 2) % servers)
	cfg.disconnect((leader + 3) % servers)

	// submit a command to each server, make sure their instances
	index := cfg.peers[leader].Start(makePutCommand(104))

	if index.Replica != leader && index.Index != 1 {
		t.Fatalf("expected instance %v.1, instead got %v", leader, index)
	}

	time.Sleep(2000 * time.Second)

	// Check that the command did not execute.
	n, _ := cfg.nExecuted(LogIndex{Replica: leader + 1, Index: 1})

	if n > 0 {
		t.Fatalf("%v executed, but no quorum", n)
	}

	// repair
	cfg.connect((leader + 1) % servers)
	cfg.connect((leader + 2) % servers)
	cfg.connect((leader + 3) % servers)

	cfg.one(leader, makePutCommand(1000), servers, true)

	cfg.end()
}

func TestConcurrentStarts3B(t *testing.T) {
	const (
		servers = 3
		iters   = 5
	)

	cfg := make_config(t, servers, false, interferesInt)
	defer cfg.cleanup()

	cfg.begin("Test (3B): concurrent Start()s")

	for try := 0; try < 5; try++ {
		if try > 0 {
			time.Sleep(3 * time.Second)
		}

		cfg.peers[0].Start(1)

		var wg sync.WaitGroup
		is := make(chan LogIndex, iters)

		for ii := 0; ii < iters; ii++ {
			wg.Add(1)

			go func(i int) {
				defer wg.Done()
				ix := cfg.peers[(i+1)%servers].Start(100 + i)
				is <- ix
			}(ii)
		}

		wg.Wait()
		close(is)

		cmds := []int{}
		for index := range is {
			cmd := cfg.wait(index, servers)
			if ix, ok := cmd.(int); ok {
				cmds = append(cmds, ix)
			} else {
				t.Fatalf("value %v is not an int", cmd)
			}
		}

		for ii := 0; ii < iters; ii++ {
			x := 100 + ii
			ok := false
			for j := 0; j < len(cmds); j++ {
				if cmds[j] == x {
					ok = true
				}
			}
			if ok == false {
				t.Fatalf("cmd %v missing in %v", x, cmds)
			}
		}
	}

	cfg.end()
}

func TestRejoin3B(t *testing.T) {
	const (
		servers = 3
		leader1 = 0
		leader2 = 1
		leader3 = 2
	)

	cfg := make_config(t, servers, false, interferesInt)
	defer cfg.cleanup()

	cfg.begin("Test (3B): rejoin of partitioned server")

	cfg.one(leader1, 101, servers, true)
	cfg.disconnect(leader1)
	cfg.peers[leader1].Start(102)
	cfg.peers[leader1].Start(103)
	cfg.peers[leader1].Start(104)

	cfg.one(leader2, 103, servers-1, true)
	cfg.disconnect(leader2)

	cfg.connect(leader1)

	cfg.one(leader3, 104, servers-1, true)
	cfg.connect(leader2)

	cfg.one(leader1, 105, servers, true)

	cfg.end()
}

func TestBackup3B(t *testing.T) {
	const (
		servers = 5
		leader1 = 0
		leader2 = 1
	)

	cfg := make_config(t, servers, false, interferesInt)
	defer cfg.cleanup()

	cfg.begin("Test (3B): replica backs up quickly")

	cfg.one(leader1, rand.Int(), servers, true)

	cfg.disconnect((leader1 + 2) % servers)
	cfg.disconnect((leader1 + 3) % servers)
	cfg.disconnect((leader1 + 4) % servers)

	for i := 0; i < 50; i++ {
		cfg.peers[leader1].Start(rand.Int())
	}

	time.Sleep(500 * time.Millisecond)

	cfg.disconnect((leader1 + 0) % servers)
	cfg.disconnect((leader1 + 1) % servers)

	cfg.connect((leader1 + 2) % servers)
	cfg.connect((leader1 + 3) % servers)
	cfg.connect((leader1 + 4) % servers)

	for i := 0; i < 50; i++ {
		cfg.one(leader2, rand.Int(), 3, true)
	}

	other := (leader1 + 2) % servers
	if leader2 == other {
		other = (leader2 + 1) % servers
	}
	cfg.disconnect(other)

	for i := 0; i < 50; i++ {
		cfg.peers[leader2].Start(rand.Int())
	}

	time.Sleep(500 * time.Millisecond)

	for i := 0; i < servers; i++ {
		cfg.disconnect(i)
	}

	cfg.connect((leader1 + 0) % servers)
	cfg.connect((leader1 + 1) % servers)
	cfg.connect(other)

	for i := 0; i < 50; i++ {
		cfg.one(leader1, rand.Int(), 3, true)
	}

	for i := 0; i < servers; i++ {
		cfg.connect(i)
	}

	cfg.one(leader1, rand.Int(), servers, true)

	cfg.end()
}
