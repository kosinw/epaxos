package epaxos

//
// EPaxos tests.
//
// these tests are originally based on the lab tests
// from lab 3 of the 2024 version of 6.5840
//

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// import "fmt"
// import "sync/atomic"
// import "sync"

func interferes1(cmd1, cmd2 interface{}) bool {
	return true
}

// check to see if we can successfully replicate
// 3 entries proposed by 3 different replicas each
func TestBasicAgree3B(t *testing.T) {
	const (
		servers = 3
		iters   = 3
	)

	cfg := make_config(t, servers, false, interferes1)
	defer cfg.cleanup()

	cfg.begin("Test (3B): basic agreement")

	key := randstring(5)

	for r := 0; r < servers; r++ {
		for i := 0; i < iters; i++ {
			index := LogIndex{Replica: r, Index: i}
			nd, _ := cfg.nExecuted(index)

			if nd > 0 {
				t.Fatalf("some have committed before Start()")
			}

			xindex := cfg.one(r, key, servers, false)
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

	cfg := make_config(t, servers, false, interferes1)
	defer cfg.cleanup()

	cfg.begin("Test (3B): RPC byte count")

	cfg.one(leader, 99, servers, false)
	bytes0 := cfg.bytesTotal()

	var sent int64 = 0

	for i := 1; i < iters+1; i++ {
		cmd := randstring(5)
		index := LogIndex{Replica: leader, Index: i}
		xindex := cfg.one(leader, cmd, servers, false)

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

func TestFollowerFailure3B(t *testing.T) {
	const (
		servers = 3
		leader  = 0
	)

	cfg := make_config(t, servers, false, interferes1)
	defer cfg.cleanup()

	cfg.begin("Test (3B): test progressive failure of followers")

	cfg.one(leader, 101, servers, false)

	// disconnect last command leader
	cfg.disconnect((leader + 1) % servers)

	// the two remaining replicas should be able to agree
	cfg.one(leader, 102, servers-1, false)
	time.Sleep(1000 * time.Millisecond)
	cfg.one(leader, 103, servers-1, false)

	// disconnect another replica
	cfg.disconnect((leader + 2) % servers)

	// submit a command to each server, make sure their instances
	index := cfg.peers[leader+1].Start(104)

	if index.Replica != leader+1 && index.Index != 1 {
		t.Fatalf("expected instance %v.1, instead got %v", leader+1, index)
	}

	time.Sleep(2000 * time.Millisecond)

	// Check that the command did not execute.
	n, _ := cfg.nExecuted(LogIndex{Replica: leader + 1, Index: 0})

	if n > 0 {
		t.Fatalf("%v executed but no quorum", n)
	}

	cfg.end()
}

// test just failure of leaders.
func TestLeaderFailure3B(t *testing.T) {
	const (
		servers = 3
		leader1 = 0
		leader2 = 1
	)

	cfg := make_config(t, servers, false, interferes1)
	defer cfg.cleanup()

	cfg.begin("Test (3B): test failure of leaders")

	cfg.one(leader1, 101, servers, false)

	// disconnect the first leader.
	cfg.disconnect(leader1)

	// the remaining followers should elect
	// a new leader.
	cfg.one(leader2, 102, servers-1, false)
	time.Sleep(1000 * time.Millisecond)
	cfg.one(leader2, 103, servers-1, false)

	// disconnect the new leader.
	cfg.disconnect(leader2)

	// submit a command to each server.
	for i := 0; i < servers; i++ {
		cfg.peers[i].Start(104)
	}

	time.Sleep(2 * 1000 * time.Millisecond)

	// check that command 104 did not commit.
	n, _ := cfg.nExecuted(LogIndex{Replica: leader2, Index: 2})
	if n > 0 {
		t.Fatalf("%v executed but no quorum", n)
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

	cfg := make_config(t, servers, false, interferes1)
	defer cfg.cleanup()

	cfg.begin("Test (3B): agreement after replica reconnects")

	cfg.one(leader, 100, servers, false)

	cfg.disconnect((leader + 1) % servers)

	cfg.one(leader, 101, servers-1, false)
	cfg.one(leader, 102, servers-1, false)
	cfg.one(leader, 103, servers-1, false)
	cfg.one(leader, 104, servers-1, false)

	cfg.connect((leader + 1) % servers)

	cfg.one(leader, 105, servers, true)
	cfg.one(leader, 106, servers, true)

	cfg.end()
}

func TestFailNoAgree3B(t *testing.T) {
	const (
		servers = 5
		leader  = 3
	)

	cfg := make_config(t, servers, false, interferes1)
	defer cfg.cleanup()

	cfg.begin("Test (3B): no agreement if too many replicas disconnect")

	cfg.one(leader, 10, servers, false)

	cfg.disconnect((leader + 1) % servers)
	cfg.disconnect((leader + 2) % servers)
	cfg.disconnect((leader + 3) % servers)

	// submit a command to each server, make sure their instances
	index := cfg.peers[leader].Start(104)

	if index.Replica != leader && index.Index != 1 {
		t.Fatalf("expected instance %v.1, instead got %v", leader, index)
	}

	time.Sleep(2000 * time.Millisecond)

	// Check that the command did not execute.
	n, _ := cfg.nExecuted(LogIndex{Replica: leader + 1, Index: 1})

	if n > 0 {
		t.Fatalf("%v executed, but no quorum", n)
	}

	// repair
	cfg.connect((leader + 1) % servers)
	cfg.connect((leader + 2) % servers)
	cfg.connect((leader + 3) % servers)

	cfg.one(leader, 1000, servers, true)

	cfg.end()
}

func TestConcurrentStarts3B(t *testing.T) {
	const (
		servers = 3
		iters   = 5
	)

	cfg := make_config(t, servers, false, interferes1)
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

	cfg := make_config(t, servers, false, interferes1)
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

	cfg := make_config(t, servers, false, interferes1)
	defer cfg.cleanup()

	cfg.begin("Test (3B): replica backs up quickly")

	cfg.one(leader1, 101, servers, true)

	cfg.disconnect((leader1 + 2) % servers)
	cfg.disconnect((leader1 + 3) % servers)
	cfg.disconnect((leader1 + 4) % servers)

	for i := 0; i < 50; i++ {
		cfg.peers[leader1].Start(102 + i)
	}

	time.Sleep(500 * time.Millisecond)

	cfg.disconnect((leader1 + 0) % servers)
	cfg.disconnect((leader1 + 1) % servers)

	cfg.connect((leader1 + 2) % servers)
	cfg.connect((leader1 + 3) % servers)
	cfg.connect((leader1 + 4) % servers)

	for i := 0; i < 50; i++ {
		cfg.one(leader2, 102+50+i, 3, true)
	}

	other := (leader1 + 2) % servers
	if leader2 == other {
		other = (leader2 + 1) % servers
	}
	cfg.disconnect(other)

	for i := 0; i < 50; i++ {
		cfg.peers[leader2].Start(102 + 50 + 50 + i)
	}

	time.Sleep(500 * time.Millisecond)

	for i := 0; i < servers; i++ {
		cfg.disconnect(i)
	}

	cfg.connect((leader1 + 0) % servers)
	cfg.connect((leader1 + 1) % servers)
	cfg.connect(other)

	for i := 0; i < 50; i++ {
		cfg.one(leader1, 102+50+50+50+i, 3, true)
	}

	for i := 0; i < servers; i++ {
		cfg.connect(i)
	}

	cfg.one(leader1, 999, servers, true)

	cfg.end()
}

func TestCount3B(t *testing.T) {
	const (
		servers = 3
	)

	leader := 0
	cfg := make_config(t, servers, false, interferes1)
	defer cfg.cleanup()

	cfg.begin("Test (3B): RPC counts aren't too high")

	rpcs := func() (n int) {
		for j := 0; j < servers; j++ {
			n += cfg.rpcCount(j)
		}
		return
	}

	total1 := rpcs()

	var total2 int
	var success bool
loop:
	for try := 0; try < 5; try++ {
		if try > 0 {
			// give solution some time to settle
			time.Sleep(3 * time.Second)
		}

		// leader = cfg.checkOneLeader()
		leader := (leader + 1) % servers
		total1 = rpcs()

		iters := 10
		starti := cfg.peers[leader].Start(1)
		// if !ok {
		// 	// leader moved on really quickly
		// 	continue
		// }
		cmds := []int{}
		for i := 1; i < iters+2; i++ {
			x := int(rand.Int31())
			cmds = append(cmds, x)
			index1 := cfg.peers[leader].Start(x)

			tmp := LogIndex{Replica: starti.Replica, Index: starti.Index + i}

			if tmp != index1 {
				t.Fatalf("Start() failed")
			}
		}

		for i := 1; i < iters+1; i++ {
			tmp := LogIndex{Replica: starti.Replica, Index: starti.Index + i}
			cmd := cfg.wait(tmp, servers)
			if ix, ok := cmd.(int); ok == false || ix != cmds[i-1] {
				if ix == -1 {
					// term changed -- try again
					continue loop
				}
				t.Fatalf("wrong value %v committed for index %v; expected %v\n", cmd, tmp, cmds)
			}
		}

		failed := false
		total2 = 0
		for j := 0; j < servers; j++ {
			total2 += cfg.rpcCount(j)
		}

		if failed {
			continue loop
		}

		if total2-total1 > (iters+1+3)*10 {
			t.Fatalf("too many RPCs (%v) for %v entries\n", total2-total1, iters)
		}

		success = true
		break
	}

	if !success {
		t.Fatalf("term changed too often")
	}

	time.Sleep(1000 * time.Millisecond)

	total3 := 0
	for j := 0; j < servers; j++ {
		total3 += cfg.rpcCount(j)
	}

	if total3-total2 > 3*20 {
		t.Fatalf("too many RPCs (%v) for 1 second of idleness\n", total3-total2)
	}

	cfg.end()
}

func TestPersist13C(t *testing.T) {
	const (
		servers = 3
		leader1 = 0
		leader2 = 1
	)
	cfg := make_config(t, servers, false, interferes1)
	defer cfg.cleanup()

	cfg.begin("Test (3C): basic persistence")

	cfg.one(leader1, 11, servers, true)

	// crash and re-start all
	for i := 0; i < servers; i++ {
		cfg.start1(i, cfg.applier)
	}
	for i := 0; i < servers; i++ {
		cfg.disconnect(i)
		cfg.connect(i)
	}

	cfg.one(leader1, 12, servers, true)

	cfg.disconnect(leader1)
	cfg.start1(leader1, cfg.applier)
	cfg.connect(leader1)

	cfg.one(leader1, 13, servers, true)

	cfg.disconnect(leader2)
	// leader2 disconnected so we send to leader1
	cfg.one(leader1, 14, servers-1, true)
	cfg.start1(leader2, cfg.applier)
	cfg.connect(leader2)
	// leader2 back

	tmp := LogIndex{Replica: leader1, Index: 3} // wait for leader2 to join before killing i3
	cfg.wait(tmp, servers)

	i3 := (leader2 + 1) % servers
	cfg.disconnect(i3)
	cfg.one(leader2, 15, servers-1, true)
	cfg.start1(i3, cfg.applier)
	cfg.connect(i3)

	cfg.one(leader2, 16, servers, true)

	cfg.end()
}

func TestPersist23C(t *testing.T) {
	const (
		servers = 5
		leader1 = 3
		leader2 = 4
	)
	cfg := make_config(t, servers, false, interferes1)
	defer cfg.cleanup()

	cfg.begin("Test (3C): more persistence")

	index := 1
	for iters := 0; iters < 5; iters++ {
		cfg.one(leader1, 10+index, servers, true)
		index++

		cfg.disconnect((leader1 + 1) % servers)
		cfg.disconnect((leader1 + 2) % servers)

		cfg.one(leader1, 10+index, servers-2, true)
		index++

		cfg.disconnect((leader1 + 0) % servers)
		cfg.disconnect((leader1 + 3) % servers)
		cfg.disconnect((leader1 + 4) % servers)

		cfg.start1((leader1 + 1) % servers, cfg.applier)
		cfg.start1((leader1 + 2) % servers, cfg.applier)
		cfg.connect((leader1 + 1) % servers)
		cfg.connect((leader1 + 2) % servers)

		time.Sleep(1000 * time.Millisecond)

		cfg.start1((leader1 + 3) % servers, cfg.applier)
		cfg.connect((leader1 + 3) % servers)
		
		// leader1 & leader1 + 4 are disconnected at this point
		cfg.one(leader2, 10+index, servers-2, true)
		index++

		cfg.connect((leader1 + 4) % servers)
		cfg.connect((leader1 + 0) % servers)
	}

	cfg.one(leader1, 1000, servers, true)

	cfg.end()
}

func TestPersist33C(t *testing.T) {
	const (
		servers = 3
		leader = 0
	)
	cfg := make_config(t, servers, false, interferes1)
	defer cfg.cleanup()

	cfg.begin("Test (3C): partitioned server, server restarts")

	cfg.one(leader, 101, servers, true)

	cfg.disconnect((leader + 2) % servers)

	cfg.one(leader, 102, servers-1, true)

	cfg.crash1((leader + 0) % servers)
	cfg.crash1((leader + 1) % servers)
	cfg.connect((leader + 2) % servers)
	cfg.start1((leader + 0) % servers, cfg.applier)
	cfg.connect((leader + 0) % servers)

	cfg.one(leader, 103, servers-1, true)

	cfg.start1((leader + 1) % servers, cfg.applier)
	cfg.connect((leader + 1) % servers)

	cfg.one(leader, 104, servers, true)

	cfg.end()
}

func TestFigure83C(t *testing.T) {
	const (
		servers = 5
		leader1 = 0
	)
	cfg := make_config(t, servers, false, interferes1)
	defer cfg.cleanup()

	cfg.begin("Test (3C): Figure 8")

	cfg.one(leader1, 101, servers, true)

	nup := servers
	for iters := 0; iters < 1000; iters++ {
		leader := -1
		// find a functional server to be leader
		for i := 0; i < servers; i++ {
			if cfg.peers[i] != nil {
				cfg.peers[i].Start(102 + iters)
				leader = i
				break
			}
		}

		cfg.peers[leader].Start(101)

		if (rand.Int() % 1000) < 100 {
			ms := rand.Int63() % 500
			time.Sleep(time.Duration(ms) * time.Millisecond)
		} else {
			ms := rand.Int63() % 13
			time.Sleep(time.Duration(ms) * time.Millisecond)
		}

		if leader != -1 {
			cfg.crash1(leader)
			nup -= 1
		}

		if nup < 3 {
			s := rand.Int() % servers
			if cfg.peers[s] == nil {
				cfg.start1(s, cfg.applier)
				cfg.connect(s)
				nup += 1
			}
		}
	}

	for i := 0; i < servers; i++ {
		if cfg.peers[i] == nil {
			cfg.start1(i, cfg.applier)
			cfg.connect(i)
		}
	}

	cfg.one(leader1, 9999, servers, true)

	cfg.end()
}


func TestUnreliableAgree3C(t *testing.T) {
	const (
		servers = 5
		leader1 = 0
	)
	cfg := make_config(t, servers, true, interferes1)
	defer cfg.cleanup()

	cfg.begin("Test (3C): unreliable agreement")

	var wg sync.WaitGroup

	for iters := 1; iters < 50; iters++ {
		for j := 0; j < 4; j++ {
			wg.Add(1)
			go func(iters, j int) {
				defer wg.Done()
				cfg.one(leader1, (100 * iters) + j, 1, true)
			} (iters, j)
		}
		cfg.one(leader1, iters, 1, true)
	}

	cfg.setunreliable(false)

	wg.Wait()

	cfg.one(leader1, 100, servers, true)

	cfg.end()
}

func TestFigure8Unreliable3C(t *testing.T) {
	const (
		servers = 5
		leader1 = 0
	)
	cfg := make_config(t, servers, true, interferes1)
	defer cfg.cleanup()

	cfg.begin("Test (3C): Figure 8 (unreliable)")

	cfg.one(leader1, 101, 1, true)

	nup := servers
	for iters := 0; iters < 1000; iters++ {
		if iters == 200 {
			cfg.setlongreordering(true)
		}
		leader := -1
		for i := 0; i < servers; i++ {
			if cfg.peers[i] != nil && cfg.connected[i] {
				cfg.peers[i].Start(102 + i)
				leader = i
				break
			}
		}

		if (rand.Int() % 1000) < 100 {
			ms := rand.Int63() % 500
			time.Sleep(time.Duration(ms) * time.Millisecond)
		} else {
			ms := (rand.Int63() % 13)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		}
		
		if leader != -1 && (rand.Int() % 1000) < 500 {
			cfg.disconnect(leader)
			nup -= 1
		}

		if nup < 3 {
			s := rand.Int() % servers
			if cfg.connected[s] == false {
				cfg.connect(s)
				nup += 1
			}
		}
	}

	for i := 0; i < servers; i++ {
		if cfg.connected[i] == false {
			cfg.connect(i)
		}
	}

	cfg.one(leader1, 9999, servers, true)

	cfg.end()
}

func internalChurn(t *testing.T, unreliable bool) {
	const (
		servers = 5
		leader1 = 0
	)
	cfg := make_config(t, servers, true, interferes1)
	defer cfg.cleanup()

	if unreliable {
		cfg.begin("Test (3C): unreliable churn")
	} else {
		cfg.begin("Test (3C): churn")
	}

	stop := int32(0)

	cfn := func(me int, ch chan []int) {
		var ret []int
		ret = nil
		defer func() { ch <- ret }()
		values := []int{}
		for atomic.LoadInt32(&stop) == 0 {
			x := rand.Int() 
			index := LogIndex{}
			ok := false
			for i := 0; i < servers; i++ {
				cfg.mu.Lock()
				peer := cfg.peers[i]
				cfg.mu.Unlock()
				if peer != nil {
					index = peer.Start(x) // didn't know how to circumvent this randInt thing
					ok = true
					break
				}
			}
			if ok {
				for _, to := range []int{10, 20, 50, 100, 200} {
					nd, cmd := cfg.nExecuted(index)
					if nd > 0 {
						if xx, ok := cmd.(int); ok {
							if xx == x {
								values = append(values, x)
							}
						} else {
							cfg.t.Fatalf("wrong command type")
						}
						break
					}
					time.Sleep(time.Duration(to) * time.Millisecond)
				}
			} else {
				time.Sleep(time.Duration(79+me*17) * time.Millisecond)
			}
		}
		ret = values
	}

	ncli := 3
	cha := []chan []int{}
	for i := 0; i < ncli; i++ {
		cha = append(cha, make(chan []int))
		go cfn(i, cha[i])
	}

	for iters := 0; iters < 20; iters++ {
		if (rand.Int() % 1000) < 200 {
			i := rand.Int() % servers
			cfg.disconnect(i)
		}

		if (rand.Int() % 1000) < 500 {
			i := rand.Int() % servers
			if cfg.peers[i] == nil {
				cfg.start1(i, cfg.applier)
			}
			cfg.connect(i)
		}

		if (rand.Int() % 1000) < 200 {
			i := rand.Int() % servers
			if cfg.peers[i] != nil {
				cfg.crash1(i)
			}
		}

		time.Sleep(700 * time.Millisecond)
	}

	time.Sleep(1000 * time.Millisecond)
	cfg.setunreliable(false)
	for i := 0; i < servers; i++ {
		if cfg.peers[i] == nil {
			cfg.start1(i, cfg.applier)
		}
		cfg.connect(i)
	}

	atomic.StoreInt32(&stop, 1)

	values := []int{}
	for i := 0; i < ncli; i++ {
		vv := <-cha[i]
		if vv == nil {
			t.Fatal("client failed")
		}
		values = append(values, vv...)
	}

	time.Sleep(1000 * time.Millisecond)

	lastIndex := cfg.one(leader1, 100, servers, true)
	really := make([]int, lastIndex.Index + 1)
	for index := 1; index <= lastIndex.Index; index++ {
		v := cfg.wait(LogIndex{Replica: lastIndex.Replica, Index: index}, servers)
		if vi, ok := v.(int); ok {
			really = append(really, vi)
		} else {
			t.Fatalf("not an int")
		}
	}

	for _, v1 := range values {
		ok := false
		for _, v2 := range really {
			if v1 == v2 {
				ok = true
			}
		}
		if ok == false {
			cfg.t.Fatalf("didn't find a value")
		}
	}

	cfg.end()
}

func TestReliableChurn3C(t *testing.T) {
	internalChurn(t, false)
}

func TestUnreliableChurn3C(t *testing.T) {
	internalChurn(t, true)
}

