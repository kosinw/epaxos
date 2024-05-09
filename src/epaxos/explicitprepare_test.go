package epaxos

import (
	"fmt"
	"testing"
	"time"
)

func interferes2(cmd1, cmd2 interface{}) bool {
	return true
}
// func TestEP1(t *testing.T) {
// 	const (
// 		servers = 5
// 		leader1 = 0
// 		leader2 = 1
// 	)

// 	cfg := make_config(t, servers, false, interferes2)
// 	defer cfg.cleanup()

// 	cfg.begin("Test EP1")

// 	cfg.one(leader1, 101, servers, true)

// 	cfg.disconnect((leader1 + 2) % servers)
// 	cfg.disconnect((leader1 + 3) % servers)
// 	cfg.disconnect((leader1 + 4) % servers)

// 	for i := 0; i < 50; i++ {
// 		cfg.peers[leader1].Start(102 + i)
// 	}

// 	time.Sleep(500 * time.Millisecond)

// 	cfg.disconnect((leader1 + 0) % servers)
// 	//	cfg.disconnect((leader1 + 1) % servers)

// 	cfg.connect((leader1 + 2) % servers)
// 	cfg.connect((leader1 + 3) % servers)
// 	cfg.connect((leader1 + 4) % servers)

// 	cfg.end()
// }

func TestEP2(t *testing.T) {
	const (
		servers = 5
		leader1 = 0
		leader2 = 3
	)

	cfg := make_config(t, servers, false, interferes2)
	defer cfg.cleanup()

	cfg.begin("Test EP2")

	cfg.one(leader1, 101, servers, true)

	cfg.disconnect((leader1 + 3) % servers)
	cfg.disconnect((leader1 + 4) % servers)
	// all committed
	for i := 0; i < 1; i++ {
		cfg.one(leader1, 102+i, 3, true)
	}

	cfg.connect((leader1 + 3) % servers)
	time.Sleep(1000 * time.Millisecond)
	cfg.disconnect((leader1 + 0) % servers)
	//	cfg.disconnect((leader1 + 1) % servers)
	cfg.one(leader2, 150, 3, false)
	//	cfg.connect((leader1 + 2) % servers)
	//	cfg.connect((leader1 + 3) % servers)
	//	cfg.connect((leader1 + 4) % servers)

	cfg.end()
}

func TestEPBackup(t *testing.T) {
	const (
		servers = 5
		leader1 = 0
		leader2 = 1
	)

	cfg := make_config(t, servers, false, interferes1)
	defer cfg.cleanup()

	cfg.begin("Test EP Backup")

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

	//for i := 0; i < 1; i++ {
	//		cfg.one(leader2, 102+50+i, 3, true)
	//	}

	other := (leader1 + 2) % servers
	if leader2 == other {
		other = (leader2 + 1) % servers
	}
	cfg.disconnect(other)
	// 3 and 4 are up
	for i := 0; i < 1; i++ {
		cfg.peers[leader2].Start(102 + 50 + 50 + i)
	}

	time.Sleep(100 * time.Millisecond)

	for i := 0; i < servers; i++ {
		cfg.disconnect(i)
	}
	//0 1 and 2 are up
	cfg.connect((leader1 + 0) % servers)
	cfg.connect((leader1 + 1) % servers)
	cfg.connect(other)

	time.Sleep(1000 * time.Millisecond)
	fmt.Printf("all connected\n")
	for i := 0; i < 50; i++ {
		cfg.one(leader1, 102+50+50+50+i, 3, false)
	}

	for i := 0; i < servers; i++ {
		cfg.connect(i)
	}

	cfg.one(leader1, 999, servers, true)

	cfg.end()
}

func TestEPBack(t *testing.T) {
	const (
		servers = 5
		leader1 = 0
		leader2 = 1
	)

	cfg := make_config(t, servers, false, interferes1)
	defer cfg.cleanup()

	cfg.begin("Test EP Back")

	cfg.one(leader1, 101, servers, true)

	cfg.disconnect((leader1 + 2) % servers)
	cfg.disconnect((leader1 + 3) % servers)
	cfg.disconnect((leader1 + 4) % servers)

	for i := 0; i < 1; i++ {
		cfg.peers[leader1].Start(102 + i)
	}

	time.Sleep(500 * time.Millisecond)

	cfg.disconnect((leader1 + 0) % servers)
	//cfg.disconnect((leader1 + 1) % servers)

	cfg.connect((leader1 + 2) % servers)
	cfg.connect((leader1 + 3) % servers)
	cfg.connect((leader1 + 4) % servers)

	for i := 0; i < 50; i++ {
		cfg.one(leader2, 102+50+i, 3, true)
	}
	//	for i := 0; i < servers; i++ {
	//	cfg.connect(i)
	//	}

	//cfg.one(leader1, 999, servers, true)

}

func TestEPReexecution(t *testing.T) {
	const (
		servers = 5
		leader1 = 0
		leader2 = 1
	)

	cfg := make_config(t, servers, false, interferes1)
	defer cfg.cleanup()

	cfg.begin("Test EP Re-Executino")

	cfg.peers[leader1].Start(101)
	time.Sleep(10 * time.Millisecond)
	cfg.crash1(leader1)

	cfg.start1(leader1, cfg.applier)
	cfg.connect(leader1)

	cfg.one(leader2, 102, servers, true)

	cfg.end()
}

func TestEPRecovery(t *testing.T) {
	const (
		servers = 5
		leader1 = 0
		leader2 = 1
	)
	cfg := make_config(t, servers, false, interferes1)
	defer cfg.cleanup()

	cfg.begin("Test EP Recovery")

	cfg.peers[leader1].Start(101)
	time.Sleep(10 * time.Millisecond)
	cfg.crash1(leader1)
	cfg.start1(leader1, cfg.applier)
	cfg.connect(leader1)
	index := LogIndex{Replica: leader1, Index:0}
	cfg.wait(index, servers)

	cfg.end()
}

func TestEP3(t *testing.T) {
	const (
		servers = 5
		leader1 = 0
		leader2 = 1
	)
	cfg := make_config(t, servers, false, interferes1)
	defer cfg.cleanup()

	cfg.begin("Test EP Recovery")

	// start with 2 replicas
	cfg.disconnect((leader1 + 2) % servers)
	cfg.disconnect((leader1 + 3) % servers)
	cfg.disconnect((leader1 + 4) % servers)

	cfg.peers[leader1].Start(101)
	time.Sleep(50 * time.Millisecond)
	cfg.disconnect(leader1)
	fmt.Printf("DISCONNECTED LEADER1 %v\n", leader1)

	cfg.connect((leader1 + 2) % servers)
	cfg.connect((leader1 + 3) % servers)
	cfg.one(leader2, 102, servers - 2, false)

	cfg.connect((leader1 + 4) % servers)
	cfg.connect(leader1)
	cfg.one((leader1 + 4), 103, servers, false)

	cfg.end()
}