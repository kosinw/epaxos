package epaxos

import (
	"testing"
	"time"
)

func interferes2(cmd1, cmd2 interface{}) bool {
	return true
}
func TestEP1(t *testing.T) {
	const (
		servers = 5
		leader1 = 0
		leader2 = 1
	)

	cfg := make_config(t, servers, false, interferes2)
	defer cfg.cleanup()

	cfg.begin("Test interfere 1")

	cfg.one(leader1, 101, servers, true)

	cfg.disconnect((leader1 + 2) % servers)
	cfg.disconnect((leader1 + 3) % servers)
	cfg.disconnect((leader1 + 4) % servers)

	for i := 0; i < 50; i++ {
		cfg.peers[leader1].Start(102 + i)
	}

	time.Sleep(500 * time.Millisecond)

	cfg.disconnect((leader1 + 0) % servers)
	//	cfg.disconnect((leader1 + 1) % servers)

	cfg.connect((leader1 + 2) % servers)
	cfg.connect((leader1 + 3) % servers)
	cfg.connect((leader1 + 4) % servers)

	cfg.end()
}

func TestEP2(t *testing.T) {
	const (
		servers = 5
		leader1 = 0
		leader2 = 3
	)

	cfg := make_config(t, servers, false, interferes2)
	defer cfg.cleanup()

	cfg.begin("Test interfere 1")

	cfg.one(leader1, 101, servers, true)

	cfg.disconnect((leader1 + 3) % servers)
	cfg.disconnect((leader1 + 4) % servers)
	//all committed
	for i := 0; i < 1; i++ {
		cfg.one(leader1, 102+i, 3, true)
	}

	cfg.connect((leader1 + 3) % servers)
	time.Sleep(10 * time.Millisecond)
	cfg.disconnect((leader1 + 0) % servers)
	//	cfg.disconnect((leader1 + 1) % servers)
	cfg.one(leader2, 150, 3, false)
	//	cfg.connect((leader1 + 2) % servers)
	//	cfg.connect((leader1 + 3) % servers)
	//	cfg.connect((leader1 + 4) % servers)

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

	cfg.begin("Test (3B): replica backs up quickly")

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
