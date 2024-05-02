package epaxos

import (
	"fmt"
	"testing"
	"time"
)

func interferes(cmd1, cmd2 interface{}) bool {
	return true;
}

func checkCommitted(log PaxosLog, cmd interface{}, commitIndex int) bool {
	committed := true
	for _, sublog := range(log) {
		if len(sublog) <= commitIndex {
			committed = false
			break
		}
		instance := sublog[commitIndex]
		if instance.Command != cmd || instance.Status != COMMITTED {
			committed = false
			break
		}
	}
	return committed
}

func TestBasicCommit(t *testing.T) {
	n := 5
	cfg := make_config(t, n, false, interferes)
	for i := 0; i < 5; i++ {
		cfg.start1(i, cfg.applier)
	}

	e := cfg.peers[0]
	fmt.Printf("[test] MY PEERS: %v\n", e.peers)
	cmd := "hi"

	logIndex := e.Start(cmd)

	for iters := 0; iters < 30; iters++ {
		committed := checkCommitted(e.log, cmd, logIndex.Index)
		if committed {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	
	fmt.Printf("e's log after 30 iters: %v\n", e.log)	
}