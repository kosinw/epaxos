package epaxos

import (
	"fmt"
	"testing"
	"time"
)

func interferes(cmd1, cmd2 interface{}) bool {
	return true
}

func checkCommitted(peers []*EPaxos, cmds []interface{}, logIndices []LogIndex) bool {
	committed := true
	// fmt.Printf("logindices: %v\n", logIndices)
	for i, logIndex := range logIndices {
		commitIndex := logIndex.Index
		cmd := cmds[i]
		// fmt.Printf("processing command %v (index %v)\n", cmd, i)
		for _, peer := range peers {
			peer.lock.Lock()
			sublog := peer.log[logIndex.Replica]

			if len(sublog) <= commitIndex {
				// fmt.Printf("[%v] len(sublog) %v <= commitIndex %v\n", peerInd, len(sublog), commitIndex)
				committed = false
				peer.lock.Unlock()
				break
			}
			instance := sublog[commitIndex]
			peer.lock.Unlock()
			// fmt.Printf("looking at commitIndex %v. instance %v\n", commitIndex, instance)
			if instance.Command != cmd || instance.Status != COMMITTED {
				// fmt.Printf("[%v] cmd %v instance.Command %v, instance.Status %v\n", peerInd, cmd, instance.Command, instance.Status)
				committed = false
				break
			}
		}
	}
	return committed
}

// one command leader process one command
func TestBasicCommit(t *testing.T) {
	n := 5
	cfg := make_config(t, n, false, interferes)

	e := cfg.peers[0]
	cmd := "hi"

	logIndex := e.Start(cmd)

	committed := false
	for iters := 0; iters < 30; iters++ {
		// fmt.Printf("[%v] checking committed\n", iters)
		committed = checkCommitted(cfg.peers, []interface{}{cmd}, []LogIndex{logIndex})
		if committed {
			break
		}
		// fmt.Printf("[%v] outcome: %v\n", iters, committed)
		time.Sleep(10 * time.Millisecond)
	}

	if !committed {
		t.Fail()
	} else {
		fmt.Printf("PASSED\n")
	}

	for ind, peer := range cfg.peers {
		fmt.Printf("peer %v log: %v\n", ind, peer.log)
	}
}

// two different command leaders process commands
func TestMultipleCommit(t *testing.T) {
	n := 5
	cfg := make_config(t, n, false, interferes)

	e0 := cfg.peers[0]
	e1 := cfg.peers[1]
	cmd1 := "hi"
	cmd2 := "bye"
	cmds := []interface{}{cmd1, cmd2}

	logIndex1 := e0.Start(cmd1)
	logIndex2 := e1.Start(cmd2)
	logIndices := []LogIndex{logIndex1, logIndex2}

	committed := false
	for iters := 0; iters < 30; iters++ {
		committed = checkCommitted(cfg.peers, cmds, logIndices)
		if committed {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if !committed {
		t.Fail()
	} else {
		fmt.Printf("PASSED\n")
	}

	for ind, peer := range cfg.peers {
		fmt.Printf("peer %v log: %v\n", ind, peer.log)
	}
}

// one command leader processes multiple commands
func TestOneCommit(t *testing.T) {
	n := 5
	cfg := make_config(t, n, false, interferes)

	e0 := cfg.peers[0]
	cmd1 := "hi"
	cmd2 := "bye"
	cmds := []interface{}{cmd1, cmd2}

	logIndex1 := e0.Start(cmd1)
	time.Sleep(10 * time.Millisecond)
	logIndex2 := e0.Start(cmd2)
	logIndices := []LogIndex{logIndex1, logIndex2}

	committed := false
	for iters := 0; iters < 30; iters++ {
		committed = checkCommitted(cfg.peers, cmds, logIndices)
		if committed {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if !committed {
		t.Fail()
	} else {
		fmt.Printf("PASSED\n")
	}

	for ind, peer := range cfg.peers {
		fmt.Printf("peer %v log: %v\n", ind, peer.log)
	}

}
