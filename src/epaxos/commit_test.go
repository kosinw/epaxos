package epaxos

import (
	"fmt"
	"testing"
	"time"
)

func interferes(cmd1, cmd2 interface{}) bool {
	return true;
}

func checkCommitted(peers []*EPaxos, cmd interface{}, commitIndex int) bool {
	committed := true
	for peerInd, peer := range(peers) {
		sublog := peer.log[peerInd]
		if len(sublog) <= commitIndex {
			fmt.Printf("[%v] len(sublog) %v <= commitIndex %v\n", peerInd, len(sublog), commitIndex)
			committed = false
			break
		}
		instance := sublog[commitIndex]
		if instance.Command != cmd || instance.Status != COMMITTED {
			fmt.Printf("[%v] instance.Command %v, instance.Status %v\n", peerInd, instance.Command, instance.Status)
			committed = false
			break
		}
		// for i, sublog := range(peer.log[peerInd]) {
		// 	if len(sublog) <= commitIndex {
		// 		fmt.Printf("[%v] len(sublog) %v <= commitIndex %v", i, len(sublog), commitIndex)
		// 		committed = false
		// 		break
		// 	}
		// 	instance := sublog[commitIndex]
		// 	if instance.Command != cmd || instance.Status != COMMITTED {
		// 		fmt.Printf("[%v] instacne.Command %v, instance.Status %v", i, instance.Command, instance.Status)
		// 		committed = false
		// 		break
		// 	}
		// }
	}
	return committed
}

func TestBasicCommit(t *testing.T) {
	n := 5
	cfg := make_config(t, n, false, interferes)

	e := cfg.peers[0]
	// fmt.Printf("[test] MY PEERS: %v\n", e.peers)
	// fmt.Printf("[test] e.log: %v\n", e.log)
	cmd := "hi"

	logIndex := e.Start(cmd)

	committed := false
	for iters := 0; iters < 30; iters++ {
		// fmt.Printf("[%v] checking committed\n", iters)
		committed = checkCommitted(cfg.peers, cmd, logIndex.Index)
		if committed {
			break
		}
		// fmt.Printf("[%v] outcome: %v\n", iters, committed)
		time.Sleep(10 * time.Millisecond)
	}
	
	if !committed {
		t.Fail()
	}

	for ind, peer := range cfg.peers {
		fmt.Printf("peer %v log: %v\n", ind, peer.log)
	}
}