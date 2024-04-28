package epaxos

import (
	"fmt"
	"sync"
	"testing"

	"6.5840/labrpc"
)

func TestStuff(t *testing.T) {

	var e EPaxos
	apply := make(chan Instance)
	e.lock = sync.Mutex{}
	e.applyCh = apply
	n := 5
	e.peers = make([]*labrpc.ClientEnd, n)
	e.lastApplied = make([]int, n)
	graph := [][]Instance{
		//0: 0.0->1.0,
		{Instance{[]LogIndex{{Replica: 1, Index: 2}, {Replica: 1, Index: 0}}, 1, "", LogIndex{0, 0}}},
		//1: 1.0 -> 2.0, 1.1->1.2, 1.2-> 1.1,3.1
		{Instance{[]LogIndex{{Replica: 2, Index: 0}}, 7, "hi", LogIndex{1, 0}}, Instance{[]LogIndex{{1, 2}}, 6, "hi", LogIndex{1, 1}}, Instance{[]LogIndex{{3, 1}, {1, 1}}, 12, "hi", LogIndex{1, 2}}},
		//2: 2.0-> 3.1 3.0
		{Instance{[]LogIndex{{Replica: 3, Index: 0}, {Replica: 3, Index: 1}}, 3, "bye", LogIndex{2, 0}}},
		//3: 3.0-> 1.0, 3.1 -> 4.1
		{Instance{[]LogIndex{{Replica: 1, Index: 0}}, 2, "hi", LogIndex{3, 0}}, Instance{[]LogIndex{{Replica: 4, Index: 1}}, 5, "hi", LogIndex{3, 1}}},
		//4: 4.0, 4.1->3.0
		{Instance{[]LogIndex{{Replica: 1, Index: 0}}, 8, "hi", LogIndex{4, 0}}, Instance{[]LogIndex{{Replica: 3, Index: 0}}, 4, "hi", LogIndex{4, 1}}},
	}
	e.status = [][]Status{
		{COMMITTED},
		{COMMITTED, COMMITTED, COMMITTED},
		{COMMITTED},
		{COMMITTED, COMMITTED},
		{COMMITTED, COMMITTED},
	}
	e.lastApplied = []int{-1, -1, -1, -1, -1}
	e.log = graph
	go e.scc(len(e.peers))
	for {
		x := <-apply
		fmt.Printf("APPLY: %v\n", x.Position)
	}
}
