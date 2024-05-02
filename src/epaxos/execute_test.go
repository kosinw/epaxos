package epaxos

import (
	"fmt"
	"sync"
	"testing"

	"6.5840/labrpc"
)

func TestExecute(t *testing.T) {

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
		//4: 4.0->1.0, 4.1->3.0
		{Instance{[]LogIndex{{Replica: 1, Index: 0}}, 8, "hi", LogIndex{4, 0}}, Instance{[]LogIndex{{Replica: 3, Index: 0}}, 4, "hi", LogIndex{4, 1}}},
	}
	totalEntries := 0
	for _, r := range graph {
		totalEntries += len(r)
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
	order := []LogIndex{}
	go e.scc(len(e.peers))
	cnt := 0
	for {
		x := <-apply
		order = append(order, x.Position)
		fmt.Printf("APPLY #%v: %v\n", cnt, x.Position)
		cnt += 1
		if cnt == totalEntries {
			break
		}
	}
	if !test_scc(n, &e, order) {
		t.Fail()
	}
	fmt.Println("PASSED")
}

func TestExecute2(t *testing.T) {

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
		//4: 4.0->1.0, 4.1->3.0
		{Instance{[]LogIndex{{Replica: 1, Index: 0}}, 8, "hi", LogIndex{4, 0}}, Instance{[]LogIndex{{Replica: 3, Index: 0}}, 4, "hi", LogIndex{4, 1}}},
	}
	totalEntries := 0
	for _, r := range graph {
		totalEntries += len(r)
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
	order := []LogIndex{}
	go e.scc(len(e.peers))
	cnt := 0
	for {
		x := <-apply
		order = append(order, x.Position)
		fmt.Printf("APPLY #%v: %v\n", cnt, x.Position)
		cnt += 1
		if cnt == totalEntries {
			break
		}
	}
	if !test_scc(n, &e, order) {
		t.Fail()
	}
	fmt.Println("PASSED")
}
func TestSCCChecker(t *testing.T) {

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
		{EXECUTED},
		{EXECUTED, EXECUTED, EXECUTED},
		{EXECUTED},
		{EXECUTED, EXECUTED},
		{EXECUTED, EXECUTED},
	}
	e.log = graph
	order := []LogIndex{{3, 0}, {2, 0}, {4, 1}, {3, 1}, {1, 0}, {1, 1}, {1, 2}, {0, 0}}
	if !test_scc(n, &e, order) {
		t.Fail()
	}
}
