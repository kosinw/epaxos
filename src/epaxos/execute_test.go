package epaxos

import (
	"fmt"
	"sync"
	"testing"
	"time"

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
		{Instance{Deps: map[LogIndex]int{{Replica: 1, Index: 2}: 1, {Replica: 1, Index: 0}: 1}, Seq: 1, Command: "", Position: LogIndex{0, 0}, Status: COMMITTED}},
		//1: 1.0 -> 2.0, 1.1->1.2, 1.2-> 1.1,3.1
		{Instance{Deps: map[LogIndex]int{{Replica: 2, Index: 0}: 1}, Seq: 7, Command: "hi", Position: LogIndex{1, 0}, Status: COMMITTED}, Instance{Deps: map[LogIndex]int{{Replica: 1, Index: 2}: 1}, Seq: 6, Command: "hi", Position: LogIndex{Replica: 1, Index: 1}, Status: COMMITTED}, Instance{Deps: map[LogIndex]int{{Replica: 3, Index: 1}: 1, {Replica: 1, Index: 1}: 1}, Seq: 12, Command: "hi", Position: LogIndex{1, 2}, Status: COMMITTED}},
		//2: 2.0-> 3.1 3.0
		{Instance{Deps: map[LogIndex]int{{Replica: 3, Index: 0}: 1, {Replica: 3, Index: 1}: 1}, Seq: 3, Command: "bye", Position: LogIndex{2, 0}, Status: COMMITTED}},
		//3: 3.0-> 1.0, 3.1 -> 4.1
		{Instance{Deps: map[LogIndex]int{{Replica: 1, Index: 0}: 1}, Seq: 2, Command: "hi", Position: LogIndex{3, 0}, Status: COMMITTED}, Instance{Deps: map[LogIndex]int{{Replica: 4, Index: 1}: 1}, Seq: 5, Command: "hi", Position: LogIndex{3, 1}, Status: COMMITTED}},
		//4: 4.0->1.0, 4.1->3.0
		{Instance{Deps: map[LogIndex]int{{Replica: 1, Index: 0}: 1}, Seq: 8, Command: "hi", Position: LogIndex{4, 0}, Status: COMMITTED}, Instance{Deps: map[LogIndex]int{{Replica: 3, Index: 0}: 1}, Seq: 4, Command: "hi", Position: LogIndex{4, 1}, Status: COMMITTED}},
	}
	totalEntries := 0
	for _, r := range graph {
		totalEntries += len(r)
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
	fmt.Println("PASSED #2")
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
		{Instance{Deps: map[LogIndex]int{{Replica: 1, Index: 2}: 1, {Replica: 1, Index: 0}: 1}, Seq: 1, Command: "", Position: LogIndex{0, 0}, Status: COMMITTED}},
		//1: 1.0 -> 2.0, 1.1->1.2, 1.2-> 1.1,3.1
		{Instance{Deps: map[LogIndex]int{{Replica: 2, Index: 0}: 1}, Seq: 7, Command: "hi", Position: LogIndex{1, 0}, Status: COMMITTED}, Instance{Deps: map[LogIndex]int{{1, 2}: 1}, Seq: 6, Command: "hi", Position: LogIndex{1, 1}, Status: COMMITTED}, Instance{Deps: map[LogIndex]int{{3, 1}: 1, {1, 1}: 1}, Seq: 12, Command: "hi", Position: LogIndex{1, 2}, Status: COMMITTED}},
		//2: 2.0-> 3.1 3.0
		{Instance{Deps: map[LogIndex]int{{Replica: 3, Index: 0}: 1, {Replica: 3, Index: 1}: 1}, Seq: 3, Command: "bye", Position: LogIndex{2, 0}, Status: COMMITTED}},
		//3: 3.0-> 1.0, 3.1 -> 4.1
		{Instance{Deps: map[LogIndex]int{{Replica: 1, Index: 0}: 1}, Seq: 2, Command: "hi", Position: LogIndex{3, 0}, Status: COMMITTED}, Instance{Deps: map[LogIndex]int{{Replica: 4, Index: 1}: 1}, Seq: 5, Command: "hi", Position: LogIndex{3, 1}, Status: COMMITTED}},
		//4: 4.0->1.0, 4.1->3.0
		{Instance{Deps: map[LogIndex]int{{Replica: 1, Index: 0}: 1}, Seq: 8, Command: "hi", Position: LogIndex{4, 0}, Status: COMMITTED}, Instance{Deps: map[LogIndex]int{{Replica: 3, Index: 0}: 1}, Seq: 4, Command: "hi", Position: LogIndex{4, 1}, Status: COMMITTED}},
	}
	totalEntries := 0
	for _, r := range graph {
		totalEntries += len(r)
	}
	e.lastApplied = []int{-1, -1, -1, -1, -1}
	e.log = graph
	order := []LogIndex{}
	go e.scc(len(e.peers))
	cnt := 0
	go func() {
		time.Sleep(100 * time.Millisecond)
		e.lock.Lock()
		e.log[1][0].Status = COMMITTED
		e.lock.Unlock()
		time.Sleep(100 * time.Millisecond)
		e.lock.Lock()
		e.log[4][1].Status = COMMITTED
		e.lock.Unlock()
	}()
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
	fmt.Println("PASSED #2")
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
		{Instance{Deps: map[LogIndex]int{{Replica: 1, Index: 2}: 1, {Replica: 1, Index: 0}: 1}, Seq: 1, Command: "", Position: LogIndex{0, 0}, Status: EXECUTED}},
		//1: 1.0 -> 2.0, 1.1->1.2, 1.2-> 1.1,3.1
		{Instance{Deps: map[LogIndex]int{{Replica: 2, Index: 0}: 1}, Seq: 7, Command: "hi", Position: LogIndex{1, 0}, Status: EXECUTED}, Instance{Deps: map[LogIndex]int{{1, 2}: 1}, Seq: 6, Command: "hi", Position: LogIndex{1, 1}, Status: EXECUTED}, Instance{Deps: map[LogIndex]int{{3, 1}: 1, {1, 1}: 1}, Seq: 12, Command: "hi", Position: LogIndex{1, 2}, Status: EXECUTED}},
		//2: 2.0-> 3.1 3.0
		{Instance{Deps: map[LogIndex]int{{Replica: 3, Index: 0}: 1, {Replica: 3, Index: 1}: 1}, Seq: 3, Command: "bye", Position: LogIndex{2, 0}, Status: EXECUTED}},
		//3: 3.0-> 1.0, 3.1 -> 4.1
		{Instance{Deps: map[LogIndex]int{{Replica: 1, Index: 0}: 1}, Seq: 2, Command: "hi", Position: LogIndex{3, 0}, Status: EXECUTED}, Instance{Deps: map[LogIndex]int{{Replica: 4, Index: 1}: 1}, Seq: 5, Command: "hi", Position: LogIndex{3, 1}, Status: EXECUTED}},
		//4: 4.0->1.0, 4.1->3.0
		{Instance{Deps: map[LogIndex]int{{Replica: 1, Index: 0}: 1}, Seq: 8, Command: "hi", Position: LogIndex{4, 0}, Status: EXECUTED}, Instance{Deps: map[LogIndex]int{{Replica: 3, Index: 0}: 1}, Seq: 4, Command: "hi", Position: LogIndex{4, 1}, Status: EXECUTED}},
	}

	e.log = graph
	order := []LogIndex{{3, 0}, {2, 0}, {4, 1}, {3, 1}, {1, 0}, {1, 1}, {1, 2}, {0, 0}}
	if !test_scc(n, &e, order) {
		t.Fail()
	}
}
