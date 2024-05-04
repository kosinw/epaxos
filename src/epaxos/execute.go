package epaxos

import (
	"fmt"
	"sort"
	"time"
)

func (e *EPaxos) execute() {
	for !e.killed() {
		if !e.scc(len(e.peers)) {
			time.Sleep(1000)
		}
		// for server := 0; server < len(e.peers); server++ {

		// 	if e.lastApplied[server] < len(e.log[server]) {
		// 		for index :=e.lastApplied[ser]

		// 	}

		// }
	}
}

// Finds the SCCs and executes instances in those SCCs
func (e *EPaxos) execDFs(replica int, curr int, disc [][]int, low [][]int, stack *[]LogIndex, inStack [][]bool, parents [][]Instance, Time *int, sccs [][]int, counter *int) bool {
	executed := false
	//  visited[curr]=true;
	//fmt.Printf("visiting replica %v index %v \n", replica, curr)
	low[replica][curr] = *Time
	disc[replica][curr] = low[replica][curr]
	*Time = *Time + 1
	*stack = append(*stack, LogIndex{replica, curr})
	inStack[replica][curr] = true
	for instance, _ := range e.log[replica][curr].Deps {
		//We wait if the dependency is not committed yet
		for len(e.log[instance.Replica]) <= instance.Index || e.log[instance.Replica][instance.Index].Status < Committed {
			e.lock.Unlock()
			//	fmt.Printf("waiting for %v status %v", instance, e.status[instance.Replica][instance.Index])
			//ADJUST
			time.Sleep(1000)
			e.lock.Lock()
		}

		if e.log[instance.Replica][instance.Index].Status == Executed {
			continue
		}
		// if(!visited[edge.first]){
		if disc[instance.Replica][instance.Index] == -1 {
			parents[instance.Replica][instance.Index] = e.log[instance.Replica][instance.Index]
			executed = e.execDFs(instance.Replica, instance.Index, disc, low, stack, inStack, parents, Time, sccs, counter) || executed
			low[replica][curr] = min(low[replica][curr], low[instance.Replica][instance.Index])
		} else if inStack[instance.Replica][instance.Index] {
			low[replica][curr] = min(low[replica][curr], disc[instance.Replica][instance.Index])
		}
	}
	//if curr is head of its own subtree it is end of connected component
	if disc[replica][curr] == low[replica][curr] {
		//	fmt.Printf("head %v index %v: \n", replica, curr)
		sorted := make([]Instance, 0)
		c := (*stack)[len(*stack)-1]
		for {
			//            cout<<c<<" ";
			sccs[c.Replica][c.Index] = *counter
			inStack[c.Replica][c.Index] = false
			sorted = append(sorted, e.log[c.Replica][c.Index])
			*stack = (*stack)[:len(*stack)-1]
			//	fmt.Printf("in comp: %v \n", c)
			if c.Replica == replica && c.Index == curr {
				break
			}
			c = (*stack)[len(*stack)-1]

		}
		less := func(i, j int) bool {
			return sorted[i].Seq < sorted[j].Seq
		}
		sort.Slice(sorted, less)
		for _, instance := range sorted {

			if e.log[instance.Position.Replica][instance.Position.Index].Status == Executed {
				fmt.Printf("ERROR, already executed%v.%v \n", instance.Position.Replica, instance.Position.Index)
				break
			}
			//	fmt.Printf("Executing %v.%v \n", instance.Position.Replica, instance.Position.Index)
			e.lock.Unlock()
			e.applyCh <- instance
			e.lock.Lock()
			e.log[instance.Position.Replica][instance.Position.Index].Status = Executed
			executed = true
		}
		// sccs[c.Replica][c.Index] = *counter
		// *counter = *counter + 1
	}
	return executed
}

// finds all strongly connected components in DIRECTED graph
func (e *EPaxos) scc(n int) bool {
	e.lock.Lock()
	defer e.lock.Unlock()
	Time := 0
	low := make([][]int, n)
	inStack := make([][]bool, n)
	disc := make([][]int, n)
	parents := make([][]Instance, n)
	sccs := make([][]int, n)
	stack := make([]LogIndex, 0)
	counter := 0
	for R := 0; R < n; R++ {
		low[R] = make([]int, len(e.log[R]))
		inStack[R] = make([]bool, len(e.log[R]))
		disc[R] = make([]int, len(e.log[R]))
		parents[R] = make([]Instance, len(e.log[R]))
		sccs[R] = make([]int, len(e.log[R]))
		for i := 0; i < len(e.log[R]); i++ {
			disc[R][i] = -1
			parents[R][i] = Instance{}
		}

	}
	executed := false
	for R := 0; R < n; R++ {
		for i := e.lastApplied[R] + 1; i < len(e.log[R]); i++ {
			if e.log[R][i].Status == Executed {
				e.lastApplied[R] += 1
				continue
			}
			if disc[R][i] == -1 {

				executed = e.execDFs(R, i, disc, low, &stack, inStack, parents, &Time, sccs, &counter) || executed
			}

		}
	}
	return executed
}
