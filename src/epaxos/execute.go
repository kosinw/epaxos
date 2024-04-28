package epaxos

import (
	"fmt"
	"sort"
	"time"
)

func (e *EPaxos) killed() bool {
	return false
	//z := atomic.LoadInt32(&rf.dead)
	//return z == 1
}

func (e *EPaxos) execute() {
	for !e.killed() {
		e.scc(len(e.peers))
		// for server := 0; server < len(e.peers); server++ {

		// 	if e.lastApplied[server] < len(e.log[server]) {
		// 		for index :=e.lastApplied[ser]

		// 	}

		// }
	}
}

// Finds the SCCs and executes instances in those SCCs
func (e *EPaxos) execDFs(replica int, curr int, disc [][]int, low [][]int, stack *[]LogIndex, inStack [][]bool, parents [][]Instance, Time *int, sccs [][]int, counter *int) {
	//  visited[curr]=true;
	fmt.Printf("visiting replica %v index %v \n", replica, curr)
	low[replica][curr] = *Time
	disc[replica][curr] = low[replica][curr]
	*Time = *Time + 1
	*stack = append(*stack, LogIndex{replica, curr})
	inStack[replica][curr] = true
	for _, instance := range e.log[replica][curr].Deps {
		//We wait if the dependency is not committed yet
		for len(e.log[instance.Replica]) <= instance.Index || e.status[instance.Replica][instance.Index] < COMMITTED {
			e.lock.Unlock()
			fmt.Printf("waiting for %v status %v", instance, e.status[instance.Replica][instance.Index])
			//ADJUST
			time.Sleep(1000 * 1000)
			e.lock.Lock()
		}
		if e.status[instance.Replica][instance.Index] == EXECUTED {
			continue
		}
		// if(!visited[edge.first]){
		if disc[instance.Replica][instance.Index] == -1 {
			parents[instance.Replica][instance.Index] = e.log[instance.Replica][instance.Index]
			e.execDFs(instance.Replica, instance.Index, disc, low, stack, inStack, parents, Time, sccs, counter)
			low[replica][curr] = min(low[replica][curr], low[instance.Replica][instance.Index])
		} else if inStack[instance.Replica][instance.Index] {
			low[replica][curr] = min(low[replica][curr], disc[instance.Replica][instance.Index])
		}
	}
	//if curr is head of its own subtree it is end of connected component
	if disc[replica][curr] == low[replica][curr] {
		fmt.Printf("head %v index %v: \n", replica, curr)
		sorted := make([]Instance, 0)
		c := (*stack)[len(*stack)-1]
		for {
			//            cout<<c<<" ";
			sccs[c.Replica][c.Index] = *counter
			inStack[c.Replica][c.Index] = false
			sorted = append(sorted, e.log[c.Replica][c.Index])
			*stack = (*stack)[:len(*stack)-1]
			fmt.Printf("in comp: %v \n", c)
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

			if e.status[instance.Position.Replica][instance.Position.Index] == EXECUTED {
				fmt.Printf("ERROR, already executed%v.%v \n", instance.Position.Replica, instance.Position.Index)
				break
			}
			fmt.Printf("Executing %v.%v \n", instance.Position.Replica, instance.Position.Index)
			e.lock.Unlock()
			e.applyCh <- instance
			e.lock.Lock()
			e.status[instance.Position.Replica][instance.Position.Index] = EXECUTED
		}
		// sccs[c.Replica][c.Index] = *counter
		// *counter = *counter + 1
	}
}

// finds all strongly connected components in DIRECTED graph
func (e *EPaxos) scc(n int) {
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

	for R := 0; R < n; R++ {
		for i := e.lastApplied[R] + 1; i < len(e.log[R]); i++ {
			//  if(!visited[i]){
			if disc[R][i] == -1 {

				e.execDFs(R, i, disc, low, &stack, inStack, parents, &Time, sccs, &counter)
			} else {
				fmt.Printf("visited replica %v index %v \n", R, i)
			}
		}
	}
}