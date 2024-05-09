package epaxos

import (
	"fmt"
	"sort"
	"time"
)

const SLEEP = 10 * time.Millisecond

func (e *EPaxos) execute() {
	for !e.killed() {
		if !e.scc(len(e.peers)) {
			time.Sleep(SLEEP)
			//	e.debug(topicExecute, "waiting to execute")
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
	for instance := range e.log[replica][curr].Deps {
		//We wait if the dependency is not committed yet
		//	e.debug(topicExecute, "waiting for %v", instance)
		for len(e.log[instance.Replica]) <= instance.Index || e.log[instance.Replica][instance.Index].Status < COMMITTED {
			//	e.debug(topic("DEBUG"), "waiting for %v status %v", instance, e.log[instance.Replica][instance.Index].Status)
			e.lock.Unlock()
			//ADJUST
			time.Sleep(SLEEP)
			e.lock.Lock()
		}
		for instance.Index >= len(disc[instance.Replica]) {
			disc[instance.Replica] = append(disc[instance.Replica], -1)
			inStack[instance.Replica] = append(inStack[instance.Replica], false)
			parents[instance.Replica] = append(parents[instance.Replica], Instance{})
			low[instance.Replica] = append(low[instance.Replica], -1)
			sccs[instance.Replica] = append(sccs[instance.Replica], -1)
		}
		if e.log[instance.Replica][instance.Index].Status == EXECUTED {
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
		//e.debug(topicExecute, "head %v index %v: \n", replica, curr)
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
			// fmt.Printf("sorted instances: %v\n", sorted)
			if e.log[instance.Position.Replica][instance.Position.Index].Status == EXECUTED {
				fmt.Printf("ERROR, already executed%v.%v \n", instance.Position.Replica, instance.Position.Index)
				break
			}
			// fmt.Printf("Executing %v.%v in e.me %v: %v \n", instance.Position.Replica, instance.Position.Index, e.me, instance)
			e.debug(topicExecute, "about to execute %v.%v: %v", instance.Position.Replica, instance.Position.Index, instance)
			e.lock.Unlock()
			e.applyCh <- instance
			e.debug(topicExecute, "Executed instance %v", instance.Position)
			e.lock.Lock()

			e.log[instance.Position.Replica][instance.Position.Index].Status = EXECUTED

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
		//e.debug(topicInfo, "%v Last applied: %v", len(e.log[R]), e.lastApplied[R])
		for i := e.lastApplied[R] + 1; i < len(e.log[R]); i++ {
			if e.log[R][i].Status == EXECUTED {
				e.lastApplied[R] += 1
				continue
			}
			for i >= len(disc[R]) {
				disc[R] = append(disc[R], -1)
				inStack[R] = append(inStack[R], false)
				parents[R] = append(parents[R], Instance{})
				low[R] = append(low[R], -1)
				sccs[R] = append(sccs[R], -1)
			}
			if disc[R][i] == -1 {
				//	e.debug(topicInfo, "waiting for %v status %v", LogIndex{R, i}, e.log[R][i].Status)
				for e.log[R][i].Status < COMMITTED {

					e.lock.Unlock()
					//	fmt.Printf("waiting for %v status %v", instance, e.status[instance.Replica][instance.Index])
					//ADJUST
					time.Sleep(SLEEP)
					e.lock.Lock()
				}
				// fmt.Printf("calling execDfs for %v status %v", LogIndex{R, i}, e.log[R][i].Status)
				// e.debug(topicInfo, "finished waiting for %v status %v", LogIndex{R, i}, e.log[R][i].Status)
				executed = e.execDFs(R, i, disc, low, &stack, inStack, parents, &Time, sccs, &counter) || executed
			}
		}
	}
	return executed
}
