package epaxos

func dfsSCC(curr int, disc []int, low []int, stack *[]int, inStack []bool, parents []int, Time *int, sccs []int, counter *int, graph [][]int) {
	//  visited[curr]=true;
	low[curr] = *Time
	disc[curr] = low[curr]
	*Time = *Time + 1
	(*stack) = append((*stack), curr)
	inStack[curr] = true
	for _, edge := range graph[curr] {
		// if(!visited[edge.first]){
		if disc[edge] == -1 {
			parents[edge] = curr
			dfsSCC(edge, disc, low, stack, inStack, parents, Time, sccs, counter, graph)
			low[curr] = min(low[curr], low[edge])
		} else if inStack[edge] {
			low[curr] = min(low[curr], disc[edge])
		}
	}
	//if curr is head of its own subtree it is end of connected component
	if disc[curr] == low[curr] {
		c := (*stack)[len((*stack))-1]
		for c != curr {
			//            cout<<c<<" ";
			sccs[c] = *counter
			inStack[c] = false
			(*stack) = (*stack)[:len((*stack))-1]
			c = (*stack)[len((*stack))-1]
		}
		sccs[c] = *counter
		*counter = *counter + 1
		//cout<<c<<"\n";
		(*stack) = (*stack)[:len((*stack))-1]
		inStack[curr] = false
	}
}

// finds all strongly connected components in DIRECTED graph
func scc(n int, graph [][]int) []int {
	Time := 0
	low := make([]int, n)
	inStack := make([]bool, n)
	disc := make([]int, n)
	parents := make([]int, n)
	sccs := make([]int, n)
	stack := make([]int, 0)
	counter := 0
	for i := 0; i < n; i++ {
		disc[i] = -1
		parents[i] = -1
	}

	for i := 0; i < n; i++ {
		//  if(!visited[i]){
		if disc[i] == -1 {
			dfsSCC(i, disc, low, &stack, inStack, parents, &Time, sccs, &counter, graph)
		}
	}
	return sccs
}
func dfsSCCGraph(scc int, graph [][]bool, visited []bool) []bool {
	visited[scc] = true
	dependencies := make([]bool, len(graph))
	for edge, ok := range graph[scc] {

		if edge != scc && ok && !visited[edge] {
			dependencies[edge] = true
			for scc2, ok := range dfsSCCGraph(edge, graph, visited) {
				dependencies[scc2] = dependencies[scc2] || ok
			}
		}
	}
	return dependencies
}
func test_scc(n int, e *EPaxos, order []LogIndex) bool {
	//10 edges per vertex max
	mxedges := 10
	graph := make([][]int, len(e.peers)*mxedges)
	for r := 0; r < len(e.peers); r++ {
		for i := 0; i < len(e.log[r]); i++ {
			graph[r*mxedges+i] = make([]int, len(e.log[r][i].Deps))
			j := 0
			for dep, _ := range e.log[r][i].Deps {
				//	fmt.Printf("instance %v.%v dep: %v.%v\n", r, i, dep.Replica, dep.Index)
				graph[r*mxedges+i][j] = dep.Replica*mxedges + dep.Index
				j += 1
			}
		}
	}
	sccs := scc(len(e.peers)*mxedges, graph)
	sccgraph := make([][]bool, len(sccs))
	for scc := 0; scc < len(sccs); scc++ {
		sccgraph[scc] = make([]bool, len(sccs))
	}
	for r := 0; r < len(e.peers); r++ {
		for i := 0; i < len(e.log[r]); i++ {
			//	fmt.Printf("instance %v.%v scc: %v\n", r, i, sccs[r*mxedges+i])
			for dep, _ := range e.log[r][i].Deps {

				sccgraph[sccs[r*mxedges+i]][sccs[dep.Replica*mxedges+dep.Index]] = true
			}
		}
	}
	for r := 0; r < len(sccs); r++ {

		visited := make([]bool, len(sccs))
		sccgraph[r] = dfsSCCGraph(r, sccgraph, visited)
	}
	for i, instance := range order {
		for j := i + 1; j < len(order); j++ {
			if sccs[instance.Replica*mxedges+instance.Index] == sccs[order[j].Replica*mxedges+order[j].Index] {
				if e.log[instance.Replica][instance.Index].Seq > e.log[order[j].Replica][order[j].Index].Seq {
					//		fmt.Printf("instance %v.%v goes first but has same scc but has greater seqnum than %v.%v\n", instance.Replica, instance.Index,
					//				order[j].Replica, order[j].Index)
					return false
				}
			} else if sccgraph[sccs[instance.Replica*mxedges+instance.Index]][sccs[order[j].Replica*mxedges+order[j].Index]] {
				//		fmt.Printf("instance %v.%v goes first but depends on %v.%v\n", instance.Replica, instance.Index, order[j].Replica, order[j].Index)
				return false
			}
		}
	}
	return true
}

//func main() {
// 	graph := [][]int{
// 		{1},
// 		{3},
// 		{0},
// 		{0},
// 	}
// 	sccs := scc(len(graph), graph)
// 	for i, comp := range sccs {
// 		fmt.Println(i, ":", comp)
// 	}
// }
