package epaxos

import "fmt"

func dfsSCC(curr int, disc []int, low []int, stack []int, inStack []bool, parents []int, Time *int, sccs []int, counter *int, graph [][]int) {
	//  visited[curr]=true;
	low[curr] = *Time
	disc[curr] = low[curr]
	*Time = *Time + 1
	stack = append(stack, curr)
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
		c := stack[len(stack)-1]
		for c != curr {
			//            cout<<c<<" ";
			sccs[c] = *counter
			inStack[c] = false
			stack = stack[:len(stack)-1]
			c = stack[len(stack)-1]
		}
		sccs[c] = *counter
		*counter = *counter + 1
		//cout<<c<<"\n";
		stack = stack[:len(stack)-1]
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
			dfsSCC(i, disc, low, stack, inStack, parents, &Time, sccs, &counter, graph)
		}
	}
	return sccs
}
func main() {
	graph := [][]int{
		{1},
		{3},
		{0},
		{0},
	}
	sccs := scc(len(graph), graph)
	for i, comp := range sccs {
		fmt.Println(i, ":", comp)
	}
}
