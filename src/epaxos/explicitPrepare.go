package epaxos

func (e *EPaxos) explicitPrepare(position LogIndex) {
	e.lock.Lock()
	e.myBallot += 1
	for i := 0; i < len(e.peers); i++ {

		go e.broadcastPrepare(i, position)
	}
}
func (e *EPaxos) broadcastPrepare(replica int, position LogIndex) {
	for !e.killed() {
		//	args := PrepareArgs{Position: position, NewBallot: {}}
		//reply := PrepareReply{}
		//		ok := e.peers[replica].Call("EPaxos.Prepare", args, reply)

	}
}
func (e *EPaxos) Prepare(args *PrepareArgs, reply *PrepareReply) {

}
func (e *EPaxos) timeoutChecker() {

}
