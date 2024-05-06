package epaxos

import (
	"sync"
	"time"
)

func (e *EPaxos) ExplicitPreparer() {
	for !e.killed() {
		i := 0
		for {
			//we restart if reached the largest index i
			passes := false //flag if all logs are less length than i
			e.lock.Lock()
			for replica := 0; replica < len(e.peers); replica++ {
				if replica == e.me {
					continue
				}
				//	if replica == 0 {
				//		e.debug(topicPrepare, " acknow %v len: %v", i, len(e.log[replica]))
				//	}
				if i >= len(e.log[replica]) {
					continue
				}
				passes = true
				if e.log[replica][i].Status < COMMITTED && e.log[replica][i].Timer != (time.Time{}) && time.Since(e.log[replica][i].Timer) > 500*time.Millisecond {
					e.debug(topicPrepare, "time %v now %v %v preparing %v replica %v\n", e.log[replica][i].Timer, time.Now(), e.me, e.log[replica][i].Position, replica)
					//fmt.Printf("%v preparing %v replica %v\n", e.me, e.log[replica][i].Position, replica)
					e.log[replica][i].Timer = time.Time{}
					position := e.log[replica][i].Position
					e.lock.Unlock()
					go e.explicitPrepare(position)
					e.lock.Lock()
				}
			}
			e.lock.Unlock()
			if !passes {
				time.Sleep(10 * time.Millisecond)
				break
			}
			i++
		}
	}
}

func (e *EPaxos) explicitPrepare(position LogIndex) {
	e.lock.Lock()
	e.log[position.Replica][position.Index].Ballot = Ballot{BallotNum: e.log[position.Replica][position.Index].Ballot.BallotNum + 1,
		ReplicaNum: e.me}
	newBallot := e.log[position.Replica][position.Index].Ballot
	e.lock.Unlock()
	e.broadcastPrepare(position, newBallot)
}
func (e *EPaxos) broadcastPrepare(position LogIndex, newBallot Ballot) (abort bool) {
	majority := e.numPeers()/2 + 1
	replyCount := 0
	rejectCount := 0
	accepts := make([]Instance, 0)
	originalAccept := false //if received a succesful reply from the original commandleader
	preaccepts := make([]Instance, 0)
	commits := make([]Instance, 0)
	lk := sync.NewCond(new(sync.Mutex))

	args := PrepareArgs{
		Position:  position,
		NewBallot: newBallot,
	}
	defer func() {
		e.lock.Lock()
		e.log[position.Replica][position.Index].Preparing = false
		e.lock.Unlock()
	}()

	for i := 0; i < e.numPeers(); i++ {

		go func(peer int, original int) {

			reply := PrepareReply{}

			for !e.sendPrepare(peer, &args, &reply) {
				reply = PrepareReply{}
			}

			lk.L.Lock()
			defer lk.L.Unlock()

			replyCount++

			if reply.Success {
				//	fmt.Printf("%v prepares for peer %v %v: %v\n", peer, e.me, position, reply)
				if peer == original {
					originalAccept = true
				}
				if reply.CurrentInstance.Status == PREACCEPTED {
					preaccepts = append(preaccepts, reply.CurrentInstance)
				} else if reply.CurrentInstance.Status == ACCEPTED {
					accepts = append(accepts, reply.CurrentInstance)
				} else if reply.CurrentInstance.Status >= COMMITTED {
					commits = append(commits, reply.CurrentInstance)
				}
			} else {
				rejectCount++
			}

			lk.Broadcast()
		}(i, position.Replica)
	}

	lk.L.Lock()
	defer lk.L.Unlock()

	for !e.killed() {
		lk.Wait()

		// Check if any of our peers have rejected our request
		if rejectCount > 0 {
			e.debug(topicPreAccept, "Stepping down as explicit prepare leader for %v...", position)
			abort = true
			break
		}

		if replyCount < majority {
			continue
		}
		e.debug(topicPrepare, "%v received prepare replies for %v: preaccepts: %v accepts: %v commits: %v\n",
			e.me, position, len(preaccepts), len(accepts), len(commits))
		//	fmt.Printf("%v received prepare replies for %v: preaccepts: %v accepts: %v commits: %v\n",
		//	e.me, position, len(preaccepts), len(accepts), len(commits))
		if len(commits) > 0 {
			e.lock.Lock()
			e.log[position.Replica][position.Index].Deps = commits[0].Deps
			e.log[position.Replica][position.Index].Seq = commits[0].Seq
			e.log[position.Replica][position.Index].Status = COMMITTED
			instance := e.log[position.Replica][position.Index]
			//fmt.Printf("%v committing %v: commit path\n", e.me, position)
			e.lock.Unlock()
			_ = e.broadcastCommit(instance)
			e.debug(topicPrepare, "%v committed %v: commit path\n", e.me, position)
		} else if len(accepts) > 0 {
			e.lock.Lock()
			e.log[position.Replica][position.Index].Deps = commits[0].Deps
			e.log[position.Replica][position.Index].Seq = commits[0].Seq
			e.log[position.Replica][position.Index].Status = COMMITTED
			instance := e.log[position.Replica][position.Index]
			e.lock.Unlock()
			abort = e.broadcastAccept(instance)
			if !abort {
				_ = e.broadcastCommit(instance)
			}
		} else if len(preaccepts) >= majority && !originalAccept {
			e.lock.Lock()
			union := make(map[LogIndex]int)
			lens := len(preaccepts[0].Deps)
			wrongLen := false
			for _, instance := range preaccepts {
				union = unionMaps(union, instance.Deps)
				if len(instance.Deps) != lens {
					wrongLen = true
					break
				}
			}

			if !wrongLen && mapsEqual(union, preaccepts[0].Deps) {
				instance := e.log[position.Replica][position.Index]
				e.lock.Unlock()
				abort = e.broadcastAccept(instance)
				if !abort {
					_ = e.broadcastCommit(instance)
				}
			} else {
				e.lock.Unlock()
				e.debug(topicPrepare, "%v trying to preaccept %v\n", e.me, position)
				e.processRequest(preaccepts[0].Command, position, true)
				e.debug(topicPrepare, "through preaccept%v committed %v\n", e.me, position)
			}
		} else if len(preaccepts) > 0 {
			e.debug(topicPrepare, "%v trying to preaccept %v\n", e.me, position)
			e.processRequest(preaccepts[0].Command, position, true)
			e.debug(topicPrepare, "through preaccept %v committed %v\n", e.me, position)

		} else {
			e.debug(topicPrepare, "%v trying to preaccept %v: NOP\n", e.me, position)
			e.processRequest(NOP, position, true)
			e.debug(topicPrepare, "%v committed %v: NOP\n", e.me, position)
		}

	}
	return
}
func (e *EPaxos) sendPrepare(server int, args *PrepareArgs, reply *PrepareReply) bool {
	e.debug(topicPrepare, "Calling %v.Prepare...: %v", replicaName(server), args.Position)
	ok := false
	if server != e.me {
		ok = e.peers[server].Call("EPaxos.Prepare", args, reply)
	} else {
		ok = true
		e.Prepare(args, reply)
	}
	if ok {
		e.debug(topicPrepare, "Finishing %v.Prepare...: %v", replicaName(server), reply.Success)
	} else {
		e.debug(topicPrepare, "Dropping %v.Prepare...", replicaName(server))
	}
	return ok
}
func (e *EPaxos) Prepare(args *PrepareArgs, reply *PrepareReply) {
	e.lock.Lock()
	defer e.lock.Unlock()
	if len(e.log[args.Position.Replica]) <= args.Position.Index {
		reply.Success = true
		reply.CurrentInstance = Instance{}
		reply.CurrentInstance.Valid = false
	} else {
		ballot := e.log[args.Position.Replica][args.Position.Index].Ballot
		if ballot.le(args.NewBallot) {
			reply.Success = true
			reply.CurrentInstance = e.log[args.Position.Replica][args.Position.Index]
		} else {
			reply.Success = false
		}
	}
}
func (e *EPaxos) timeoutChecker() {

}
