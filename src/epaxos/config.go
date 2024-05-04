package epaxos

//
// support for EPaxos tester.
//

import (
	"log"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"6.5840/labrpc"

	crand "crypto/rand"
	"encoding/base64"
	"fmt"
	"math/big"
	"time"
)

func randstring(n int) string {
	b := make([]byte, 2*n)
	crand.Read(b)
	s := base64.URLEncoding.EncodeToString(b)
	return s[0:n]
}

func makeSeed() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := crand.Int(crand.Reader, max)
	x := bigx.Int64()
	return x
}

type config struct {
	mu        sync.Mutex
	t         *testing.T
	finished  int32
	net       *labrpc.Network
	n         int
	peers     []*EPaxos
	applyErr  []string // from apply channel readers
	connected []bool   // whether each server is on the net
	saved     []*Persister
	endnames  [][]string // the port file names each sends to
	// logs        []map[int]interface{} // copy of each server's committed entries
	logs [][]map[int]interface{}
	// lastApplied []int
	start time.Time // time at which make_config() was called
	// begin()/end() statistics
	t0         time.Time // time at which test_test.go called cfg.begin()
	rpcs0      int       // rpcTotal() at start of test
	cmds0      int       // number of agreements
	bytes0     int64
	maxIndex   []int
	maxIndex0  []int
	interferes func(cmd1, cdm2 interface{}) bool
}

var ncpu_once sync.Once

func make_config(t *testing.T, n int, unreliable bool, interferes func(cmd1, cmd2 interface{}) bool) *config {
	ncpu_once.Do(func() {
		if runtime.NumCPU() < 2 {
			fmt.Printf("warning: only one CPU, which may conceal locking bugs\n")
		}
		rand.Seed(makeSeed())
	})
	runtime.GOMAXPROCS(4)
	cfg := &config{}
	cfg.t = t
	cfg.net = labrpc.MakeNetwork()
	cfg.n = n
	cfg.interferes = interferes
	cfg.applyErr = make([]string, cfg.n)
	cfg.peers = make([]*EPaxos, cfg.n)
	cfg.connected = make([]bool, cfg.n)
	cfg.saved = make([]*Persister, cfg.n)
	cfg.endnames = make([][]string, cfg.n)
	cfg.logs = make([][]map[int]interface{}, cfg.n)
	// cfg.lastApplied = make([]int, cfg.n)
	cfg.maxIndex = make([]int, cfg.n)
	cfg.maxIndex0 = make([]int, cfg.n)
	cfg.start = time.Now()

	cfg.setunreliable(unreliable)

	cfg.net.LongDelays(true)

	applier := cfg.applier
	// if snapshot {
	// 	applier = cfg.applierSnap
	// }
	// create a full set of peers.
	for i := 0; i < cfg.n; i++ {
		cfg.logs[i] = make([]map[int]interface{}, cfg.n)

		for j := 0; j < cfg.n; j++ {
			cfg.logs[i][j] = map[int]interface{}{}
		}

		cfg.start1(i, applier)
	}

	// connect everyone
	for i := 0; i < cfg.n; i++ {
		cfg.connect(i)
	}

	return cfg
}

// shut down a server but save its persistent state.
func (cfg *config) crash1(i int) {
	cfg.disconnect(i)
	cfg.net.DeleteServer(i) // disable client connections to the server.

	cfg.mu.Lock()
	defer cfg.mu.Unlock()

	// a fresh persister, in case old instance
	// continues to update the Persister.
	// but copy old persister's content so that we always
	// pass Make() the last persisted state.
	if cfg.saved[i] != nil {
		cfg.saved[i] = cfg.saved[i].Copy()
	}

	rf := cfg.peers[i]
	if rf != nil {
		cfg.mu.Unlock()
		rf.Kill()
		cfg.mu.Lock()
		cfg.peers[i] = nil
	}

	if cfg.saved[i] != nil {
		epaxoslog := cfg.saved[i].ReadEPaxosState()
		snapshot := cfg.saved[i].ReadSnapshot()
		cfg.saved[i] = &Persister{}
		cfg.saved[i].Save(epaxoslog, snapshot)
	}
}

func (cfg *config) checkLogs(peer int, m Instance) (string, bool) {
	err_msg := ""
	v := m.Command
	R := m.Position.Replica
	i := m.Position.Index

	// Here we should check for log inconsistencies at instance R.i at
	// every peer
	for j := 0; j < len(cfg.logs); j++ {
		if old, oldok := cfg.logs[j][R][i]; oldok && old != v {
			log.Printf("%v: log %v; server %v\n", peer, cfg.logs[peer], cfg.logs[j])
			err_msg = fmt.Sprintf("commit instance=%v server=%v %v != server=%v %v",
				m.Position, peer, v, j, old)
		}
	}

	_, prevok := cfg.logs[peer][R][i-1]
	cfg.logs[peer][R][i] = v
	if i > cfg.maxIndex[R] {
		cfg.maxIndex[R] = i
	}
	return err_msg, prevok
}

// applier reads message from apply ch and checks that they match the log
// contents
func (cfg *config) applier(i int, applyCh chan Instance) {
	for m := range applyCh {
		cfg.mu.Lock()
		err_msg, prevok := cfg.checkLogs(i, m)
		cfg.mu.Unlock()
		if m.Position.Index > 1 && prevok == false {
			err_msg = fmt.Sprintf("server %v execute instance out of order %v", i, m.Position)
		}
		if err_msg != "" {
			log.Fatalf("apply error: %v", err_msg)
			cfg.applyErr[i] = err_msg
		}
	}
}

// returns "" or error string
// func (cfg *config) ingestSnap(i int, snapshot []byte, index int) string {
// 	if snapshot == nil {
// 		log.Fatalf("nil snapshot")
// 		return "nil snapshot"
// 	}
// 	r := bytes.NewBuffer(snapshot)
// 	d := labgob.NewDecoder(r)
// 	var lastIncludedIndex int
// 	var xlog []interface{}
// 	if d.Decode(&lastIncludedIndex) != nil ||
// 		d.Decode(&xlog) != nil {
// 		log.Fatalf("snapshot decode error")
// 		return "snapshot Decode() error"
// 	}
// 	if index != -1 && index != lastIncludedIndex {
// 		err := fmt.Sprintf("server %v snapshot doesn't match m.SnapshotIndex", i)
// 		return err
// 	}
// 	cfg.logs[i] = map[int]interface{}{}
// 	for j := 0; j < len(xlog); j++ {
// 		cfg.logs[i][j] = xlog[j]
// 	}
// 	cfg.lastApplied[i] = lastIncludedIndex
// 	return ""
// }

// const SnapShotInterval = 10

// periodically snapshot state
// func (cfg *config) applierSnap(i int, applyCh chan ApplyMsg) {
// 	cfg.mu.Lock()
// 	rf := cfg.peers[i]
// 	cfg.mu.Unlock()
// 	if rf == nil {
// 		return // ???
// 	}

// 	for m := range applyCh {
// 		err_msg := ""
// 		if m.SnapshotValid {
// 			cfg.mu.Lock()
// 			err_msg = cfg.ingestSnap(i, m.Snapshot, m.SnapshotIndex)
// 			cfg.mu.Unlock()
// 		} else if m.CommandValid {
// 			if m.CommandIndex != cfg.lastApplied[i]+1 {
// 				err_msg = fmt.Sprintf("server %v apply out of order, expected index %v, got %v", i, cfg.lastApplied[i]+1, m.CommandIndex)
// 			}

// 			if err_msg == "" {
// 				cfg.mu.Lock()
// 				var prevok bool
// 				err_msg, prevok = cfg.checkLogs(i, m)
// 				cfg.mu.Unlock()
// 				if m.CommandIndex > 1 && prevok == false {
// 					err_msg = fmt.Sprintf("server %v apply out of order %v", i, m.CommandIndex)
// 				}
// 			}

// 			cfg.mu.Lock()
// 			cfg.lastApplied[i] = m.CommandIndex
// 			cfg.mu.Unlock()

// 			if (m.CommandIndex+1)%SnapShotInterval == 0 {
// 				w := new(bytes.Buffer)
// 				e := labgob.NewEncoder(w)
// 				e.Encode(m.CommandIndex)
// 				var xlog []interface{}
// 				for j := 0; j <= m.CommandIndex; j++ {
// 					xlog = append(xlog, cfg.logs[i][j])
// 				}
// 				e.Encode(xlog)
// 				rf.Snapshot(m.CommandIndex, w.Bytes())
// 			}
// 		} else {
// 			// Ignore other types of ApplyMsg.
// 		}
// 		if err_msg != "" {
// 			log.Fatalf("apply error: %v", err_msg)
// 			cfg.applyErr[i] = err_msg
// 		}
// 	}
// }

// start or re-start a Peer.
// if one already exists, "kill" it first.
// allocate new outgoing port file names, and a new
// state persister, to isolate previous instance of
// this server. since we cannot really kill it.
func (cfg *config) start1(i int, applier func(int, chan Instance)) {
	cfg.crash1(i)

	// a fresh set of outgoing ClientEnd names.
	// so that old crashed instance's ClientEnds can't send.
	cfg.endnames[i] = make([]string, cfg.n)
	for j := 0; j < cfg.n; j++ {
		cfg.endnames[i][j] = randstring(20)
	}

	// a fresh set of ClientEnds.
	ends := make([]*labrpc.ClientEnd, cfg.n)
	for j := 0; j < cfg.n; j++ {
		ends[j] = cfg.net.MakeEnd(cfg.endnames[i][j])
		cfg.net.Connect(cfg.endnames[i][j], j)
	}

	cfg.mu.Lock()

	// cfg.lastApplied[i] = 0

	// a fresh persister, so old instance doesn't overwrite
	// new instance's persisted state.
	// but copy old persister's content so that we always
	// pass Make() the last persisted state.
	if cfg.saved[i] != nil {
		cfg.saved[i] = cfg.saved[i].Copy()

		// snapshot := cfg.saved[i].ReadSnapshot()
		// if snapshot != nil && len(snapshot) > 0 {
		// 	// mimic KV server and process snapshot now.
		// 	err := cfg.ingestSnap(i, snapshot, -1)
		// 	if err != "" {
		// 		cfg.t.Fatal(err)
		// 	}
		// }
	} else {
		cfg.saved[i] = MakePersister()
	}

	cfg.mu.Unlock()

	applyCh := make(chan Instance)

	ep := Make(ends, i, cfg.saved[i], applyCh, cfg.interferes)

	cfg.mu.Lock()
	cfg.peers[i] = ep
	cfg.mu.Unlock()

	go applier(i, applyCh)

	svc := labrpc.MakeService(ep)
	srv := labrpc.MakeServer()
	srv.AddService(svc)
	cfg.net.AddServer(i, srv)
}

func (cfg *config) checkTimeout() {
	// enforce a two minute real-time limit on each test
	if !cfg.t.Failed() && time.Since(cfg.start) > 120*time.Second {
		cfg.t.Fatal("test took longer than 120 seconds")
	}
}

func (cfg *config) checkFinished() bool {
	z := atomic.LoadInt32(&cfg.finished)
	return z != 0
}

func (cfg *config) cleanup() {
	atomic.StoreInt32(&cfg.finished, 1)
	for i := 0; i < len(cfg.peers); i++ {
		if cfg.peers[i] != nil {
			cfg.peers[i].Kill()
		}
	}
	cfg.net.Cleanup()
	cfg.checkTimeout()
}

// attach server i to the net.
func (cfg *config) connect(i int) {
	// fmt.Printf("connect(%d)\n", i)

	cfg.connected[i] = true

	// outgoing ClientEnds
	for j := 0; j < cfg.n; j++ {
		if cfg.connected[j] {
			endname := cfg.endnames[i][j]
			cfg.net.Enable(endname, true)
		}
	}

	// incoming ClientEnds
	for j := 0; j < cfg.n; j++ {
		if cfg.connected[j] {
			endname := cfg.endnames[j][i]
			cfg.net.Enable(endname, true)
		}
	}
}

// detach server i from the net.
func (cfg *config) disconnect(i int) {
	// fmt.Printf("disconnect(%d)\n", i)

	cfg.connected[i] = false

	// outgoing ClientEnds
	for j := 0; j < cfg.n; j++ {
		if cfg.endnames[i] != nil {
			endname := cfg.endnames[i][j]
			cfg.net.Enable(endname, false)
		}
	}

	// incoming ClientEnds
	for j := 0; j < cfg.n; j++ {
		if cfg.endnames[j] != nil {
			endname := cfg.endnames[j][i]
			cfg.net.Enable(endname, false)
		}
	}
}

func (cfg *config) rpcCount(server int) int {
	return cfg.net.GetCount(server)
}

func (cfg *config) rpcTotal() int {
	return cfg.net.GetTotalCount()
}

func (cfg *config) setunreliable(unrel bool) {
	cfg.net.Reliable(!unrel)
}

func (cfg *config) bytesTotal() int64 {
	return cfg.net.GetTotalBytes()
}

func (cfg *config) setlongreordering(longrel bool) {
	cfg.net.LongReordering(longrel)
}

// how many servers think a log entry has been executed?
func (cfg *config) nExecuted(index LogIndex) (int, interface{}) {
	count := 0
	var cmd interface{} = nil
	for i := 0; i < len(cfg.peers); i++ {
		if cfg.applyErr[i] != "" {
			cfg.t.Fatal(cfg.applyErr[i])
		}

		cfg.mu.Lock()
		cmd1, ok := cfg.logs[i][index.Replica][index.Index]
		cfg.mu.Unlock()

		if ok {
			if count > 0 && cmd != cmd1 {
				cfg.t.Fatalf("committed values do not match: index %v, %v, %v",
					index, cmd, cmd1)
			}
			count += 1
			cmd = cmd1
		}
	}
	return count, cmd
}

// wait for at least n servers to commit.
// but don't wait forever.
func (cfg *config) wait(index LogIndex, n int) interface{} {
	to := 10 * time.Millisecond
	for iters := 0; iters < 30; iters++ {
		nd, _ := cfg.nExecuted(index)
		if nd >= n {
			break
		}
		time.Sleep(to)
		if to < time.Second {
			to *= 2
		}
	}
	nd, cmd := cfg.nExecuted(index)
	if nd < n {
		cfg.t.Fatalf("only %d decided for index %d; wanted %d",
			nd, index, n)
	}
	return cmd
}

// do a complete agreement.
// it might choose the wrong leader initially,
// and have to re-submit after giving up.
// entirely gives up after about 10 seconds.
// indirectly checks that the servers agree on the
// same value, since nExecuted() checks this,
// as do the threads that read from applyCh.
// returns index.
// if retry==true, may submit the command multiple
// times, in case a leader fails just after Start().
// if retry==false, calls Start() only once, in order
// to simplify the early Lab 3B tests.
func (cfg *config) one(peer int, cmd interface{}, expectedServers int, retry bool) LogIndex {
	t0 := time.Now()
	index := LogIndex{-1, -1}

	for time.Since(t0).Seconds() < 10 && cfg.checkFinished() == false {
		cfg.mu.Lock()
		if !cfg.connected[peer] {
			cfg.mu.Unlock()
			return index
		}

		e := cfg.peers[peer]
		cfg.mu.Unlock()

		if e == nil {
			return index
		}

		index = e.Start(cmd)

		t1 := time.Now()
		for time.Since(t1).Seconds() < 2 {
			nd, cmd1 := cfg.nExecuted(index)
			if nd > 0 && nd >= expectedServers {
				// committed
				if cmd1 == cmd {
					// and it was the command we submitted.
					return index
				}
			}
			time.Sleep(20 * time.Millisecond)
		}
		if retry == false {
			cfg.t.Fatalf("one(%v) failed to reach agreement", cmd)
		}
	}
	if cfg.checkFinished() == false {
		cfg.t.Fatalf("one(%v) failed to reach agreement", cmd)
	}
	return index
}

// start a Test.
// print the Test message.
// e.g. cfg.begin("Test (3B): RPC counts aren't too high")
func (cfg *config) begin(description string) {
	fmt.Printf("%s ...\n", description)
	cfg.t0 = time.Now()
	cfg.rpcs0 = cfg.rpcTotal()
	cfg.bytes0 = cfg.bytesTotal()
	cfg.cmds0 = 0
	// cfg.maxIndex0 = cfg.maxIndex
	copy(cfg.maxIndex0, cfg.maxIndex)
}

// end a Test -- the fact that we got here means there
// was no failure.
// print the Passed message,
// and some performance numbers.
func (cfg *config) end() {
	cfg.checkTimeout()
	if cfg.t.Failed() == false {
		cfg.mu.Lock()
		t := time.Since(cfg.t0).Seconds()       // real time
		npeers := cfg.n                         // number of EPaxos peers
		nrpc := cfg.rpcTotal() - cfg.rpcs0      // number of RPC sends
		nbytes := cfg.bytesTotal() - cfg.bytes0 // number of bytes
		ncmds := 0

		for i := 0; i < cfg.n; i++ {
			ncmds += cfg.maxIndex[i] - cfg.maxIndex0[i] // number of EPaxos agreements reported
		}

		cfg.mu.Unlock()

		fmt.Printf("  ... Passed --")
		fmt.Printf("  %4.1f  %d %4d %7d %4d\n", t, npeers, nrpc, nbytes, ncmds)
	}
}

// Maximum log size across all servers
func (cfg *config) LogSize() int {
	logsize := 0
	for i := 0; i < cfg.n; i++ {
		n := cfg.saved[i].ReadSize()
		if n > logsize {
			logsize = n
		}
	}
	return logsize
}
