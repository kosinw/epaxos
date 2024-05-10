package benchmark

import (
	"fmt"
	"strconv"
	"testing"
	"time"
)

// to make sure timestamps use the monotonic clock, instead of computing
// absolute timestamps with `time.Now().UnixNano()` (which uses the wall
// clock), we measure time relative to `t0` using `time.Since(t0)`, which uses
// the monotonic clock
var t0 = time.Now()

// get/put/putappend that keep counts
func Get(cfg Config, ck Clerk, key string, cli int) string {
	// start := int64(time.Since(t0))
	v := ck.Get(key)
	// end := int64(time.Since(t0))
	cfg.op()

	return v
}

func Put(cfg Config, ck Clerk, key string, value string, cli int) {
	// start := int64(time.Since(t0))
	ck.Put(key, value)
	// end := int64(time.Since(t0))
	cfg.op()
}

func Append(cfg Config, ck Clerk, key string, value string, cli int) {
	// start := int64(time.Since(t0))
	ck.Append(key, value)
	// end := int64(time.Since(t0))
	cfg.op()
}

func check(cfg Config, t *testing.T, ck Clerk, key string, value string) {
	v := Get(cfg, ck, key, -1)
	if v != value {
		t.Fatalf("Get(%v): expected:\n%v\nreceived:\n%v", key, value, v)
	}
}

// a client runs the function f and then signals it is done
func run_client(t *testing.T, cfg Config, me int, ca chan bool, fn func(me int, ck Clerk, t *testing.T)) {
	ok := false
	defer func() { ca <- ok }()
	ck := cfg.makeClient(cfg.All())
	fn(me, ck, t)
	ok = true
	cfg.deleteClient(ck)
}

// spawn ncli clients and wait until they are all done
func spawn_clients_and_wait(t *testing.T, cfg Config, ncli int, fn func(me int, ck Clerk, t *testing.T)) {
	ca := make([]chan bool, ncli)
	for cli := 0; cli < ncli; cli++ {
		ca[cli] = make(chan bool)
		go run_client(t, cfg, cli, ca[cli], fn)
	}
	// log.Printf("spawn_clients_and_wait: waiting for clients")
	for cli := 0; cli < ncli; cli++ {
		ok := <-ca[cli]
		// log.Printf("spawn_clients_and_wait: client %d is done\n", cli)
		if ok == false {
			t.Fatalf("failure")
		}
	}
}

type makeConfigFn func(t *testing.T, n int, unreliable bool) Config

func BasicLatencyBenchmark(t *testing.T, part string, make_config makeConfigFn) {
	const nservers = 3
	const numOps = 1000
	cfg := make_config(t, nservers, false)
	defer cfg.cleanup()

	ck := cfg.makeClient(cfg.All())

	cfg.begin(fmt.Sprintf("Bench: %s basic latency benchmark", part))

	// // wait until first op completes, so we know a leader is elected
	// // and KV servers are ready to process client requests
	ck.Get("x")

	start := time.Now()
	for i := 0; i < numOps; i++ {
		ck.Append("x", "x 0 "+strconv.Itoa(i)+" y")
	}
	dur := time.Since(start)

	// v := ck.Get("x")
	ck.Get("x")
	// checkClntAppends(t, 0, v, numOps)

	// heartbeat interval should be ~ 100 ms; require at least 3 ops per
	const heartbeatInterval = 100 * time.Millisecond
	const opsPerInterval = 3
	const timePerOp = heartbeatInterval / opsPerInterval
	if dur > numOps*timePerOp {
		t.Fatalf("Operations completed too slowly %v/op > %v/op\n", dur/numOps, timePerOp)
	}

	cfg.end()
}

func TestRaftBasicLatencyBenchmark(t *testing.T) {
	BasicLatencyBenchmark(t, "raft", make_raft_config)
}

func TestEPaxosBasicLatencyBenchmark(t *testing.T) {
	BasicLatencyBenchmark(t, "epaxos (100%)", make_epaxos_config)
}
