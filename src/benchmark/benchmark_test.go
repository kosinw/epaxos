package benchmark

import (
	"fmt"
	"os"
	"testing"
	"time"
)

// to make sure timestamps use the monotonic clock, instead of computing
// absolute timestamps with `time.Now().UnixNano()` (which uses the wall
// clock), we measure time relative to `t0` using `time.Since(t0)`, which uses
// the monotonic clock
var t0 = time.Now()

func setup() {

}

// func logOperation(operation string, start int64, end int64) {
// 	file, _ := os.OpenFile("latency.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
// 	latency := end - start
// 	_, err := file.WriteString(fmt.Sprintf("%s: start: %d, end: %d, latency: %d\n", operation, start, end, latency))
// 	if err != nil {
// 		fmt.Println("Error writing to file:", err)
// 	}
// }

// get/put/putappend that keep counts
func Get(cfg Config, ck Clerk, key string, cli int) string {
	v := ck.Get(key)
	cfg.op()

	return v
}

func Put(cfg Config, ck Clerk, key string, value string, cli int) {
	ck.Put(key, value)
	cfg.op()
}

func Append(cfg Config, ck Clerk, key string, value string, cli int) {
	ck.Append(key, value)
	cfg.op()
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

func TestMain(m *testing.M) {
	setup()
	code := m.Run()
	os.Exit(code)
}

type makeConfigFn func(t *testing.T, n int, unreliable bool) Config

func BasicThroughputBenchmark(t *testing.T, part string, fname string, contention float64, nservers int, nclients int, make_config makeConfigFn) {
	cfg := make_config(t, nservers, false)
	defer cfg.cleanup()

	// ck := cfg.makeClient(cfg.All())

	cfg.begin(fmt.Sprintf("Bench: %s basic latency benchmark", part))

	// Spawn goroutine that samples throughput every tenth of a second
	go func() {
		time.Sleep(100 * time.Millisecond)
	}()

	// Wait for 30 seconds
	time.Sleep(30 * time.Second)

	cfg.end()
}

func TestRaftBasicThroughputBenchmark(t *testing.T) {
	BasicThroughputBenchmark(t, "raft, 5 clients", "raft_tput", 0.0, 5, 5, make_raft_config)
}

func TestEPaxos0BasicThroughputBenchmark(t *testing.T) {
	BasicThroughputBenchmark(t, "epaxos (0%), 5 clients", "epaxos_0_tput", 0.0, 5, 5, make_epaxos_config)
}

func TestEPaxos20BasicThroughputBenchmark(t *testing.T) {
	BasicThroughputBenchmark(t, "epaxos (20%), 5 clients", "epaxos_20_tput", 0.2, 5, 5, make_epaxos_config)
}

func TestEPaxosBasic50ThroughputBenchmark(t *testing.T) {
	BasicThroughputBenchmark(t, "epaxos (50%), 5 clients", "epaxos_50_tput", 0.5, 5, 5, make_epaxos_config)
}

func TestEPaxosBasic100ThroughputBenchmark(t *testing.T) {
	BasicThroughputBenchmark(t, "epaxos (100%), 5 clients", "epaxos_100_tput", 1, 5, 5, make_epaxos_config)
}
