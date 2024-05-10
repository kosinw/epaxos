package benchmark

import (
	"fmt"
	"math/rand"
	"os"
	"path"
	"strconv"
	"testing"
	"time"
)

// to make sure timestamps use the monotonic clock, instead of computing
// absolute timestamps with `time.Now().UnixNano()` (which uses the wall
// clock), we measure time relative to `t0` using `time.Since(t0)`, which uses
// the monotonic clock
var t0 = time.Now()
var dirname string

func setup() {
	// generate directory name through a timestamp
	dirname = "bench-" + time.Now().Format("2006-01-02_15-04-05")

	err := os.Mkdir(dirname, 0755)

	if err != nil {
		fmt.Println("Error creating directory:", err)
		return
	}
}

func log(file *os.File, datapoint int) {
	_, err := file.WriteString(fmt.Sprintf("%v\n", datapoint))
	if err != nil {
		fmt.Println("Error writing to file:", err)
	}
}

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

func ThroughputBenchmark(t *testing.T, part string, fname string, contention int, nservers int, nclients int, wide_area bool, make_config makeConfigFn) {
	cfg := make_config(t, nservers, false)
	o0 := cfg.getOps()
	pathname := path.Join(dirname, fname)
	f, _ := os.OpenFile(pathname, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)

	if wide_area {
		cfg.configureWideArea()
	}

	defer cfg.cleanup()

	cfg.begin(fmt.Sprintf("Bench: %s throughput", part))

	// Spawn goroutine that samples throughput every second
	killed1 := make(chan struct{})
	killed2 := make(chan struct{})

	keys := make([]string, 100)

	for i := range keys {
		keys[i] = fmt.Sprintf("%v", i)
	}

	for i := 0; i < contention; i++ {
		keys[i] = "same"
	}

	go spawn_clients_and_wait(t, cfg, nclients, func(me int, ck Clerk, t *testing.T) {
		i := 0
		for {
			select {
			case <-killed1:
				return
			default:
			}

			key := keys[rand.Intn(len(keys))]
			Append(cfg, ck, key, "x "+strconv.Itoa(me)+" "+strconv.Itoa(i)+" y", me)
			i++
		}
	})

	go func() {
		for {
			select {
			case <-killed2:
				return
			default:
				time.Sleep(1 * time.Second)
				o1 := cfg.getOps()
				tput := (o1 - o0)
				o0 = o1
				log(f, int(tput))
			}
		}
	}()

	// Wait for 1 minute
	time.Sleep(1 * time.Minute)
	killed1 <- struct{}{}
	killed2 <- struct{}{}
	cfg.end()
}

func TestRaftBasicThroughputBenchmark(t *testing.T) {
	ThroughputBenchmark(t, "raft, 5 clients", "raft_tput", 100, 5, 5, false, make_raft_config)
}

func TestEPaxos0BasicThroughputBenchmark(t *testing.T) {
	ThroughputBenchmark(t, "epaxos (0%), 5 clients", "epaxos_0_tput", 0, 5, 5, false, make_epaxos_config)
}

func TestEPaxos20BasicThroughputBenchmark(t *testing.T) {
	ThroughputBenchmark(t, "epaxos (20%), 5 clients", "epaxos_20_tput", 20, 5, 5, false, make_epaxos_config)
}

func TestEPaxosBasic50ThroughputBenchmark(t *testing.T) {
	ThroughputBenchmark(t, "epaxos (50%), 5 clients", "epaxos_50_tput", 50, 5, 5, false, make_epaxos_config)
}

func TestEPaxosBasic100ThroughputBenchmark(t *testing.T) {
	ThroughputBenchmark(t, "epaxos (100%), 5 clients", "epaxos_100_tput", 100, 5, 5, false, make_epaxos_config)
}

func TestRaftWideAreaThroughputBenchmark(t *testing.T) {
	ThroughputBenchmark(t, "raft, 5 clients, wide area", "raft_wa_tput", 100, 5, 5, true, make_raft_config)
}

func TestEPaxos0WideAreaThroughputBenchmark(t *testing.T) {
	ThroughputBenchmark(t, "epaxos (0%), 5 clients, wide area", "epaxos_wa_0_tput", 0, 5, 5, true, make_epaxos_config)
}

func TestEPaxos20WideAreaThroughputBenchmark(t *testing.T) {
	ThroughputBenchmark(t, "epaxos (20%), 5 clients, wide area", "epaxos_wa_20_tput", 20, 5, 5, true, make_epaxos_config)
}

func TestEPaxosWideArea50ThroughputBenchmark(t *testing.T) {
	ThroughputBenchmark(t, "epaxos (50%), 5 clients, wide area", "epaxos_wa_50_tput", 50, 5, 5, true, make_epaxos_config)
}

func TestEPaxosWideArea100ThroughputBenchmark(t *testing.T) {
	ThroughputBenchmark(t, "epaxos (100%), 5 clients, wide area", "epaxos_wa_100_tput", 100, 5, 5, true, make_epaxos_config)
}
