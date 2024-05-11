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

var t1 = time.Now()

func BasicLatencyBenchmark(t *testing.T, part string, fname string, contention int, nservers int, nclients int, wide_area bool, make_config makeConfigFn) {
	cfg := make_config(t, nservers, false)
	pathname := path.Join(dirname, fname)
	f, _ := os.OpenFile(pathname, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)

	if wide_area {
		cfg.configureWideArea()
	}

	defer cfg.cleanup()

	cfg.begin(fmt.Sprintf("Bench: %s basic latency benchmark", part))

	killed1 := make(chan struct{})
	// killed2 := make(chan struct{})

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
			t2 := time.Since(t1)
			Append(cfg, ck, key, "x "+strconv.Itoa(me)+" "+strconv.Itoa(i)+" y", me)
			t3 := time.Since(t1)
			log(f, int(t3 - t2))
			i++
		}
	})

	time.Sleep(1 * time.Minute)
	killed1 <- struct{}{}
	cfg.end()
}

func TestRaftBasicLatencyBenchmark(t *testing.T) {
	BasicLatencyBenchmark(t, "raft, 5 clients", "raft_latency", 100, 5, 5, false, make_raft_config)
}

func TestEPaxos0BasicLatencyBenchmark(t *testing.T) {
	BasicLatencyBenchmark(t, "epaxos (0%), 5 clients", "epaxos_0_latency", 0, 5, 5, false, make_epaxos_config)
}

func TestEPaxos20BasicLatencyBenchmark(t *testing.T) {
	BasicLatencyBenchmark(t, "epaxos (20%), 5 clients", "epaxos_20_latency", 20, 5, 5, false, make_epaxos_config)
}

func TestEPaxos50BasicLatencyBenchmark(t *testing.T) {
	BasicLatencyBenchmark(t, "epaxos (50%), 5 clients", "epaxos_50_latency", 50, 5, 5, false, make_epaxos_config)
}

func TestEPaxos100BasicLatencyBenchmark(t *testing.T) {
	BasicLatencyBenchmark(t, "epaxos (100%), 5 clients", "epaxos_100_latency", 100, 5, 5, false, make_epaxos_config)
}

func TestRaftWideAreaLatencyBenchmark(t *testing.T) {
	BasicLatencyBenchmark(t, "raft, 5 clients", "raft_wa_latency", 100, 5, 5, true, make_raft_config)
}

func TestEPaxos0WideAreaLatencyBenchmark(t *testing.T) {
	BasicLatencyBenchmark(t, "epaxos (0%), 5 clients", "epaxos_0_wa_latency", 0, 5, 5, true, make_epaxos_config)
}

func TestEPaxos20WideAreaLatencyBenchmark(t *testing.T) {
	BasicLatencyBenchmark(t, "epaxos (20%), 5 clients", "epaxos_20_wa_latency", 20, 5, 5, true, make_epaxos_config)
}

func TestEPaxos50WideAreaLatencyBenchmark(t *testing.T) {
	BasicLatencyBenchmark(t, "epaxos (50%), 5 clients", "epaxos_50_wa_latency", 50, 5, 5, true, make_epaxos_config)
}

func TestEPaxos100WideAreaLatencyBenchmark(t *testing.T) {
	BasicLatencyBenchmark(t, "epaxos (100%), 5 clients", "epaxos_100_wa_latency", 100, 5, 5, true, make_epaxos_config)
}
