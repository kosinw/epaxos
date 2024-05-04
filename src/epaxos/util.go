package epaxos

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"sync/atomic"
	"time"
)

type topic string

const (
	topicClient    topic = "CLIENT"
	topicCommit    topic = "COMMIT"
	topicExecute   topic = "EXECUTE"
	topicError     topic = "ERROR"
	topicInfo      topic = "INFO"
	topicLog       topic = "LOG"
	topicPersist   topic = "PERSIST"
	topicTimer     topic = "TIMER"
	topicWarn      topic = "WARN"
	topicLock      topic = "LOCK"
	topicStart     topic = "START"
	topicRpc       topic = "RPC"
	topicPreAccept topic = "PACCEPT"
	topicAccept    topic = "ACCEPT"
)

var debugStart time.Time
var debugEnabled int32

func checkDebugMode() bool {
	v := os.Getenv("VERBOSE")

	if v != "" {
		level, err := strconv.Atoi(v)

		if err != nil {
			panic("invalid verbosity")
		}

		return level == 1 // verbosity level 1 is for debugging epaxos
	}

	return false
}

func debugTimestamp() int64 {
	return time.Since(debugStart).Milliseconds()
}

func init() {
	// debugEnabled.Store(checkDebugMode())
	atomic.StoreInt32(&debugEnabled, 1)
	debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.LstdFlags))
	log.SetOutput(os.Stdout)

	enableLogging()
}

func disableLogging() {
	atomic.StoreInt32(&debugEnabled, 0)
}

func enableLogging() {
	if checkDebugMode() {
		atomic.StoreInt32(&debugEnabled, 1)
	} else {
		atomic.StoreInt32(&debugEnabled, 0)
	}
}

func debug(topic topic, me int, format string, a ...interface{}) {
	if atomic.LoadInt32(&debugEnabled) == 1 {
		timestamp := debugTimestamp()
		prefix := fmt.Sprintf("%06d % -7s S%d ", timestamp, string(topic), me)
		log.Printf(prefix+format, a...)
	}
}

func assert(cond bool, me int, format string, a ...interface{}) {
	if !cond {
		timestamp := debugTimestamp()
		prefix := fmt.Sprintf("%06d % -7s S%d ", timestamp, string(topicError), me)
		panic(fmt.Sprintf(prefix+format, a...))
	}
}
