package shardkv

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
	topicPut     topic = "PUT"
	topicAppend  topic = "APPEND"
	topicGet     topic = "GET"
	topicConfig  topic = "CONFIG"
	topicClient  topic = "CLIENT"
	topicSnap    topic = "SNAP"
	topicInstall topic = "INSTALL"
	topicClerk   topic = "CLERK"
	topicService topic = "SERVICE"
	topicError   topic = "ERROR"
	topicLock    topic = "LOCK"
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

		return level == 4
	}

	return false
}

func debugTimestamp() int64 {
	return time.Since(debugStart).Milliseconds()
}

func init() {
	if checkDebugMode() {
		atomic.StoreInt32(&debugEnabled, 1)
	}

	debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.LstdFlags))
	log.SetOutput(os.Stdout)
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
