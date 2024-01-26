package kvraft

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
	topicClerk   topic = "CLERK"
	topicService topic = "SERVICE"
	topicError   topic = "ERROR"
	topicWarn    topic = "WARN"
	topicInfo    topic = "INFO"
	topicSnap    topic = "SNAP"
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

		return level == 2 // verbosity level 2 is for debugging kvraft
	}

	return false
}

func debugTimestamp() int64 {
	return time.Since(debugStart).Milliseconds()
}

func init() {
	atomic.StoreInt32(&debugEnabled, 1)
	debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.LstdFlags))
	log.SetOutput(os.Stdout)
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
