package raft

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"
)

// Debugging
var Debug = "1"

func init() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	if debug := os.Getenv("debug"); len(debug) > 0 {
		Debug = debug
	}
}

func DPrintf(ctx context.Context, format string, a ...interface{}) (n int, err error) {
	if Debug != "0" {
		log.Printf("node: %d, term %d, leader: %d, index: %d, log: %s", ctx.Value("node"), ctx.Value("term"), ctx.Value("leader"), ctx.Value("index"), fmt.Sprintf(format, a...))
	}
	return
}

// waitTimeout waits for the waitgroup for the specified max timeout.
// Returns true if waiting timed out.
func waitTimeout(wg *sync.WaitGroup, timeout time.Duration) bool {
	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()
	select {
	case <-c:
		return false // completed normally
	case <-time.After(timeout):
		return true // timed out
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
