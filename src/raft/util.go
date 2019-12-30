package raft

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"
)

// Debugging
const Debug = 1

func init() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
}

func DPrintf(ctx context.Context, format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf("node: %d, term %d, leader: %d log: %s", ctx.Value("node"), ctx.Value("term"), ctx.Value("leader"), fmt.Sprintf(format, a...))
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
