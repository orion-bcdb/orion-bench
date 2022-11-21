// Author: Liran Funaro <liran.funaro@ibm.com>

package common

import (
	"sync"
	"time"
)

func WaitTimeout(wg *sync.WaitGroup, timeout time.Duration) bool {
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

func Max(x, y uint64) uint64 {
	if x < y {
		return y
	}
	return x
}

type CyclicCounter struct {
	Value uint64
	Size  uint64
}

func (c *CyclicCounter) Inc(by uint64) {
	if c.Size == 0 {
		return
	}
	c.Value = (c.Value + by) % c.Size
}
