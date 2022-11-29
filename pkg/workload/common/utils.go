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

// Inc return true if a cycle is completed
func (c *CyclicCounter) Inc(by uint64) bool {
	if c.Size == 0 {
		return true
	}
	c.Value += by
	cycleCompleted := c.Value >= c.Size
	c.Value %= c.Size
	return cycleCompleted
}

func (c *CyclicCounter) IsNextCompleteCycle(by uint64) bool {
	if c.Size == 0 {
		return true
	}
	return c.Value+by >= c.Size
}
