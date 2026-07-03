package agent

import (
	"context"
	"sync"
	"time"
)

type activeRequestCounter struct {
	mu    sync.Mutex
	cond  *sync.Cond
	count int
}

func newActiveRequestCounter() *activeRequestCounter {
	c := &activeRequestCounter{}
	c.cond = sync.NewCond(&c.mu)
	return c
}

func (c *activeRequestCounter) inc() {
	c.mu.Lock()
	c.count++
	c.mu.Unlock()
}

func (c *activeRequestCounter) activeRequestsInc() {
	c.inc()
}

func (c *activeRequestCounter) dec() {
	c.mu.Lock()
	c.count--
	if c.count == 0 {
		c.cond.Broadcast()
	}
	c.mu.Unlock()
}

func (c *activeRequestCounter) activeRequestsDec() {
	c.dec()
}

func (c *activeRequestCounter) wait(ctx context.Context, timeout time.Duration) bool {
	c.mu.Lock()
	if c.count == 0 {
		c.mu.Unlock()
		return true
	}

	var timedOut bool
	if timeout > 0 {
		time.AfterFunc(timeout, func() {
			c.mu.Lock()
			timedOut = true
			c.cond.Broadcast()
			c.mu.Unlock()
		})
	}
	go func() {
		<-ctx.Done()
		c.mu.Lock()
		c.cond.Broadcast()
		c.mu.Unlock()
	}()

	for c.count > 0 && !timedOut && ctx.Err() == nil {
		c.cond.Wait()
	}
	result := c.count == 0
	c.mu.Unlock()
	return result
}
