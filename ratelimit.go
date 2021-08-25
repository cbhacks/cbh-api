package main

import (
	"time"
	"sync"
)

type RateBucket struct {
	mutex sync.Mutex
	capacity int
	amount int
	duration time.Duration
	nextTime time.Time
}

func MakeRateBucket(reqsPerMinute, burstCapacity int) *RateBucket {
	d := time.Duration(60_000_000_000 / reqsPerMinute)
	return &RateBucket {
		capacity: burstCapacity,
		amount: burstCapacity / 2,
		duration: d,
		nextTime: time.Now().Add(d),
	}
}

func (rb *RateBucket) TryTake() bool {
	rb.mutex.Lock()
	defer rb.mutex.Unlock()

	now := time.Now()

	// Pump tokens into the bucket.
	for rb.amount < rb.capacity && now.After(rb.nextTime) {
		// Enough time has passed for another token.
		rb.amount++
		rb.nextTime = rb.nextTime.Add(rb.duration)
	}

	if rb.amount == 0 {
		// The bucket is empty; rate limit exceeded.
		return false
	} else if rb.amount == rb.capacity {
		// The bucket is full. Take one token and restart the timer.
		rb.amount--
		rb.nextTime = now.Add(rb.duration)
		return true
	} else {
		// The bucket is somewhere in-between. Take one token.
		rb.amount--
		return true
	}
}
