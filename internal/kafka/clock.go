package kafka

import (
	"sync"
	"time"
)

// Clock abstracts time measurement for deterministic testing of delays and timeouts.
type Clock interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
	Sleep(d time.Duration)
}

// RealClock implements Clock using the standard library time package.
type RealClock struct{}

func NewRealClock() *RealClock {
	return &RealClock{}
}

func (r *RealClock) Now() time.Time {
	return time.Now().UTC()
}

func (r *RealClock) After(d time.Duration) <-chan time.Time {
	return time.After(d)
}

func (r *RealClock) Sleep(d time.Duration) {
	time.Sleep(d)
}

// FakeClock implements Clock with deterministic manual time advancement for unit tests.
type FakeClock struct {
	mu      sync.Mutex
	current time.Time
	timers  []*fakeTimer
}

type fakeTimer struct {
	triggerAt time.Time
	ch        chan time.Time
}

// NewFakeClock constructs a FakeClock initialized to the given time.
func NewFakeClock(initial time.Time) *FakeClock {
	return &FakeClock{
		current: initial.UTC(),
		timers:  make([]*fakeTimer, 0),
	}
}

// Now returns the current fake time.
func (f *FakeClock) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.current
}

// After returns a channel that receives the fake time when Advance reaches now + d.
func (f *FakeClock) After(d time.Duration) <-chan time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()

	ch := make(chan time.Time, 1)
	trigger := f.current.Add(d)

	if !trigger.After(f.current) {
		ch <- f.current
		return ch
	}

	f.timers = append(f.timers, &fakeTimer{
		triggerAt: trigger,
		ch:        ch,
	})
	return ch
}

// Sleep simulates sleeping by advancing fake time.
func (f *FakeClock) Sleep(d time.Duration) {
	f.Advance(d)
}

// Advance advances the fake clock by duration d, triggering any pending timers.
func (f *FakeClock) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.current = f.current.Add(d)

	active := make([]*fakeTimer, 0, len(f.timers))
	for _, timer := range f.timers {
		if !timer.triggerAt.After(f.current) {
			select {
			case timer.ch <- timer.triggerAt:
			default:
			}
		} else {
			active = append(active, timer)
		}
	}
	f.timers = active
}

// TimersCount returns the number of active un-triggered timers.
func (f *FakeClock) TimersCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.timers)
}
