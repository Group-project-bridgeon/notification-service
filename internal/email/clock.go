package email

import (
	"sync"
	"time"
)

// Clock abstracts time operations for deterministic unit testing.
type Clock interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
	Sleep(d time.Duration)
}

// RealClock provides actual wall-clock time operations.
type RealClock struct{}

// NewRealClock returns a RealClock instance.
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

// FakeClock enables manual time advancement in tests.
type FakeClock struct {
	mu      sync.Mutex
	current time.Time
	timers  []*fakeTimer
}

type fakeTimer struct {
	triggerAt time.Time
	ch        chan time.Time
}

// NewFakeClock creates a FakeClock initialized to the given time.
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

// After returns a channel triggered when the clock advances past now + d.
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

// Sleep advances fake time by d.
func (f *FakeClock) Sleep(d time.Duration) {
	f.Advance(d)
}

// Advance moves fake time forward, firing due timers.
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
