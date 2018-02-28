package crystal

import (
	"container/heap"
	"context"
	"sync"
	"time"
)

const (
	bufferSize = 512
	defInterval = 100
)

/* 'expiration' is the time when timer time out, if 'interval' > 0
the timer will time out periodically, 'timeout' contains the callback
to be called when times out */
type Timer struct {
	Id         	uint64
	Chan 		chan uint64
	expiration 	time.Time
	interval   	time.Duration
}

func (t *Timer) isRepeat() bool {
	return int64(t.interval) > 0
}

func (t *Timer) Close() {
	TimerWheelInstance().UnregTimer(t.Id)
	close(t.Chan)
}

// timerHeap is a heap-based priority queue
type timerHeap []*Timer
func (th timerHeap) Remove(id uint64) int {
	for i, t := range th {
		if t.Id == id {
			heap.Remove(&th, i)
		}
	}
	return -1
}

func (th timerHeap) Len() int {
	return len(th)
}

func (th timerHeap) Less(i, j int) bool {
	return th[i].expiration.UnixNano() < th[j].expiration.UnixNano()
}

func (th timerHeap) Swap(i, j int) {
	th[i], th[j] = th[j], th[i]
}

func (th *timerHeap) Push(x interface{}) {
	tr := x.(*Timer)
	*th = append(*th, tr)
}

func (th *timerHeap) Pop() interface{} {
	old := *th
	n := len(old)
	tr := old[n-1]
	*th = old[0 : n-1]
	return tr
}

// TimerWheel manages all the timed task.
type TimerWheel struct {
	timers      timerHeap
	ticker      *time.Ticker
	wg          *sync.WaitGroup
	regChan     chan *Timer		// reg timer in loop
	unregChan   chan uint64      	// unreg timer in loop
	ctx         context.Context
	cancel      context.CancelFunc
}

// NewTimerWheel returns a *TimerWheel ready for use.
func NewTimerWheel(ctx context.Context, interval time.Duration) *TimerWheel {
	if globalTimerWheel == nil {
		timerWheel := &TimerWheel{
			regChan	: make(chan *Timer, bufferSize),
			unregChan: make(chan uint64, bufferSize),
			ticker	: time.NewTicker(time.Millisecond * interval),
			timers	: make(timerHeap, 0),
			wg		: &sync.WaitGroup{},
		}
		timerWheel.ctx, timerWheel.cancel = context.WithCancel(ctx)
		heap.Init(&timerWheel.timers)
		timerWheel.wg.Add(1)
		go func() {
			timerWheel.start()
			timerWheel.wg.Done()
		}()
		globalTimerWheel = timerWheel
	}
	return globalTimerWheel
}

func NewTimer(elapsed time.Duration, interv time.Duration) *Timer {
	return &Timer{
		Id 			: NewGuid(2000, 1),
		expiration	: time.Now().Add(elapsed * time.Millisecond),
		interval	: interv * time.Millisecond,
		Chan		: make(chan uint64),
	}
}

// AddTimer adds new timed task.
func (tw *TimerWheel) AddTimer(t *Timer, elapsed time.Duration) bool {
	if t.Id > 0 && t.Chan != nil{
		t.expiration = time.Now().Add(elapsed * time.Millisecond)
		tw.regChan <- t
		return true
	}
	return false
}

// RegTimer adds new timed task.
func (tw *TimerWheel) RegTimer(elapsed time.Duration, interv time.Duration) *Timer {
	tr := NewTimer(elapsed, interv)
	tw.regChan <- tr
	return tr
}

// DeleteTimer cancels a timed task with specified timer ID.
func (tw *TimerWheel) DeleteTimer(t *Timer) {
	tw.unregChan <- t.Id
}

// UnregTimer cancels a timed task with specified timer ID.
func (tw *TimerWheel) UnregTimer(timerID uint64) {
	tw.unregChan <- timerID
}

// Stop stops the TimerWheel.
func (tw *TimerWheel) Stop() {
	tw.cancel()
	tw.wg.Wait()
}

func (tw *TimerWheel) getExpired() []*Timer {
	expired := make([]*Timer, 0)
	for tw.timers.Len() > 0 {
		tr := heap.Pop(&tw.timers).(*Timer)
		elapsed := time.Since(tr.expiration).Seconds()
		if elapsed > 1.0 {
			Warnf("timer %d elapsed %f second\n", tr.Id, elapsed)
		}
		if elapsed > 0.0 {
			expired = append(expired, tr)
			continue
		} else {
			heap.Push(&tw.timers, tr)
			break
		}
	}
	return expired
}

func (tw *TimerWheel) update(timers []*Timer) {
	if timers != nil {
		for _, t := range timers {
			if t.isRepeat() { // repeatable timer task
				t.expiration = t.expiration.Add(t.interval)
				// if task time out for at least 5 seconds, the expiration time needs
				// to be updated in case this task executes every time timer wakes up.
				if time.Since(t.expiration).Seconds() >= 5.0 {
					t.expiration = time.Now()
				}
				heap.Push(&tw.timers, t)
			}
		}
	}
}

func (tw *TimerWheel) start() {
	for {
		select {
		case timerID := <-tw.unregChan:
			tw.timers.Remove(timerID)
		case <-tw.ctx.Done():
			tw.timers = timerHeap{}
			tw.ticker.Stop()
			return
		case timer := <-tw.regChan:
			heap.Push(&tw.timers, timer)
		case <-tw.ticker.C:
			timers := tw.getExpired()
			for _, t := range timers {
				t.Chan <- t.Id
			}
			tw.update(timers)
		}
	}
}

var globalTimerWheel *TimerWheel
// TimerWheelInstance returns the global pool.
func TimerWheelInstance() *TimerWheel {
	return globalTimerWheel
}