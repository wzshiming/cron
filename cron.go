package cron

import (
	"sync"
	"time"

	"github.com/wzshiming/llrb"
)

type (
	NextFunc func(now time.Time) (time.Time, bool)
	DoFunc   func()
)

type linkedJob struct {
	Next NextFunc
	Do   DoFunc
}

type linkedJobs struct {
	Jobs []*linkedJob
}

// Cron is a cron job manager.
type Cron struct {
	mut           sync.Mutex
	linked        *llrb.Tree[int64, *linkedJobs]
	nextTimestamp int64
	newly         chan struct{}
	indexJob      map[*linkedJob]int64
	state         int // 0: stop, 1: running
}

var (
	intervalDuration = time.Minute
)

func nowUTC() time.Time {
	return time.Now().UTC()
}

// NewCron returns a new Cron.
func NewCron() *Cron {
	return &Cron{}
}

func (t *Cron) init() {
	if t.state != 0 {
		return
	}
	t.state = 1
	t.indexJob = map[*linkedJob]int64{}
	t.newly = make(chan struct{})
	t.linked = llrb.NewTree[int64, *linkedJobs]()
	go t.run()
}

// AddWithCancel adds a job to the cron and returns a function to cancel the job.
func (t *Cron) AddWithCancel(nextFunc NextFunc, doFunc DoFunc) (cancelFunc DoFunc, ok bool) {
	j, ok := t.add(nextFunc, doFunc)
	if !ok {
		return nil, false
	}
	once := sync.Once{}
	return func() {
		once.Do(func() {
			t.cancel(j)
		})
	}, true
}

func (t *Cron) cancel(j *linkedJob) {
	t.mut.Lock()
	defer t.mut.Unlock()
	timestamp := t.indexJob[j]
	delete(t.indexJob, j)
	jobs, ok := t.linked.Get(timestamp)
	if !ok {
		return
	}
	for i, job := range jobs.Jobs {
		if job == j {
			jobs.Jobs = append(jobs.Jobs[:i], jobs.Jobs[i+1:]...)
			break
		}
	}

	if len(jobs.Jobs) == 0 {
		t.linked.Delete(timestamp)
	}

	if t.nextTimestamp == timestamp {
		t.update(time.Second)
	}
	return
}

// Add adds a job to the cron.
func (t *Cron) Add(nextFunc NextFunc, doFunc DoFunc) bool {
	_, ok := t.add(nextFunc, doFunc)
	return ok
}

func (t *Cron) add(nextFunc NextFunc, doFunc DoFunc) (*linkedJob, bool) {
	if nextFunc == nil || doFunc == nil {
		return nil, false
	}
	j := &linkedJob{
		Next: nextFunc,
		Do:   doFunc,
	}

	now := nowUTC()
	if !t.addLinkedJob(j, now) {
		return nil, false
	}
	return j, true
}

func (t *Cron) addLinkedJob(j *linkedJob, now time.Time) bool {
	t.mut.Lock()
	defer t.mut.Unlock()
	t.init()

	next, ok := j.Next(now)
	if !ok {
		delete(t.indexJob, j)
		return false
	}

	timestamp := next.UnixNano()

	jobs, ok := t.linked.Get(timestamp)
	if ok {
		jobs.Jobs = append(jobs.Jobs, j)
	} else {
		t.linked.Put(timestamp, &linkedJobs{
			Jobs: []*linkedJob{j},
		})
	}

	t.indexJob[j] = timestamp

	if t.nextTimestamp == 0 || timestamp < t.nextTimestamp {
		t.update(time.Second)
	}

	return true
}

func (t *Cron) update(duration time.Duration) {
	select {
	case t.newly <- struct{}{}:
	case <-time.After(duration):
	}
}

func (t *Cron) wait(duration time.Duration) {
	select {
	case <-time.After(duration):
	case <-t.newly:
	}
}

func (t *Cron) run() {
	for {
		// Get the next job.
		next, jobs, ok := t.getNextJobs()
		if !ok {
			// Wait for a new job to be added.
			t.wait(intervalDuration)

			// TODO: Exit the goroutine and reset cron if there is no job for a long time
			continue
		}
		if len(jobs.Jobs) == 0 {
			// Delete the empty job. This should not happen.
			t.deleteJobs(next)
			continue
		}
		now := nowUTC()
		sub := time.Unix(0, next).Sub(now)
		if sub > 0 {
			// Avoid inaccurate execution time caused by long sleep.
			if sub > intervalDuration {
				sub = intervalDuration
			}
			// Wait for the next job to be executed.
			t.wait(sub)
			continue
		}

		// Will be executed immediately, delete the job first.
		ok = t.deleteJobs(next)
		if !ok {
			// This should not happen.
			continue
		}

		// Execute the jobs of the time.
		for _, j := range jobs.Jobs {
			j.Do()
		}

		now = nowUTC()
		// Schedule next execution time.
		for _, j := range jobs.Jobs {
			_ = t.addLinkedJob(j, now)
		}
	}
}

func (t *Cron) getNextJobs() (int64, *linkedJobs, bool) {
	t.mut.Lock()
	defer t.mut.Unlock()
	nextTimestamp, jobs, ok := t.linked.Min()
	if !ok {
		t.nextTimestamp = 0
		return 0, nil, false
	}
	t.nextTimestamp = nextTimestamp
	return nextTimestamp, jobs, true
}

func (t *Cron) deleteJobs(timestamp int64) bool {
	t.mut.Lock()
	defer t.mut.Unlock()
	_, ok := t.linked.Delete(timestamp)
	return ok
}

func (t *Cron) pending() int {
	t.mut.Lock()
	defer t.mut.Unlock()
	return t.linked.Len()
}

func (t *Cron) waitPendingToBeEmpty() {
	for t.pending() != 0 {
		time.Sleep(time.Second)
	}
}
