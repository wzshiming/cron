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

type Cron struct {
	mut           sync.Mutex
	linked        *llrb.Tree[int64, *linkedJobs]
	doStart       sync.Once
	nextTimestamp int64
	now           func() time.Time
	newly         chan struct{}
	indexJob      map[*linkedJob]int64
}

func NewCron() *Cron {
	return &Cron{
		linked: llrb.NewTree[int64, *linkedJobs](),
		now: func() time.Time {
			return time.Now().UTC()
		},
		newly:    make(chan struct{}),
		indexJob: map[*linkedJob]int64{},
	}
}

func (t *Cron) AddWithCancel(nextFunc NextFunc, doFunc DoFunc) (cancelFunc DoFunc, ok bool) {
	j, ok := t.add(nextFunc, doFunc)
	if !ok {
		return nil, ok
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

	now := t.now()
	if !t.addLinkedJob(j, now) {
		return nil, false
	}
	return j, true
}

func (t *Cron) addLinkedJob(j *linkedJob, now time.Time) bool {
	t.doStart.Do(func() {
		go t.run()
	})

	t.mut.Lock()
	defer t.mut.Unlock()

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
		next, jobs, ok := t.getNextJobs()
		if !ok {
			t.wait(time.Second)
			continue
		}
		if len(jobs.Jobs) == 0 {
			t.deleteJobs(next)
			continue
		}
		now := t.now()
		sub := time.Unix(0, next).Sub(now)
		if sub > 0 {
			// Avoid inaccurate execution time caused by long sleep
			if sub > time.Minute {
				sub = time.Minute
			}
			t.wait(sub)
			continue
		}

		ok = t.deleteJobs(next)
		if !ok {
			// It won't get here.
			continue
		}

		// Do present job
		for _, j := range jobs.Jobs {
			j.Do()
		}

		now = t.now()
		// Schedule next job
		for _, j := range jobs.Jobs {
			t.addLinkedJob(j, now)
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

func (t *Cron) len() int {
	t.mut.Lock()
	defer t.mut.Unlock()
	return t.linked.Len()
}

func (t *Cron) Wait() {
	for t.len() != 0 {
		time.Sleep(time.Second)
	}
}
