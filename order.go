package cron

import (
	"sort"
	"time"
)

func Order(ts ...time.Time) NextFunc {
	index := 0
	sort.Slice(ts, func(i, j int) bool {
		return ts[i].Before(ts[j])
	})
	return func(_ time.Time) (time.Time, bool) {
		if len(ts) <= index {
			return time.Time{}, false
		}
		next := ts[index]
		index++
		return next, true
	}
}
