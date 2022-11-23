package cron

import (
	"reflect"
	"testing"
	"time"
)

func TestCron(t *testing.T) {
	cron := NewCron()
	tests := []struct {
		name     string
		schedule [][]int
		want     []int
	}{
		{
			schedule: [][]int{
				{1, 2, 3},
			},
			want: []int{1, 2, 3},
		},
		{
			schedule: [][]int{
				{1},
				{2},
				{3},
			},
			want: []int{1, 2, 3},
		},
		{
			schedule: [][]int{
				{3},
				{1},
			},
			want: []int{1, 3},
		},
		{
			schedule: [][]int{
				{1},
				{1},
			},
			want: []int{1, 1},
		},
		{
			schedule: [][]int{
				{2, 3, 4},
				{1, 2, 3},
			},
			want: []int{1, 2, 2, 3, 3, 4},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := []int{}
			now := time.Now().UTC().Truncate(time.Second)
			do := func() {
				got = append(got, int(time.Now().UTC().Truncate(time.Second).Sub(now)/time.Second))
			}

			for _, schedule := range tt.schedule {
				times := []time.Time{}
				for _, s := range schedule {
					times = append(times, now.Add(time.Duration(s)*time.Second))
				}
				cron.Add(Order(times...), do)
				time.Sleep(time.Millisecond)
			}
			cron.waitPendingToBeEmpty()
			if !reflect.DeepEqual(tt.want, got) {
				t.Fatalf("the execution time is not as expected: want %v, got %v", tt.want, got)
			}
			time.Sleep(2 * time.Second)
		})
	}
}

func TestCronCancel(t *testing.T) {
	cron := NewCron()
	now := time.Now().UTC().Truncate(time.Second)

	do := func() {
		t.Errorf("should not be executed to")
	}
	cancels := []DoFunc{}
	for _, d := range []int{2, 2, 3, 4, 2} {
		cancel, _ := cron.AddWithCancel(Order(now.Add(time.Duration(d)*time.Second)), do)
		cancels = append(cancels, cancel)
		time.Sleep(time.Millisecond)
	}

	for _, cancel := range cancels {
		cancel()
	}
	cron.waitPendingToBeEmpty()
}
