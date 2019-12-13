// Copyright 2016 Google Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package runtime contains common utilities use to meter time for benchmarks.
package runtime

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"time"
)

// Use to allow injection of mock time Now during testing.
var timeNow = time.Now

// TrackDuration measure the duration of the run time function using the
// wall clock. You should not consider the returned duration in the presence
// or an error since it will likely have shortcut the execution of the
// function being executed.
func TrackDuration(f func() error) (time.Duration, error) {
	ts := timeNow()
	err := f()
	d := timeNow().Sub(ts)
	return d, err
}

// RepetitionDurationStats extracts some duration stats by repeatedly execution
// and measuring runtime. Returns the mean and deviation of the run duration of
// the provided function. If an error is return by the function it will shortcut
// the execution and return just the error.
func RepetitionDurationStats(reps int, setup, f, teardown func() error) (time.Duration, time.Duration, error) {
	if reps < 1 {
		return time.Duration(0), 0, fmt.Errorf("repetions need to be %d >= 1", reps)
	}
	if setup == nil {
		return time.Duration(0), 0, errors.New("setup function is required")
	}
	if f == nil {
		return time.Duration(0), 0, errors.New("benchmark function is required")
	}
	if teardown == nil {
		return time.Duration(0), 0, errors.New("teardown function is required")
	}
	var durations []time.Duration
	for i := 0; i < reps; i++ {
		if err := setup(); err != nil {
			return time.Duration(0), 0, err
		}
		d, err := TrackDuration(f)
		if err != nil {
			return 0, 0, err
		}
		durations = append(durations, d)
		if err := teardown(); err != nil {
			return time.Duration(0), 0, err
		}
	}
	mean := int64(0)
	for _, d := range durations {
		mean += int64(d)
	}
	mean /= int64(len(durations))
	dev, expSquare := int64(0), mean*mean
	for _, d := range durations {
		dev = int64(d)*int64(d) - expSquare
	}
	dev = int64(math.Sqrt(math.Abs(float64(dev))))
	return time.Duration(mean), time.Duration(dev), nil
}

// BenchEntry contains the bench entry id, the function to run, and the number
// of repetitions to run.
type BenchEntry struct {
	BatteryID string
	ID        string
	Triples   int
	Reps      int
	Setup     func() error
	F         func() error
	TearDown  func() error
}

// BenchResult contains the results of running a bench mark.
type BenchResult struct {
	BatteryID string
	ID        string
	Triples   int
	Err       error
	Mean      time.Duration
	StdDev    time.Duration
}

// RunBenchmarkBatterySequentially runs all the bench entries and returns the
// timing results.
func RunBenchmarkBatterySequentially(entries []*BenchEntry) []*BenchResult {
	var res []*BenchResult
	for _, entry := range entries {
		m, d, err := RepetitionDurationStats(entry.Reps, entry.Setup, entry.F, entry.TearDown)
		res = append(res, &BenchResult{
			BatteryID: entry.BatteryID,
			ID:        entry.ID,
			Triples:   entry.Triples,
			Err:       err,
			Mean:      m,
			StdDev:    d,
		})
	}
	return res
}

// RunBenchmarkBatteryConcurrently runs all the bench entries and returns the
// timing results concurrently. The benchmarks will all be run concurrently.
func RunBenchmarkBatteryConcurrently(entries []*BenchEntry) []*BenchResult {
	var (
		mu  sync.Mutex
		wg  sync.WaitGroup
		res []*BenchResult
	)
	for _, entry := range entries {
		wg.Add(1)
		go func(entry *BenchEntry) {
			m, d, err := RepetitionDurationStats(entry.Reps, entry.Setup, entry.F, entry.TearDown)
			mu.Lock()
			defer mu.Unlock()
			defer wg.Done()
			res = append(res, &BenchResult{
				BatteryID: entry.BatteryID,
				ID:        entry.ID,
				Triples:   entry.Triples,
				Err:       err,
				Mean:      m,
				StdDev:    d,
			})
		}(entry)
	}
	wg.Wait()
	return res
}
