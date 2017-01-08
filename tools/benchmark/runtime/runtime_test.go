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
	"sync"
	"testing"
	"time"
)

func init() {
	// Inject fake time clock.
	var mu sync.Mutex
	i := int64(0)
	timeNow = func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		t := time.Unix(i, 0)
		i++
		return t
	}
}

func TestIncreasingMonotonicTimeNowIncrease(t *testing.T) {
	to := timeNow()
	for i := 0; i <= 100; i++ {
		tn := timeNow()
		if got, old := tn, to; !got.After(old) {
			t.Fatalf("mock timeNow() should be monoticly ascending, got %d vs old %v instead", got, old)
		}
		to = tn
	}
}

func TestTrackDuration(t *testing.T) {
	if _, err := TrackDuration(func() error {
		return errors.New("some arbitrary error")
	}); err == nil {
		t.Fatalf("TrackDuration should have returned an error")
	}
	d, _ := TrackDuration(func() error {
		return nil
	})
	if d <= 0 {
		t.Fatalf("TrackDuration should have returned a valid duration")
	}
}

func TestDurationStats(t *testing.T) {
	nop := func() error {
		return nil
	}
	if _, _, err := RepetitionDurationStats(0, nop, nop, nop); err == nil {
		t.Fatalf("RepetitionDurationStats(0, _) should have failed with invalid repetitions count")
	}

	if _, _, err := RepetitionDurationStats(10, nop, func() error {
		return errors.New("some random error")
	}, nop); err == nil {
		t.Fatalf("RepetitionDurationStats(_, _) should have failed and propagate the error")
	}

	d, dev, err := RepetitionDurationStats(10, nop, nop, nop)
	if err != nil {
		t.Fatalf("RepetitionDurationStats(_, _) should have failed with %v", err)
	}
	if got, want := d, time.Second; got != want {
		t.Fatalf("RepetitionDurationStats(_, _) faild to compute the right mean; got %d, want %d", got, want)
	}
	if got, want := dev, time.Duration(0); got != want {
		t.Fatalf("RepetitionDurationStats(_, _) faild to compute the right deviation; got %d, want %d", got, want)
	}
}

func TestRunBenchmarkBattery(t *testing.T) {
	var testData []*BenchEntry
	for i := 0; i < 1000; i++ {
		testData = append(testData, &BenchEntry{
			ID:   "foo",
			Reps: 10,
			F: func() error {
				return nil
			},
		})
	}
	bes := RunBenchmarkBatterySequentially(testData)
	if got, want := len(bes), len(testData); got != want {
		t.Errorf("RunBenchmarkBatterySequentially(_) failed to return the right number of results; got %d, want %d", got, want)
	}
	bec := RunBenchmarkBatteryConcurrently(testData)
	if got, want := len(bec), len(testData); got != want {
		t.Errorf("RunBenchmarkBatteryConcurrently(_) failed to return the right number of results; got %d, want %d", got, want)
	}
}
