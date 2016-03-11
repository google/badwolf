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
	"fmt"
	"math"
	"time"
)

// Use to allow injection of mock time Now during testing.
var timeNow = time.Now

// TrackDuration measure the duration of the run time function using the
// wall clock. You should not consider the returned duration in the precense
// or an error since it will likely have shortcut the excecution of the
// function being executed.
func TrackDuration(f func() error) (time.Duration, error) {
	ts := timeNow()
	err := f()
	d := timeNow().Sub(ts)
	return d, err
}

// DurationStats extracts some duration stats by repeateadly execution and
// measuring runtime. Retuns the mean and deviation of the run duration of the
// provided function. If an error is return by the function it will shortcut
// the execution and return just the error.
func DurationStats(reps int, f func() error) (time.Duration, int64, error) {
	if reps < 1 {
		return time.Duration(0), 0, fmt.Errorf("repetions need to be %d >= 1", reps)
	}
	var durations []time.Duration
	for i := 0; i < reps; i++ {
		d, err := TrackDuration(f)
		if err != nil {
			return 0, 0, err
		}
		durations = append(durations, d)
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
	dev = int64(math.Sqrt(float64(dev)))
	return time.Duration(mean), dev, nil
}
