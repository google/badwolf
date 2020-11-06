// Copyright 2018 Google Inc. All rights reserved.
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

// Package tracer contains the implementation of basic execution tracing tools.
package tracer

import (
	"io"
	"time"
)

// Arguments encapsulates the elements passed to the tracer.
type Arguments struct {
	Msgs []string
}

// event encapsulates a single tracing event.
type event struct {
	w          io.Writer
	t          time.Time
	tracerArgs func() *Arguments
}

// MessageTracer encapsulates the intrinsic verbosity of a given tracing message.
type MessageTracer struct {
	verbosity int
}

// tracerVerbosity represents the global verbosity level of the current tracer. Level 1 means minimum verbosity (printing
// only what is crucial) while level 3 means maximum verbosity (printing all available tracing messages).
var tracerVerbosity int

// events is the channel through which all the tracing events will be sent for being, in the
// future, consumed and written to the output.
var events chan *event

func init() {
	tracerVerbosity = 1               // The default tracer has minimum verbosity.
	events = make(chan *event, 10000) // Large enough to avoid blocking as much as possible.

	go func() {
		for e := range events {
			for _, msg := range e.tracerArgs().Msgs {
				e.w.Write([]byte("["))
				e.w.Write([]byte(e.t.Format(time.RFC3339Nano)))
				e.w.Write([]byte("] "))
				e.w.Write([]byte(msg))
				e.w.Write([]byte("\n"))
			}
		}
	}()
}

// SetVerbosity sets the global verbosity of the current tracer to the value received as
// input, 1 meaning minimum and 3 meaning maximum verbosity. If the received value is not
// in the interval [1, 3], then it is truncated. The function returns the actual verbosity set.
func SetVerbosity(verbosity int) int {
	// Truncate verbosity if out of the range supported.
	if verbosity < 1 {
		verbosity = 1
	} else if verbosity > 3 {
		verbosity = 3
	}

	tracerVerbosity = verbosity
	return tracerVerbosity
}

// V returns a MessageTracer with the specified verbosity level. Level 1 here means that the correspondent
// message has the highest priority and will always be printed to the tracing output, while 3 means that this message
// has the lowest priority and will be printed to the output only if the current tracer has maximum tracerVerbosity.
// If the received verbosity level is out of the range [1, 3] supported, then it is truncated.
func V(verbosity int) MessageTracer {
	// Truncate verbosity if out of the range supported.
	if verbosity < 1 {
		verbosity = 1
	} else if verbosity > 3 {
		verbosity = 3
	}

	return MessageTracer{verbosity}
}

// isTraceable returns true if the current tracer is verbose enough to let the given MessageTracer
// indeed trace its correspondent message.
func (t MessageTracer) isTraceable() bool {
	return t.verbosity <= tracerVerbosity
}

// Trace attempts to write a trace if a valid writer is provided and the verbosity level
// of the MessageTracer is coherent with the global tracer verbosity. The tracer is lazy
// on the arguments generation to avoid adding too much overhead when tracing is not on.
func (t MessageTracer) Trace(w io.Writer, tracerArgs func() *Arguments) {
	if w == nil || !t.isTraceable() {
		return
	}
	events <- &event{w, time.Now(), tracerArgs}
}
