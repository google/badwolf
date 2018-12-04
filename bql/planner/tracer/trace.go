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

// Trace attempts to write a trace if a valid writer is provided. The
// tracer is lazy on the string generation to avoid adding too much
// overhead when tracing ins not on.
func Trace(w io.Writer, msgs func() []string) {
	if w == nil {
		return
	}
	for _, msg := range msgs() {
		w.Write([]byte("["))
		w.Write([]byte(time.Now().Format("2006-01-02T15:04:05.999999-07:00")))
		w.Write([]byte("] "))
		w.Write([]byte(msg))
		w.Write([]byte("\n"))
	}
}
