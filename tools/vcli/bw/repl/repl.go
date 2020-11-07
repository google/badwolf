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

// Package repl contains the implementation of the command that prints the
// BQL version.
package repl

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"time"

	"github.com/google/badwolf/bql/planner/tracer"
	"github.com/google/badwolf/storage/memoization"

	"github.com/google/badwolf/bql/grammar"
	"github.com/google/badwolf/bql/planner"
	"github.com/google/badwolf/bql/semantic"
	"github.com/google/badwolf/bql/table"
	"github.com/google/badwolf/bql/version"
	"github.com/google/badwolf/storage"
	"github.com/google/badwolf/tools/vcli/bw/command"
	"github.com/google/badwolf/tools/vcli/bw/export"
	bio "github.com/google/badwolf/tools/vcli/bw/io"
	"github.com/google/badwolf/tools/vcli/bw/load"
)

const prompt = "bql> "

// New create the version command.
func New(driver storage.Store, chanSize, bulkSize, builderSize int, rl ReadLiner, done chan bool) *command.Command {
	return &command.Command{
		Run: func(ctx context.Context, args []string) int {
			REPL(driver, os.Stdin, rl, chanSize, bulkSize, builderSize, done)
			return 0
		},
		UsageLine: "bql",
		Short:     "starts a REPL to run BQL statements.",
		Long:      "Starts a REPL from the command line to accept BQL statements. Type quit; to leave the REPL.",
	}
}

// ReadLiner returns a channel with the imput to be used for the REPL.
type ReadLiner func(done chan bool) <-chan string

// SimpleReadLine reads a line from the provided file. This does not support
// any advanced terminal capabilities.
//
// This function can be replaced with more advanced functionality, as shown
// https://github.com/xllora/bwdrivers/blob/master/bw/main.go.
func SimpleReadLine(done chan bool) <-chan string {
	c := make(chan string)
	go func() {
		defer close(c)
		scanner := bufio.NewScanner(os.Stdin)
		cmd := ""
		fmt.Print("bql> ")
		for {
			if !scanner.Scan() {
				break
			}
			cmd = strings.TrimSpace(cmd + " " + strings.TrimSpace(scanner.Text()))
			if strings.HasSuffix(cmd, ";") {
				c <- cmd
				if <-done {
					break
				}
				cmd = ""
				fmt.Print("bql> ")
			}
		}
	}()
	return c
}

// startProfiling tries to start pprof profiling, returning the two files to which the profiling metrics will be printed.
func startProfiling() (*os.File, *os.File, error) {
	cpuProfile, err := os.Create("cpuprofile")
	if err != nil {
		return nil, nil, err
	}
	memProfile, err := os.Create("memprofile")
	if err != nil {
		return nil, nil, err
	}

	err = pprof.StartCPUProfile(cpuProfile)
	if err != nil {
		return nil, nil, err
	}

	return cpuProfile, memProfile, nil
}

// stopProfiling stops pprof profiling, writing the final profiling metrics to the correspondent files and closing them.
func stopProfiling(cpuProfile, memProfile *os.File) {
	if cpuProfile == nil || memProfile == nil {
		fmt.Println("cpuProfile and memProfile must be both non-nil to stop profiling.")
		return
	}

	pprof.StopCPUProfile()
	err := pprof.WriteHeapProfile(memProfile)
	if err != nil {
		fmt.Println(err)
	}

	cpuProfile.Close()
	memProfile.Close()
}

// REPL starts a read-evaluation-print-loop to run BQL commands.
func REPL(od storage.Store, input *os.File, rl ReadLiner, chanSize, bulkSize, builderSize int, done chan bool) int {
	var traceWriter io.Writer
	ctx, isTracingToFile, isProfiling, sessionStart := context.Background(), false, false, time.Now()
	var cpuProfile, memProfile *os.File

	driverPlain := func() storage.Store {
		return od
	}

	driverWithMemoization := func() storage.Store {
		return memoization.New(od)
	}

	driver := driverWithMemoization

	stopTracing := func() {
		if traceWriter != nil {
			if isTracingToFile {
				fmt.Println("Closing tracing file.")
				traceWriter.(*os.File).Close()
			}
			traceWriter, isTracingToFile = nil, false
		}
	}
	defer stopTracing()

	fmt.Printf("Welcome to BadWolf vCli (%d.%d.%d-%s)\n", version.Major, version.Minor, version.Patch, version.Release)
	fmt.Printf("Using driver %s/%s. Type quit; to exit.\n", driver().Name(ctx), driver().Version(ctx))
	fmt.Printf("Session started at %v.\n", sessionStart.Format("2006-01-02T15:04:05.999999-07:00"))
	fmt.Println("Memoization enabled. Type help; to print help.")
	fmt.Println()
	defer func() {
		fmt.Printf("\n\nThanks for all those BQL queries!\nSession duration: %v\n\n", time.Now().Sub(sessionStart))
	}()

	for l := range rl(done) {
		if strings.HasPrefix(l, "quit") {
			if isProfiling {
				fmt.Println("Stopping profiling and closing correspondent files.")
				stopProfiling(cpuProfile, memProfile)
				isProfiling = false
			}
			done <- true
			break
		}
		if strings.HasPrefix(l, "help") {
			printHelp()
			done <- false
			continue
		}
		if strings.HasPrefix(l, "enable memoization") {
			driver = driverWithMemoization
			fmt.Println("[OK] Partial query memoization is on.")
			done <- false
			continue
		}
		if strings.HasPrefix(l, "disable memoization") {
			driver = driverPlain
			fmt.Println("[OK] Partial query memoization is off.")
			done <- false
			continue
		}
		if strings.HasPrefix(l, "start tracing") {
			args := strings.Split(strings.TrimSpace(l)[:len(l)-1], " ")
			switch len(args) {
			case 2:
				// Start tracing to the console.
				stopTracing()
				traceWriter, isTracingToFile = os.Stdout, false
				tracer.SetVerbosity(1)
				fmt.Println("[WARNING] Tracing is on. This may slow your BQL queries.\nDefault verbosity level set to 1 (minimum).")
			case 3:
				// Start tracing to file.
				stopTracing()
				f, err := os.Create(args[2])
				if err != nil {
					fmt.Println(err)
					fmt.Println("Tracing failed to start.")
					break
				}
				traceWriter, isTracingToFile = f, true
				tracer.SetVerbosity(1)
				fmt.Printf("[WARNING] Tracing to %q is on. This may slow your BQL queries.\nDefault verbosity level set to 1 (minimum).\n", f.Name())
			case 4:
				// Start tracing to the console with specified verbosity level.
				stopTracing()
				if args[2] != "-v" {
					fmt.Printf("Invalid syntax with %q.\n\tstart tracing [-v verbosity_level] [trace_file]\n", args[2])
					break
				}
				verbosity, err := strconv.ParseInt(args[3], 10, 32)
				if err != nil {
					fmt.Println(err)
					fmt.Println("Tracing failed to start.")
					break
				}
				traceWriter, isTracingToFile = os.Stdout, false
				verbositySet := tracer.SetVerbosity(int(verbosity))
				fmt.Printf("[WARNING] Tracing is on. This may slow your BQL queries.\nVerbosity level set to %d.\n", verbositySet)
			case 5:
				// Start tracing to file with specified verbosity level.
				stopTracing()
				if args[2] != "-v" {
					fmt.Printf("Invalid syntax with %q.\n\tstart tracing [-v verbosity_level] [trace_file]\n", args[2])
					break
				}
				verbosity, err := strconv.ParseInt(args[3], 10, 32)
				if err != nil {
					fmt.Println(err)
					fmt.Println("Tracing failed to start.")
					break
				}
				f, err := os.Create(args[4])
				if err != nil {
					fmt.Println(err)
					fmt.Println("Tracing failed to start.")
					break
				}
				traceWriter, isTracingToFile = f, true
				verbositySet := tracer.SetVerbosity(int(verbosity))
				fmt.Printf("[WARNING] Tracing to %q is on. This may slow your BQL queries.\nVerbosity level set to %d.\n", f.Name(), verbositySet)
			default:
				fmt.Println("Invalid syntax.\n\tstart tracing [-v verbosity_level] [trace_file]")
			}
			done <- false
			continue
		}
		if strings.HasPrefix(l, "stop tracing") {
			stopTracing()
			fmt.Println("Tracing is off.")
			done <- false
			continue
		}
		if strings.HasPrefix(l, "start profiling") {
			if isProfiling {
				fmt.Println("Profiling is already ongoing.")
				done <- false
				continue
			}
			args := strings.Split(strings.TrimSpace(l)[:len(l)-1], " ")
			var err error
			switch len(args) {
			case 2:
				cpuProfile, memProfile, err = startProfiling()
				if err != nil {
					fmt.Println(err)
					fmt.Println("Profiling failed to start.")
					break
				}
				isProfiling = true
				fmt.Println("Profiling with pprof is on.")
			case 4:
				if args[2] != "-cpurate" {
					fmt.Printf("Invalid syntax with %q.\n\tstart profiling [-cpurate samples_per_second]\n", args[2])
					break
				}
				cpuProfRate, err := strconv.ParseInt(args[3], 10, 32)
				if err != nil {
					fmt.Println(err)
					fmt.Println("Profiling failed to start.")
					break
				}
				runtime.SetCPUProfileRate(int(cpuProfRate))
				cpuProfile, memProfile, err = startProfiling()
				if err != nil {
					fmt.Println(err)
					fmt.Println("Profiling failed to start.")
					break
				}
				isProfiling = true
				fmt.Printf("Profiling with pprof is on (CPU profiling rate: %d samples per second).\n", cpuProfRate)
			default:
				fmt.Println("Invalid syntax.\n\tstart profiling [-cpurate samples_per_second]")
			}
			done <- false
			continue
		}
		if strings.HasPrefix(l, "stop profiling") {
			if isProfiling {
				stopProfiling(cpuProfile, memProfile)
				isProfiling = false
				fmt.Println("Profiling with pprof is turned off.")
			} else {
				fmt.Println("Profiling with pprof is already off.")
			}
			done <- false
			continue
		}
		if strings.HasPrefix(l, "export") {
			now := time.Now()
			args := strings.Split("bw "+strings.TrimSpace(l)[:len(l)-1], " ")
			usage := "Wrong syntax\n\n\tload <graph_names_separated_by_commas> <file_path>\n"
			export.Eval(ctx, usage, args, driver(), bulkSize)
			fmt.Println("[OK] Time spent: ", time.Now().Sub(now))
			done <- false
			continue
		}
		if strings.HasPrefix(l, "load") {
			now := time.Now()
			args := strings.Split("bw "+strings.TrimSpace(l[:len(l)-1]), " ")
			usage := "Wrong syntax\n\n\tload <file_path> <graph_names_separated_by_commas>\n"
			load.Eval(ctx, usage, args, driver(), bulkSize, builderSize)
			fmt.Println("[OK] Time spent: ", time.Now().Sub(now))
			done <- false
			continue
		}
		if strings.HasPrefix(l, "desc") {
			pln, err := planBQL(ctx, l[4:], driver(), chanSize, bulkSize, nil)
			if err != nil {
				fmt.Printf("[ERROR] %s\n\n", err)
			} else {
				if pln != nil {
					fmt.Println(pln.String(ctx))
				}
				fmt.Println("[OK]")
			}
			done <- false
			continue
		}
		if strings.HasPrefix(l, "run") {
			now := time.Now()
			path, cmds, err := runBQLFromFile(ctx, driver(), chanSize, bulkSize, strings.TrimSpace(l[:len(l)-1]), traceWriter)
			if err != nil {
				fmt.Printf("[ERROR] %s\n\n", err)
			} else {
				fmt.Printf("Loaded %q and run %d BQL commands successfully\n\n", path, cmds)
			}
			fmt.Println("Time spent: ", time.Now().Sub(now))
			done <- false
			continue
		}

		now := time.Now()
		table, err := runBQL(ctx, l, driver(), chanSize, bulkSize, traceWriter)
		bqlDiff := time.Now().Sub(now)
		if err != nil {
			fmt.Printf("[ERROR] %s\n", err)
			fmt.Println("Time spent: ", time.Now().Sub(now))
			fmt.Println()
		} else {
			if table == nil {
				fmt.Printf("[OK] 0 rows retrieved. BQL time: %v. Display time: %v\n",
					bqlDiff, time.Now().Sub(now)-bqlDiff)
			} else {
				if len(table.Bindings()) > 0 {
					fmt.Println(table.String())
				}
				fmt.Printf("[OK] %d rows retrieved. BQL time: %v. Display time: %v\n",
					table.NumRows(), bqlDiff, time.Now().Sub(now)-bqlDiff)
			}
		}
		done <- false
	}
	return 0
}

// printHelp prints help for the console commands.
func printHelp() {
	fmt.Println()
	fmt.Println("help                                                  - prints help for the bw console.")
	fmt.Println("disable memoization                                   - disables partial result memoization on query resolution.")
	fmt.Println("enable memoization                                    - enables partial result memoization of partial query results.")
	fmt.Println("export <graph_names_separated_by_commas> <file_path>  - dumps triples from graphs into a file path.")
	fmt.Println("desc <BQL>                                            - prints the execution plan for a BQL statement.")
	fmt.Println("load <file_path> <graph_names_separated_by_commas>    - load triples into the specified graphs.")
	fmt.Println("run <file_with_bql_statements>                        - runs all the BQL statements in the file.")
	fmt.Println("start tracing [-v verbosity_level] [trace_file]       - starts tracing queries, verbosity levels supported are 1, 2 and 3 (with 3 meaning maximum verbosity).")
	fmt.Println("stop tracing                                          - stops tracing queries.")
	fmt.Println("start profiling [-cpurate samples_per_second]         - starts pprof profiling for queries (customizable CPU sampling rate).")
	fmt.Println("stop profiling                                        - stops pprof profiling for queries.")
	fmt.Println("quit                                                  - quits the console.")
	fmt.Println()
}

// runBQLFromFile loads all the statements in the file and runs them.
func runBQLFromFile(ctx context.Context, driver storage.Store, chanSize, bulkSize int, line string, w io.Writer) (string, int, error) {
	ss := strings.Split(strings.TrimSpace(line), " ")
	if len(ss) != 2 {
		return "", 0, fmt.Errorf("wrong syntax: run <file_with_bql_statements>")
	}
	path := ss[1]
	tracer.V(1).Trace(w, func() *tracer.Arguments {
		return &tracer.Arguments{
			Msgs: []string{fmt.Sprintf("Attempting to read file %q", path)},
		}
	})
	lines, err := bio.GetStatementsFromFile(path)
	if err != nil {
		msg := fmt.Errorf("failed to read file %q; error %v", path, err)
		tracer.V(1).Trace(w, func() *tracer.Arguments {
			return &tracer.Arguments{
				Msgs: []string{msg.Error()},
			}
		})
		return "", 0, msg
	}
	for idx, stm := range lines {
		fmt.Printf("Processing statement (%d/%d)\n", idx+1, len(lines))
		_, err := runBQL(ctx, stm, driver, chanSize, bulkSize, w)
		if err != nil {
			msg := fmt.Errorf("%q; %v", stm, err)
			tracer.V(1).Trace(w, func() *tracer.Arguments {
				return &tracer.Arguments{
					Msgs: []string{msg.Error()},
				}
			})
			return "", 0, msg
		}
	}
	fmt.Println()
	return path, len(lines), nil
}

// runBQL attempts to execute the provided query against the given store.
func runBQL(ctx context.Context, bql string, s storage.Store, chanSize, bulkSize int, w io.Writer) (*table.Table, error) {
	tracer.V(1).Trace(w, func() *tracer.Arguments {
		return &tracer.Arguments{
			Msgs: []string{fmt.Sprintf("Executing query: %s", bql)},
		}
	})
	pln, err := planBQL(ctx, bql, s, chanSize, bulkSize, w)
	if err != nil {
		return nil, err
	}
	if pln == nil {
		return nil, nil
	}
	res, err := pln.Execute(ctx)
	if err != nil {
		msg := fmt.Errorf("planner.Execute: failed to execute; %v", err)
		tracer.V(1).Trace(w, func() *tracer.Arguments {
			return &tracer.Arguments{
				Msgs: []string{msg.Error()},
			}
		})
		return nil, msg
	}
	nRows := res.NumRows()
	tracer.V(1).Trace(w, func() *tracer.Arguments {
		return &tracer.Arguments{
			Msgs: []string{fmt.Sprintf("Executed plan returned %d rows", nRows)},
		}
	})
	return res, nil
}

// planBQL attempts to create the execution plan for the provided query against the given store.
func planBQL(ctx context.Context, bql string, s storage.Store, chanSize, bulkSize int, w io.Writer) (planner.Executor, error) {
	bql = strings.TrimSpace(bql)
	if bql == ";" {
		tracer.V(1).Trace(w, func() *tracer.Arguments {
			return &tracer.Arguments{
				Msgs: []string{"Empty statement found"},
			}
		})
		return nil, nil
	}
	p, err := grammar.NewParser(grammar.SemanticBQL())
	if err != nil {
		msg := fmt.Errorf("NewParser failed; %v", err)
		tracer.V(1).Trace(w, func() *tracer.Arguments {
			return &tracer.Arguments{
				Msgs: []string{msg.Error()},
			}
		})
		return nil, msg
	}
	stm := &semantic.Statement{}
	if err := p.Parse(grammar.NewLLk(bql, 1), stm); err != nil {
		msg := fmt.Errorf("NewLLk parser failed; %v", err)
		tracer.V(1).Trace(w, func() *tracer.Arguments {
			return &tracer.Arguments{
				Msgs: []string{msg.Error()},
			}
		})
		return nil, msg
	}
	pln, err := planner.New(ctx, s, stm, chanSize, bulkSize, w)
	if err != nil {
		msg := fmt.Errorf("planner.New failed with error: %v", err)
		tracer.V(1).Trace(w, func() *tracer.Arguments {
			return &tracer.Arguments{
				Msgs: []string{msg.Error()},
			}
		})
		return nil, msg
	}
	tracer.V(1).Trace(w, func() *tracer.Arguments {
		return &tracer.Arguments{
			Msgs: []string{"Plan successfully created"},
		}
	})
	return pln, nil
}
