package main

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/badwolf/bql/grammar"
	"github.com/google/badwolf/bql/planner"
	"github.com/google/badwolf/bql/semantic"
	"github.com/google/badwolf/bql/table"
	"github.com/google/badwolf/storage"
	"github.com/google/badwolf/storage/memoization"
	"github.com/google/badwolf/storage/memory"
)

var env storage.Store

func init() {
	Reset()
}

// Reset the enviroment
func Reset() {
	env = memoization.New(memory.NewStore())
}

// Eval accepts Slang code and returns the string representation resulting
// of the evaluation.
func Eval(bqls string) string {
	b := bytes.NewBufferString("")
	ss := strings.Split(bqls, ";")
	if ss[len(ss)-1] == "" {
		ss = ss[0 : len(ss)-1]
	}
	for i, bql := range ss {
		b.WriteString(fmt.Sprintf("Statement %d:\n", i))
		now := time.Now()
		table, err := runBQL(context.Background(), bql+";", env, 0, 0)
		bqlDiff := time.Now().Sub(now)
		if err != nil {
			b.WriteString(fmt.Sprintf("[ERROR] %s\n", err))
			b.WriteString(fmt.Sprintf("Time spent: %v\n", time.Now().Sub(now)))
		} else {
			if table == nil {
				b.WriteString(fmt.Sprintf("[OK] 0 rows retrieved. BQL time: %v. Display time: %v\n",
					bqlDiff, time.Now().Sub(now)-bqlDiff))
			} else {
				if len(table.Bindings()) > 0 {
					b.WriteString(table.String())
				}
				b.WriteString(fmt.Sprintf("[OK] %d rows retrieved. BQL time: %v. Display time: %v\n",
					table.NumRows(), bqlDiff, time.Now().Sub(now)-bqlDiff))
			}
		}
		b.WriteString("\n")
	}
	return b.String()
}

// runBQL attempts to execute the provided query against the given store.
func runBQL(ctx context.Context, bql string, s storage.Store, chanSize, bulkSize int) (*table.Table, error) {
	pln, err := planBQL(ctx, bql, s, chanSize, bulkSize)
	if err != nil {
		return nil, err
	}
	if pln == nil {
		return nil, nil
	}
	res, err := pln.Execute(ctx)
	if err != nil {
		msg := fmt.Errorf("planner.Execute: failed to execute; %v", err)
		return nil, msg
	}
	return res, nil
}

// planBQL attempts to create the execution plan for the provided query against the given store.
func planBQL(ctx context.Context, bql string, s storage.Store, chanSize, bulkSize int) (planner.Executor, error) {
	bql = strings.TrimSpace(bql)
	if bql == ";" {
		return nil, nil
	}
	p, err := grammar.NewParser(grammar.SemanticBQL())
	if err != nil {
		return nil, fmt.Errorf("NewParser failed; %v", err)
	}
	stm := &semantic.Statement{}
	if err := p.Parse(grammar.NewLLk(bql, 1), stm); err != nil {
		return nil, fmt.Errorf("NewLLk parser failed; %v", err)
	}
	pln, err := planner.New(ctx, s, stm, chanSize, bulkSize, nil)
	if err != nil {
		return nil, fmt.Errorf("planer.New failed failed; %v", err)
	}
	return pln, nil
}
