// Copyright 2015 Google Inc. All rights reserved.
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

// Package planner contains all the machinery to transform the semantic output
// into an actionable plan.
package planner

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/context"

	"github.com/google/badwolf/bql/lexer"
	"github.com/google/badwolf/bql/semantic"
	"github.com/google/badwolf/bql/table"
	"github.com/google/badwolf/storage"
	"github.com/google/badwolf/triple"
	"github.com/google/badwolf/triple/literal"
)

// Executor interface unifies the execution of statements.
type Executor interface {
	// Execute runs the proposed plan for a given statement.
	Execute(ctx context.Context) (*table.Table, error)

	// String returns a readable description of the execution plan.
	String() string
}

// trace attemps to write a trace if a valid writer is provided. The
// tracer is lazy on the string generation to avoid adding too much
// overhead when tracing ins not on.
func trace(w io.Writer, msgs func() []string) {
	if w == nil {
		return
	}
	for _, msg := range msgs() {
		w.Write([]byte("["))
		w.Write([]byte(time.Now().String()))
		w.Write([]byte("] "))
		w.Write([]byte(msg))
		w.Write([]byte("\n"))
	}
}

// createPlan encapsulates the sequence of instructions that need to be
// executed in order to satisfy the execution of a valid create BQL statement.
type createPlan struct {
	stm    *semantic.Statement
	store  storage.Store
	tracer io.Writer
}

// Execute creates the indicated graphs.
func (p *createPlan) Execute(ctx context.Context) (*table.Table, error) {
	t, err := table.New([]string{})
	if err != nil {
		return nil, err
	}
	errs := []string{}
	for _, g := range p.stm.GraphNames() {
		trace(p.tracer, func() []string {
			return []string{"Creating new graph \"" + g + "\""}
		})
		if _, err := p.store.NewGraph(ctx, g); err != nil {
			errs = append(errs, err.Error())
		}
	}
	if len(errs) > 0 {
		return nil, errors.New(strings.Join(errs, "; "))
	}
	return t, nil
}

// String returns a readable description of the execution plan.
func (p *createPlan) String() string {
	return fmt.Sprintf("CREATE plan:\n\nstore(%q).NewGraph(_, %v)", p.store.Name(nil), p.stm.Graphs())
}

// dropPlan encapsulates the sequence of instructions that need to be
// executed in order to satisfy the execution of a valid drop BQL statement.
type dropPlan struct {
	stm    *semantic.Statement
	store  storage.Store
	tracer io.Writer
}

// Execute drops the indicated graphs.
func (p *dropPlan) Execute(ctx context.Context) (*table.Table, error) {
	t, err := table.New([]string{})
	if err != nil {
		return nil, err
	}
	errs := []string{}
	for _, g := range p.stm.GraphNames() {
		trace(p.tracer, func() []string {
			return []string{"Deleting graph \"" + g + "\""}
		})
		if err := p.store.DeleteGraph(ctx, g); err != nil {
			errs = append(errs, err.Error())
		}
	}
	if len(errs) > 0 {
		return nil, errors.New(strings.Join(errs, "; "))
	}
	return t, nil
}

// String returns a readable description of the execution plan.
func (p *dropPlan) String() string {
	return fmt.Sprintf("DROP plan:\n\nstore(%q).DeleteGraph(_, %v)", p.store.Name(nil), p.stm.Graphs())
}

// insertPlan encapsulates the sequence of instructions that need to be
// executed in order to satisfy the execution of a valid insert BQL statement.
type insertPlan struct {
	stm    *semantic.Statement
	store  storage.Store
	tracer io.Writer
}

type updater func(storage.Graph, []*triple.Triple) error

func update(ctx context.Context, stm *semantic.Statement, store storage.Store, f updater) error {
	var (
		mu   sync.Mutex
		wg   sync.WaitGroup
		errs []string
	)
	appendError := func(err error) {
		mu.Lock()
		defer mu.Unlock()
		errs = append(errs, err.Error())
	}

	for _, graphBinding := range stm.GraphNames() {
		wg.Add(1)
		go func(graph string) {
			defer wg.Done()
			g, err := store.Graph(ctx, graph)
			if err != nil {
				appendError(err)
				return
			}
			err = f(g, stm.Data())
			if err != nil {
				appendError(err)
			}
		}(graphBinding)
	}
	wg.Wait()
	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "; "))
	}
	return nil
}

// Execute inserts the provided data into the indicated graphs.
func (p *insertPlan) Execute(ctx context.Context) (*table.Table, error) {
	t, err := table.New([]string{})
	if err != nil {
		return nil, err
	}
	return t, update(ctx, p.stm, p.store, func(g storage.Graph, d []*triple.Triple) error {
		trace(p.tracer, func() []string {
			return []string{"Inserting triples to graph \"" + g.ID(ctx) + "\""}
		})
		return g.AddTriples(ctx, d)
	})
}

// String returns a readable description of the execution plan.
func (p *insertPlan) String() string {
	b := bytes.NewBufferString("INSERT plan:\n\n")
	for _, g := range p.stm.Graphs() {
		b.WriteString(fmt.Sprintf("store(%q).Graph(%q).AddTriples(_, data)\n", p.store.Name(nil), g))
	}
	b.WriteString("where data:\n")
	for _, t := range p.stm.Data() {
		b.WriteString("\t")
		b.WriteString(t.String())
		b.WriteString("\n")
	}
	return b.String()
}

// deletePlan encapsulates the sequence of instructions that need to be
// executed in order to satisfy the execution of a valid delete BQL statement.
type deletePlan struct {
	stm    *semantic.Statement
	store  storage.Store
	tracer io.Writer
}

// Execute deletes the provided data into the indicated graphs.
func (p *deletePlan) Execute(ctx context.Context) (*table.Table, error) {
	t, err := table.New([]string{})
	if err != nil {
		return nil, err
	}
	return t, update(ctx, p.stm, p.store, func(g storage.Graph, d []*triple.Triple) error {
		trace(p.tracer, func() []string {
			return []string{"Removing triples from graph \"" + g.ID(ctx) + "\""}
		})
		return g.RemoveTriples(ctx, d)
	})
}

// String returns a readable description of the execution plan.
func (p *deletePlan) String() string {
	b := bytes.NewBufferString("DELETE plan:\n\n")
	for _, g := range p.stm.Graphs() {
		b.WriteString(fmt.Sprintf("store(%q).Graph(%q).RemoveTriples(_, data)\n", p.store.Name(nil), g))
	}
	b.WriteString("where data:\n")
	for _, t := range p.stm.Data() {
		b.WriteString("\t")
		b.WriteString(t.String())
		b.WriteString("\n")
	}
	return b.String()
}

// queryPlan encapsulates the sequence of instructions that need to be
// executed in order to satisfy the execution of a valid query BQL statement.
type queryPlan struct {
	// Plan input.
	stm   *semantic.Statement
	store storage.Store
	// Prepared plan information.
	bndgs     []string
	grfsNames []string
	grfs      []storage.Graph
	cls       []*semantic.GraphClause
	tbl       *table.Table
	chanSize  int
	tracer    io.Writer
}

// newQueryPlan returns a new query plan ready to be executed.
func newQueryPlan(ctx context.Context, store storage.Store, stm *semantic.Statement, chanSize int, w io.Writer) (*queryPlan, error) {
	bs := []string{}
	for _, b := range stm.Bindings() {
		bs = append(bs, b)
	}
	t, err := table.New([]string{})
	if err != nil {
		return nil, err
	}
	return &queryPlan{
		stm:       stm,
		store:     store,
		bndgs:     bs,
		grfsNames: stm.GraphNames(),
		cls:       stm.SortedGraphPatternClauses(),
		tbl:       t,
		chanSize:  chanSize,
		tracer:    w,
	}, nil
}

// processClause retrieves the triples for the provided triple given the
// information available.
func (p *queryPlan) processClause(ctx context.Context, cls *semantic.GraphClause, lo *storage.LookupOptions) (bool, error) {
	// This method decides how to process the clause based on the current
	// list of bindings solved and data available.
	if cls.Specificity() == 3 {
		t, err := triple.New(cls.S, cls.P, cls.O)
		if err != nil {
			return false, err
		}
		b, tbl, err := simpleExist(ctx, p.grfs, cls, t)
		if err != nil {
			return false, err
		}
		if err := p.tbl.AppendTable(tbl); err != nil {
			return b, err
		}
		return b, nil
	}
	exist, total := 0, 0
	for _, b := range cls.Bindings() {
		total++
		if p.tbl.HasBinding(b) {
			exist++
		}
	}
	if exist == 0 {
		// Data is new.
		tbl, err := simpleFetch(ctx, p.grfs, cls, lo, p.chanSize)
		if err != nil {
			return false, err
		}
		if len(p.tbl.Bindings()) > 0 {
			return false, p.tbl.DotProduct(tbl)
		}
		return false, p.tbl.AppendTable(tbl)
	}
	if exist > 0 && exist < total {
		// Data is partially bound, retrieve data either extends the row with the
		// new bindings or filters it out if now new bindings are available.
		return false, p.specifyClauseWithTable(ctx, cls, lo)
	}
	if exist > 0 && exist == total {
		// Since all bindings in the clause are already solved, the clause becomes a
		// fully specified triple. If the triple does not exist the row will be
		// deleted.
		return false, p.filterOnExistence(ctx, cls, lo)
	}
	// Something is wrong with the code.
	return false, fmt.Errorf("queryPlan.processClause(%v) should have never failed to resolve the clause", cls)
}

// getBoundValueForComponent return the unique bound value if available on
// the provided row.
func getBoundValueForComponent(r table.Row, bs []string) *table.Cell {
	var cs []*table.Cell
	for _, b := range bs {
		if v, ok := r[b]; ok {
			cs = append(cs, v)
		}
	}
	if len(cs) == 1 || len(cs) == 2 && reflect.DeepEqual(cs[0], cs[1]) {
		return cs[0]
	}
	return nil
}

// addSpecifiedData specializes the clause given the row provided and attempt to
// retrieve the corresponding clause data.
func (p *queryPlan) addSpecifiedData(ctx context.Context, r table.Row, cls *semantic.GraphClause, lo *storage.LookupOptions) error {
	if cls.S == nil {
		v := getBoundValueForComponent(r, []string{cls.SBinding, cls.SAlias})
		if v != nil {
			if v.N != nil {
				cls.S = v.N
			}
		}
	}
	if cls.P == nil {
		v := getBoundValueForComponent(r, []string{cls.PBinding, cls.PAlias})
		if v != nil {
			if v.N != nil {
				cls.P = v.P
			}
		}
		nlo, err := updateTimeBoundsForRow(lo, cls, r)
		if err != nil {
			return err
		}
		lo = nlo
	}
	if cls.O == nil {
		v := getBoundValueForComponent(r, []string{cls.OBinding, cls.OAlias})
		if v != nil {
			o, err := cellToObject(v)
			if err == nil {
				cls.O = o
			}
		}
		nlo, err := updateTimeBoundsForRow(lo, cls, r)
		if err != nil {
			return err
		}
		lo = nlo
	}
	tbl, err := simpleFetch(ctx, p.grfs, cls, lo, p.chanSize)
	if err != nil {
		return err
	}
	p.tbl.AddBindings(tbl.Bindings())
	for _, nr := range tbl.Rows() {
		p.tbl.AddRow(table.MergeRows([]table.Row{r, nr}))
	}
	return nil
}

// specifyClauseWithTable runs the clause, but it specifies it further based on
// the current row being processed.
func (p *queryPlan) specifyClauseWithTable(ctx context.Context, cls *semantic.GraphClause, lo *storage.LookupOptions) error {
	rws := p.tbl.Rows()
	p.tbl.Truncate()
	for _, r := range rws {
		tmpCls := &semantic.GraphClause{}
		*tmpCls = *cls
		if err := p.addSpecifiedData(ctx, r, tmpCls, lo); err != nil {
			return err
		}
	}
	return nil
}

// cellToObject returns an object for the given cell.
func cellToObject(c *table.Cell) (*triple.Object, error) {
	if c == nil {
		return nil, errors.New("cannot create an object out of and empty cell")
	}
	if c.N != nil {
		return triple.NewNodeObject(c.N), nil
	}
	if c.P != nil {
		return triple.NewPredicateObject(c.P), nil
	}
	if c.L != nil {
		return triple.NewLiteralObject(c.L), nil
	}
	if c.S != nil {
		l, err := literal.DefaultBuilder().Parse(fmt.Sprintf(`"%s"^^type:string`, *c.S))
		if err != nil {
			return nil, err
		}
		return triple.NewLiteralObject(l), nil
	}
	return nil, fmt.Errorf("invalid cell %v", c)
}

// filterOnExistence removes rows based on the existence of the fully qualified
// triple after the biding of the clause.
func (p *queryPlan) filterOnExistence(ctx context.Context, cls *semantic.GraphClause, lo *storage.LookupOptions) error {
	rows := p.tbl.Rows()
	for idx, pending := 0, len(rows); pending > 0; pending-- {
		r := rows[idx]
		sbj, prd, obj := cls.S, cls.P, cls.O
		// Attempt to rebind the subject.
		if sbj == nil && p.tbl.HasBinding(cls.SBinding) {
			v, ok := r[cls.SBinding]
			if !ok {
				return fmt.Errorf("row %+v misses binding %q", r, cls.SBinding)
			}
			if v.N == nil {
				return fmt.Errorf("binding %q requires a node, got %+v instead", cls.SBinding, v)
			}
			sbj = v.N
		}
		if sbj == nil && p.tbl.HasBinding(cls.SAlias) {
			v, ok := r[cls.SAlias]
			if !ok {
				return fmt.Errorf("row %+v misses binding %q", r, cls.SAlias)
			}
			if v.N == nil {
				return fmt.Errorf("binding %q requires a node, got %+v instead", cls.SAlias, v)
			}
			sbj = v.N
		}
		// Attempt to rebind the predicate.
		if prd == nil && p.tbl.HasBinding(cls.PBinding) {
			v, ok := r[cls.PBinding]
			if !ok {
				return fmt.Errorf("row %+v misses binding %q", r, cls.PBinding)
			}
			if v.P == nil {
				return fmt.Errorf("binding %q requires a predicate, got %+v instead", cls.PBinding, v)
			}
			prd = v.P
		}
		if prd == nil && p.tbl.HasBinding(cls.PAlias) {
			v, ok := r[cls.PAlias]
			if !ok {
				return fmt.Errorf("row %+v misses binding %q", r, cls.SAlias)
			}
			if v.N == nil {
				return fmt.Errorf("binding %q requires a predicate, got %+v instead", cls.SAlias, v)
			}
			prd = v.P
		}
		// Attempt to rebind the object.
		if obj == nil && p.tbl.HasBinding(cls.OBinding) {
			v, ok := r[cls.OBinding]
			if !ok {
				return fmt.Errorf("row %+v misses binding %q", r, cls.OBinding)
			}
			co, err := cellToObject(v)
			if err != nil {
				return err
			}
			obj = co
		}
		if obj == nil && p.tbl.HasBinding(cls.OAlias) {
			v, ok := r[cls.OAlias]
			if !ok {
				return fmt.Errorf("row %+v misses binding %q", r, cls.OAlias)
			}
			if v.N == nil {
				return fmt.Errorf("binding %q requires a object, got %+v instead", cls.OAlias, v)
			}
			co, err := cellToObject(v)
			if err != nil {
				return err
			}
			obj = co
		}
		// Attempt to filter.
		if sbj == nil || prd == nil || obj == nil {
			return fmt.Errorf("failed to fully specify clause %v for row %+v", cls, r)
		}
		for _, g := range p.stm.Graphs() {
			t, err := triple.New(sbj, prd, obj)
			if err != nil {
				return err
			}
			b, err := g.Exist(ctx, t)
			if err != nil {
				return err
			}
			if !b {
				p.tbl.DeleteRow(idx)
				idx--
			}
		}
		idx++
	}
	return nil
}

// processGraphPattern process the query graph pattern to retrieve the
// data from the specified graphs.
func (p *queryPlan) processGraphPattern(ctx context.Context, lo *storage.LookupOptions) error {
	for _, cls := range p.cls {
		trace(p.tracer, func() []string {
			return []string{"Processing graph clause " + cls.String()}
		})
		// The current planner is based on naively executing clauses by
		// specificity.
		unresolvable, err := p.processClause(ctx, cls, lo)
		if err != nil {
			return err
		}
		if unresolvable {
			p.tbl.Truncate()
			return nil
		}
	}
	return nil
}

// projectAndGroupBy takes the resulting table and projects its contents and
// groups it by if needed.
func (p *queryPlan) projectAndGroupBy() error {
	grp := p.stm.GroupByBindings()
	if len(grp) == 0 { // The table only needs to be projected.
		trace(p.tracer, func() []string {
			return []string{fmt.Sprintf("Running projection for %v", grp)}
		})
		p.tbl.AddBindings(p.stm.OutputBindings())
		// For each row, copy each input binding value to its appropriate alias.
		for _, prj := range p.stm.Projections() {
			for _, row := range p.tbl.Rows() {
				row[prj.Alias] = row[prj.Binding]
			}
		}
		trace(p.tracer, func() []string {
			return []string{fmt.Sprintf("Output bindings projected %v", p.stm.OutputBindings())}
		})
		return p.tbl.ProjectBindings(p.stm.OutputBindings())
	}
	trace(p.tracer, func() []string {
		return []string{"Starting roup reduce and projection"}
	})
	// The table needs to be group reduced.
	// Project only binding involved in the group operation.
	tmpBindings := []string{}
	mapBindings := make(map[string]bool)
	// The table requires group reduce.
	cfg := table.SortConfig{}
	aaps := []table.AliasAccPair{}
	for _, prj := range p.stm.Projections() {
		trace(p.tracer, func() []string {
			return []string{"Analysing projection " + prj.String()}
		})
		// Only include used incoming bindings.
		tmpBindings = append(tmpBindings, prj.Binding)
		// Update sorting configuration.
		found := false
		for _, g := range p.stm.GroupByBindings() {
			if prj.Binding == g {
				found = true
			}
		}
		if found && !mapBindings[prj.Binding] {
			cfg = append(cfg, table.SortConfig{{Binding: prj.Binding}}...)
			mapBindings[prj.Binding] = true
		}
		aap := table.AliasAccPair{
			InAlias: prj.Binding,
		}
		if prj.Alias == "" {
			aap.OutAlias = prj.Binding
		} else {
			aap.OutAlias = prj.Alias
		}
		// Update accumulators.
		switch prj.OP {
		case lexer.ItemCount:
			if prj.Modifier == lexer.ItemDistinct {
				aap.Acc = table.NewCountDistinctAccumulator()
			} else {
				aap.Acc = table.NewCountAccumulator()
			}
		case lexer.ItemSum:
			cell := p.tbl.Rows()[0][prj.Binding]
			if cell.L == nil {
				return fmt.Errorf("cannot only sum int64 and float64 literals; found %s instead for binding %q", cell, prj.Binding)
			}
			switch cell.L.Type() {
			case literal.Int64:
				aap.Acc = table.NewSumInt64LiteralAccumulator(0)
			case literal.Float64:
				aap.Acc = table.NewSumFloat64LiteralAccumulator(0)
			default:
				return fmt.Errorf("cannot only sum int64 and float64 literals; found literal type %s instead for binding %q", cell.L.Type(), prj.Binding)
			}
		}
		aaps = append(aaps, aap)
	}
	trace(p.tracer, func() []string {
		return []string{fmt.Sprintf("Projecting %v", tmpBindings)}
	})
	if err := p.tbl.ProjectBindings(tmpBindings); err != nil {
		return err
	}
	trace(p.tracer, func() []string {
		return []string{"Reducing the table using configuration " + cfg.String()}
	})
	p.tbl.Reduce(cfg, aaps)
	return nil
}

// orderBy takes the resulting table and sorts its contents according to the
// specifications of the ORDER BY clause.
func (p *queryPlan) orderBy() {
	order := p.stm.OrderByConfig()
	if len(order) <= 0 {
		return
	}
	trace(p.tracer, func() []string {
		return []string{"Ordering by " + order.String()}
	})
	p.tbl.Sort(order)
}

// having runs the filtering based on the having clause if needed.
func (p *queryPlan) having() error {
	if p.stm.HasHavingClause() {
		trace(p.tracer, func() []string {
			return []string{"Having filtering"}
		})
		eval := p.stm.HavingEvaluator()
		ok := true
		var eErr error
		p.tbl.Filter(func(r table.Row) bool {
			b, err := eval.Evaluate(r)
			if err != nil {
				ok, eErr = false, err
			}
			return !b
		})
		if !ok {
			return eErr
		}
	}
	return nil
}

// limit truncates the table if the limit clause if available.
func (p *queryPlan) limit() {
	if p.stm.IsLimitSet() {
		trace(p.tracer, func() []string {
			return []string{"Limit results to " + strconv.Itoa(int(p.stm.Limit()))}
		})
		p.tbl.Limit(p.stm.Limit())
	}
}

// Execute queries the indicated graphs.
func (p *queryPlan) Execute(ctx context.Context) (*table.Table, error) {
	// Fetch and catch graph instances.
	trace(p.tracer, func() []string {
		return []string{fmt.Sprintf("Caching graph instances for graphs %v", p.stm.GraphNames())}
	})
	if err := p.stm.Init(ctx, p.store); err != nil {
		return nil, err
	}
	p.grfs = p.stm.Graphs()
	// Retrieve the data.
	lo := p.stm.GlobalLookupOptions()
	trace(p.tracer, func() []string {
		return []string{"Setting global lookup options to " + lo.String()}
	})
	if err := p.processGraphPattern(ctx, lo); err != nil {
		return nil, err
	}
	if err := p.projectAndGroupBy(); err != nil {
		return nil, err
	}
	p.orderBy()
	err := p.having()
	if err != nil {
		return nil, err
	}
	p.limit()
	if p.tbl.NumRows() == 0 {
		// Correct the bindings.
		t, err := table.New(p.stm.OutputBindings())
		if err != nil {
			return nil, err
		}
		p.tbl = t
	}
	return p.tbl, nil
}

// String returns a readable description of the execution plan.
func (p *queryPlan) String() string {
	b := bytes.NewBufferString("QUERY plan:\n\n")
	b.WriteString("using store(\"")
	b.WriteString(p.store.Name(nil))
	b.WriteString(fmt.Sprintf("\") graphs %v\nresolve\n", p.grfsNames))
	for _, c := range p.cls {
		b.WriteString("\t")
		b.WriteString(c.String())
		b.WriteString("\n")
	}
	b.WriteString("project results using\n")
	for _, p := range p.stm.Projection() {
		b.WriteString("\t")
		b.WriteString(p.String())
		b.WriteString("\n")
	}
	if gb := p.stm.GroupBy(); gb != nil {
		b.WriteString("group results using\n")
		for _, g := range gb {
			b.WriteString("\t")
			b.WriteString(g)
			b.WriteString("\n")
		}
	}
	if ob := p.stm.OrderBy(); ob != nil {
		b.WriteString("order results by ")
		b.WriteString(ob.String())
		b.WriteString("\n")
	}
	if hv := p.stm.HavingExpression(); hv != nil {
		b.WriteString("having projected values\n")
		for _, h := range hv {
			b.WriteString("\t")
			b.WriteString(h.Token().String())
			b.WriteString("\n")
		}
	}
	if p.stm.HasLimit() {
		b.WriteString("limit results to ")
		b.WriteString(fmt.Sprintf("%d", p.stm.Limit()))
		b.WriteString(" rows\n")
	}
	return b.String()
}

// New create a new executable plan given a semantic BQL statement.
func New(ctx context.Context, store storage.Store, stm *semantic.Statement, chanSize int, w io.Writer) (Executor, error) {
	switch stm.Type() {
	case semantic.Query:
		return newQueryPlan(ctx, store, stm, chanSize, w)
	case semantic.Insert:
		return &insertPlan{
			stm:    stm,
			store:  store,
			tracer: w,
		}, nil
	case semantic.Delete:
		return &deletePlan{
			stm:    stm,
			store:  store,
			tracer: w,
		}, nil
	case semantic.Create:
		return &createPlan{
			stm:    stm,
			store:  store,
			tracer: w,
		}, nil
	case semantic.Drop:
		return &dropPlan{
			stm:    stm,
			store:  store,
			tracer: w,
		}, nil
	default:
		return nil, fmt.Errorf("planner.New: unknown statement type in statement %v", stm)
	}
}
