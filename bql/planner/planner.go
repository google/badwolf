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
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/badwolf/bql/lexer"
	"github.com/google/badwolf/bql/planner/filter"
	"github.com/google/badwolf/bql/planner/tracer"
	"github.com/google/badwolf/bql/semantic"
	"github.com/google/badwolf/bql/table"
	"github.com/google/badwolf/storage"
	"github.com/google/badwolf/triple"
	"github.com/google/badwolf/triple/literal"
	"github.com/google/badwolf/triple/predicate"
	"golang.org/x/sync/errgroup"
)

// Executor interface unifies the execution of statements.
type Executor interface {
	// Execute runs the proposed plan for a given statement.
	Execute(ctx context.Context) (*table.Table, error)

	// String returns a readable description of the execution plan.
	String(ctx context.Context) string

	// Type returns the type of plan used by the executor.
	Type() string
}

// createPlan encapsulates the sequence of instructions that need to be
// executed in order to satisfy the execution of a valid create BQL statement.
type createPlan struct {
	stm    *semantic.Statement
	store  storage.Store
	tracer io.Writer
}

// Type returns the type of plan used by the executor.
func (p *createPlan) Type() string {
	return "CREATE"
}

// Execute creates the indicated graphs.
func (p *createPlan) Execute(ctx context.Context) (*table.Table, error) {
	t, err := table.New([]string{})
	if err != nil {
		return nil, err
	}
	errs := []string{}
	for _, gName := range p.stm.GraphNames() {
		gNameCopy := gName // creating a local copy of the loop variable to not pass it by reference to the closure of the lazy tracer.
		tracer.V(2).Trace(p.tracer, func() *tracer.Arguments {
			return &tracer.Arguments{
				Msgs: []string{fmt.Sprintf("Creating new graph %q", gNameCopy)},
			}
		})
		if _, err := p.store.NewGraph(ctx, gName); err != nil {
			errs = append(errs, err.Error())
		}
	}
	if len(errs) > 0 {
		return nil, errors.New(strings.Join(errs, "; "))
	}
	return t, nil
}

// String returns a readable description of the execution plan.
func (p *createPlan) String(ctx context.Context) string {
	return fmt.Sprintf("CREATE plan:\n\nstore(%q).NewGraph(_, %v)", p.store.Name(nil), p.stm.Graphs())
}

// dropPlan encapsulates the sequence of instructions that need to be
// executed in order to satisfy the execution of a valid drop BQL statement.
type dropPlan struct {
	stm    *semantic.Statement
	store  storage.Store
	tracer io.Writer
}

// Type returns the type of plan used by the executor.
func (p *dropPlan) Type() string {
	return "DROP"
}

// Execute drops the indicated graphs.
func (p *dropPlan) Execute(ctx context.Context) (*table.Table, error) {
	t, err := table.New([]string{})
	if err != nil {
		return nil, err
	}
	errs := []string{}
	for _, gName := range p.stm.GraphNames() {
		gNameCopy := gName // creating a local copy of the loop variable to not pass it by reference to the closure of the lazy tracer.
		tracer.V(2).Trace(p.tracer, func() *tracer.Arguments {
			return &tracer.Arguments{
				Msgs: []string{fmt.Sprintf("Deleting graph %q", gNameCopy)},
			}
		})
		if err := p.store.DeleteGraph(ctx, gName); err != nil {
			errs = append(errs, err.Error())
		}
	}
	if len(errs) > 0 {
		return nil, errors.New(strings.Join(errs, "; "))
	}
	return t, nil
}

// String returns a readable description of the execution plan.
func (p *dropPlan) String(ctx context.Context) string {
	return fmt.Sprintf("DROP plan:\n\nstore(%q).DeleteGraph(_, %v)", p.store.Name(nil), p.stm.Graphs())
}

// insertPlan encapsulates the sequence of instructions that need to be
// executed in order to satisfy the execution of a valid insert BQL statement.
type insertPlan struct {
	stm    *semantic.Statement
	store  storage.Store
	tracer io.Writer
}

// Type returns the type of plan used by the executor.
func (p *insertPlan) Type() string {
	return "INSERT"
}

type updater func(storage.Graph, []*triple.Triple) error

func update(ctx context.Context, ts []*triple.Triple, gbs []string, store storage.Store, f updater) error {
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

	for _, graphBinding := range gbs {
		wg.Add(1)
		go func(graph string) {
			defer wg.Done()
			g, err := store.Graph(ctx, graph)
			if err != nil {
				appendError(err)
				return
			}
			err = f(g, ts)
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
	return t, update(ctx, p.stm.Data(), p.stm.OutputGraphNames(), p.store, func(g storage.Graph, d []*triple.Triple) error {
		gID := g.ID(ctx)
		nTrpls := len(d)
		tracer.V(2).Trace(p.tracer, func() *tracer.Arguments {
			return &tracer.Arguments{
				Msgs: []string{fmt.Sprintf("Inserting %d triples to graph %q", nTrpls, gID)},
			}
		})
		return g.AddTriples(ctx, d)
	})
}

// String returns a readable description of the execution plan.
func (p *insertPlan) String(ctx context.Context) string {
	b := bytes.NewBufferString("INSERT plan:\n\n")
	for _, g := range p.stm.OutputGraphs() {
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

// Type returns the type of plan used by the executor.
func (p *deletePlan) Type() string {
	return "DELETE"
}

// Execute deletes the provided data into the indicated graphs.
func (p *deletePlan) Execute(ctx context.Context) (*table.Table, error) {
	t, err := table.New([]string{})
	if err != nil {
		return nil, err
	}
	return t, update(ctx, p.stm.Data(), p.stm.InputGraphNames(), p.store, func(g storage.Graph, d []*triple.Triple) error {
		gID := g.ID(ctx)
		nTrpls := len(d)
		tracer.V(2).Trace(p.tracer, func() *tracer.Arguments {
			return &tracer.Arguments{
				Msgs: []string{fmt.Sprintf("Removing %d triples from graph %q", nTrpls, gID)},
			}
		})
		return g.RemoveTriples(ctx, d)
	})
}

// String returns a readable description of the execution plan.
func (p *deletePlan) String(ctx context.Context) string {
	b := bytes.NewBufferString("DELETE plan:\n\n")
	for _, g := range p.stm.InputGraphs() {
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
	clauses   []*semantic.GraphClause
	filters   []*semantic.FilterClause
	tbl       *table.Table
	chanSize  int
	tracer    io.Writer
}

// Type returns the type of plan used by the executor.
func (p *queryPlan) Type() string {
	return "SELECT"
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
		grfsNames: stm.InputGraphNames(),
		clauses:   stm.GraphPatternClauses(),
		filters:   stm.FilterClauses(),
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
		tracer.V(3).Trace(p.tracer, func() *tracer.Arguments {
			return &tracer.Arguments{
				Msgs: []string{"Clause is fully specified"},
			}
		})
		if cls.Optional && !cls.HasAlias() {
			tracer.V(3).Trace(p.tracer, func() *tracer.Arguments {
				return &tracer.Arguments{
					Msgs: []string{fmt.Sprintf("Processing optional clause of specificity 3: %v", cls)},
				}
			})
			return false, nil
		}
		t, err := triple.New(cls.S, cls.P, cls.O)
		if err != nil {
			return false, err
		}
		b, tbl, err := simpleExist(ctx, p.grfs, cls, t, p.tracer)
		if err != nil {
			return false, err
		}
		if err := p.tbl.AppendTable(tbl); err != nil {
			return b, err
		}
		return b, nil
	}

	exist, total := 0, 0
	var existing []string
	for _, b := range cls.Bindings() {
		total++
		if p.tbl.HasBinding(b) {
			exist++
			existing = append(existing, b)
		}
	}

	if exist == 0 {
		tracer.V(3).Trace(p.tracer, func() *tracer.Arguments {
			return &tracer.Arguments{
				Msgs: []string{fmt.Sprintf("None of the clause binding exist %v/%v", cls.Bindings(), existing)},
			}
		})
		// Data is new.
		stmLimit := int64(0)
		if len(p.stm.GraphPatternClauses()) == 1 && len(p.stm.GroupBy()) == 0 && len(p.stm.HavingExpression()) == 0 {
			stmLimit = p.stm.Limit()
		}
		tbl, err := simpleFetch(ctx, p.grfs, cls, lo, stmLimit, p.chanSize, p.tracer)
		if err != nil {
			return true, err
		}

		if len(p.tbl.Bindings()) > 0 {
			if cls.Optional {
				tracer.V(3).Trace(p.tracer, func() *tracer.Arguments {
					return &tracer.Arguments{
						Msgs: []string{fmt.Sprintf("Processing optional clause of disjoint bindings: %v", cls)},
					}
				})
				return false, p.tbl.LeftOptionalJoin(tbl)
			}
			return false, p.tbl.DotProduct(tbl)
		}
		return false, p.tbl.AppendTable(tbl)
	}

	tracer.V(3).Trace(p.tracer, func() *tracer.Arguments {
		return &tracer.Arguments{
			Msgs: []string{fmt.Sprintf("Some clause binding exist %v/%v", cls.Bindings(), existing)},
		}
	})
	return false, p.specifyClauseWithTable(ctx, cls, lo)
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
	if cls.P == nil && cls.PID != "" && cls.PAnchorBinding != "" {
		v := r[cls.PAnchorBinding]
		if v != nil && v.T != nil {
			p, err := predicate.NewTemporal(cls.PID, *v.T)
			if err != nil {
				return err
			}
			cls.P = p
		}
	}
	if cls.P == nil {
		v := getBoundValueForComponent(r, []string{cls.PBinding, cls.PAlias})
		if v != nil {
			if v.P != nil {
				cls.P = v.P
			}
		}
		nlo, err := updateTimeBoundsForRow(lo, cls, r)
		if err != nil {
			return err
		}
		lo = nlo
	}
	if cls.O == nil && cls.OID != "" && cls.OAnchorBinding != "" {
		v := r[cls.OAnchorBinding]
		if v != nil && v.T != nil {
			p, err := predicate.NewTemporal(cls.OID, *v.T)
			if err != nil {
				return err
			}
			cls.O = triple.NewPredicateObject(p)
		}
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

	tracer.V(3).Trace(p.tracer, func() *tracer.Arguments {
		return &tracer.Arguments{
			Msgs: []string{fmt.Sprintf("Corrected clause: %v", cls)},
		}
	})

	stmLimit := int64(0)
	if len(p.stm.GraphPatternClauses()) == 1 && len(p.stm.GroupBy()) == 0 && len(p.stm.HavingExpression()) == 0 {
		stmLimit = p.stm.Limit()
	}
	tbl, err := simpleFetch(ctx, p.grfs, cls, lo, stmLimit, p.chanSize, p.tracer)
	if err != nil {
		return err
	}

	p.tbl.AddBindings(tbl.Bindings())
	if tbl.NumRows() == 0 && cls.Optional {
		nr := make(table.Row)
		for _, k := range tbl.Bindings() {
			if _, ok := r[k]; !ok {
				nr[k] = &table.Cell{}
			}
		}
		p.tbl.AddRow(table.MergeRows([]table.Row{r, nr}))
		return nil
	}
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
	grp, gCtx := errgroup.WithContext(ctx)
	for _, tmpRow := range rws {
		r := tmpRow
		grp.Go(func() error {
			var tmpCls = *cls
			// The table manipulations are now thread safe.
			return p.addSpecifiedData(gCtx, r, &tmpCls, lo)
		})
	}
	return grp.Wait()
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
	data := p.tbl.Rows()
	p.tbl.Truncate()
	ocls := *cls
	grp, gCtx := errgroup.WithContext(ctx)
	for _, tmp := range data {
		if gCtx.Err() != nil {
			// Fail fast by not processing more record (in case another goroutine alredy failed, just abort)
			break
		}
		r := tmp
		cls := ocls
		grp.Go(func() error {
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
			exist := false
			for _, g := range p.stm.InputGraphs() {
				gID := g.ID(gCtx)
				t, err := triple.New(sbj, prd, obj)
				if err != nil {
					return err
				}
				tracer.V(2).Trace(p.tracer, func() *tracer.Arguments {
					return &tracer.Arguments{
						Msgs: []string{fmt.Sprintf("g.Exist(%v), graph: %s", t, gID)},
					}
				})
				b, err := g.Exist(gCtx, t)
				if err != nil {
					return err
				}
				exist = exist || b
				if exist || gCtx.Err() != nil {
					break
				}
			}
			if exist {
				p.tbl.AddRow(r)
			}
			return nil
		})
	}
	return grp.Wait()
}

// organizeClausesByBinding takes the graph clauses received as input and organize them in a map
// on which the keys are the bindings of these clauses.
func organizeClausesByBinding(clauses []*semantic.GraphClause) map[string][]*semantic.GraphClause {
	clausesByBinding := map[string][]*semantic.GraphClause{}
	for _, cls := range clauses {
		for b := range cls.BindingsMap() {
			clausesByBinding[b] = append(clausesByBinding[b], cls)
		}
	}

	return clausesByBinding
}

// compatibleBindingsInClauseForFilterOperation returns a function that, for each given clause, returns the bindings that are
// compatible with the specified filter operation.
func compatibleBindingsInClauseForFilterOperation(operation filter.Operation) (compatibleBindingsInClause func(cls *semantic.GraphClause) (bindingsByField map[filter.Field]map[string]bool), err error) {
	switch operation {
	case filter.Latest:
		compatibleBindingsInClause = func(cls *semantic.GraphClause) (bindingsByField map[filter.Field]map[string]bool) {
			bindingsByField = map[filter.Field]map[string]bool{
				filter.PredicateField: {cls.PBinding: true, cls.PAlias: true},
				filter.ObjectField:    {cls.OBinding: true, cls.OAlias: true},
			}
			return bindingsByField
		}
		return compatibleBindingsInClause, nil
	case filter.IsImmutable:
		compatibleBindingsInClause = func(cls *semantic.GraphClause) (bindingsByField map[filter.Field]map[string]bool) {
			bindingsByField = map[filter.Field]map[string]bool{
				filter.PredicateField: {cls.PBinding: true, cls.PAlias: true},
				filter.ObjectField:    {cls.OBinding: true, cls.OAlias: true},
			}
			return bindingsByField
		}
		return compatibleBindingsInClause, nil
	case filter.IsTemporal:
		compatibleBindingsInClause = func(cls *semantic.GraphClause) (bindingsByField map[filter.Field]map[string]bool) {
			bindingsByField = map[filter.Field]map[string]bool{
				filter.PredicateField: {cls.PBinding: true, cls.PAlias: true},
				filter.ObjectField:    {cls.OBinding: true, cls.OAlias: true},
			}
			return bindingsByField
		}
		return compatibleBindingsInClause, nil
	default:
		return nil, fmt.Errorf("filter function %q has no bindings in clause specified for it (planner level)", operation)
	}
}

// organizeFilterOptionsByClause processes all the given filters and organize them in a map that has as keys the
// clauses to which they must be applied.
func organizeFilterOptionsByClause(filters []*semantic.FilterClause, clauses []*semantic.GraphClause) (map[*semantic.GraphClause]*filter.StorageOptions, error) {
	clausesByBinding := organizeClausesByBinding(clauses)
	filterOptionsByClause := map[*semantic.GraphClause]*filter.StorageOptions{}

	for _, f := range filters {
		if _, ok := clausesByBinding[f.Binding]; !ok {
			return nil, fmt.Errorf("binding %q referenced by filter clause %q does not exist in the graph pattern", f.Binding, f)
		}

		compatibleBindingsInClause, err := compatibleBindingsInClauseForFilterOperation(f.Operation)
		if err != nil {
			return nil, err
		}
		for _, cls := range clausesByBinding[f.Binding] {
			if _, ok := filterOptionsByClause[cls]; ok {
				return nil, fmt.Errorf("multiple filters for the same graph clause or same binding are not supported at the moment")
			}

			compatibleBindingsByField := compatibleBindingsInClause(cls)
			filterBindingIsCompatible := false
			for field, bndgs := range compatibleBindingsByField {
				if bndgs[f.Binding] {
					filterBindingIsCompatible = true
					filterOptionsByClause[cls] = &filter.StorageOptions{
						Operation: f.Operation,
						Field:     field,
						Value:     f.Value,
					}
					break
				}
			}
			if !filterBindingIsCompatible {
				return nil, fmt.Errorf("binding %q occupies a position in graph clause %q that is incompatible with filter function %q", f.Binding, cls, f.Operation)
			}
		}
	}

	return filterOptionsByClause, nil
}

// addFilterOptions adds FilterOptions to lookup options if the given clause has bindings for which
// filters were defined (organized in filterOptionsByClause).
func addFilterOptions(lo *storage.LookupOptions, cls *semantic.GraphClause, filterOptionsByClause map[*semantic.GraphClause]*filter.StorageOptions) {
	if _, ok := filterOptionsByClause[cls]; ok {
		lo.FilterOptions = filterOptionsByClause[cls]
	}
}

// resetFilterOptions resets FilterOptions in lookup options to nil.
func resetFilterOptions(lo *storage.LookupOptions) {
	lo.FilterOptions = (*filter.StorageOptions)(nil)
}

// processGraphPattern process the query graph pattern to retrieve the
// data from the specified graphs.
func (p *queryPlan) processGraphPattern(ctx context.Context, lo *storage.LookupOptions) error {
	clauses := p.clauses
	tracer.V(3).Trace(p.tracer, func() *tracer.Arguments {
		var res []string
		for i, cls := range clauses {
			res = append(res, fmt.Sprintf("Clause %d to process: %v", i, cls))
		}
		return &tracer.Arguments{
			Msgs: res,
		}
	})

	filterOptionsByClause, err := organizeFilterOptionsByClause(p.filters, p.clauses)
	if err != nil {
		return err
	}
	tracer.V(2).Trace(p.tracer, func() *tracer.Arguments {
		return &tracer.Arguments{
			Msgs: []string{fmt.Sprintf("Starting to process clauses")},
		}
	})
	tStartClauses := time.Now()
	for i, cls := range p.clauses {
		iCopy, clsCopy := i, cls // creating local copies of the loop variables to not pass them by reference to the closure of the lazy tracer.
		tracer.V(2).Trace(p.tracer, func() *tracer.Arguments {
			return &tracer.Arguments{
				Msgs: []string{fmt.Sprintf("Starting to process clause %d: %v", iCopy, clsCopy)},
			}
		})

		tStartCurrClause := time.Now()
		addFilterOptions(lo, cls, filterOptionsByClause)
		unresolvable, err := p.processClause(ctx, cls, lo)
		resetFilterOptions(lo)
		tElapsedCurrClause := time.Now().Sub(tStartCurrClause)

		tracer.V(2).Trace(p.tracer, func() *tracer.Arguments {
			return &tracer.Arguments{
				Msgs: []string{fmt.Sprintf("Finished processing clause %d: %v, latency: %v", iCopy, clsCopy, tElapsedCurrClause)},
			}
		})
		if err != nil {
			return err
		}
		if unresolvable {
			p.tbl.Truncate()
			return nil
		}
	}
	tElapsedClauses := time.Now().Sub(tStartClauses)
	tracer.V(2).Trace(p.tracer, func() *tracer.Arguments {
		return &tracer.Arguments{
			Msgs: []string{fmt.Sprintf("Finished processing all clauses, total latency: %v", tElapsedClauses)},
		}
	})

	return nil
}

// projectAndGroupBy takes the resulting table and projects its contents and
// groups it by if needed.
func (p *queryPlan) projectAndGroupBy() error {
	grp := p.stm.GroupByBindings()
	if len(grp) == 0 { // The table only needs to be projected.
		tracer.V(3).Trace(p.tracer, func() *tracer.Arguments {
			return &tracer.Arguments{
				Msgs: []string{fmt.Sprintf("Running projection for %v", grp)},
			}
		})
		p.tbl.AddBindings(p.stm.OutputBindings())
		// For each row, copy each input binding value to its appropriate alias.
		for _, prj := range p.stm.Projections() {
			for _, row := range p.tbl.Rows() {
				row[prj.Alias] = row[prj.Binding]
			}
		}
		outputBindings := p.stm.OutputBindings()
		tracer.V(3).Trace(p.tracer, func() *tracer.Arguments {
			return &tracer.Arguments{
				Msgs: []string{fmt.Sprintf("Output bindings projected %v", outputBindings)},
			}
		})
		return p.tbl.ProjectBindings(outputBindings)
	}
	tracer.V(3).Trace(p.tracer, func() *tracer.Arguments {
		return &tracer.Arguments{
			Msgs: []string{"Starting group reduce and projection"},
		}
	})
	// The table needs to be group reduced.
	// Project only binding involved in the group operation.
	tmpBindings := []string{}
	mapBindings := make(map[string]bool)
	// The table requires group reduce.
	cfg := table.SortConfig{}
	aaps := []table.AliasAccPair{}
	for _, prj := range p.stm.Projections() {
		prjCopy := prj // creating a local copy of the loop variable to not pass it by reference to the closure of the lazy tracer.
		tracer.V(3).Trace(p.tracer, func() *tracer.Arguments {
			return &tracer.Arguments{
				Msgs: []string{fmt.Sprintf("Analysing projection %q", prjCopy)},
			}
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
				return fmt.Errorf("can only sum int64 and float64 literals; found %s instead for binding %q", cell, prj.Binding)
			}
			switch cell.L.Type() {
			case literal.Int64:
				aap.Acc = table.NewSumInt64LiteralAccumulator(0)
			case literal.Float64:
				aap.Acc = table.NewSumFloat64LiteralAccumulator(0)
			default:
				return fmt.Errorf("can only sum int64 and float64 literals; found literal type %s instead for binding %q", cell.L.Type(), prj.Binding)
			}
		}
		aaps = append(aaps, aap)
	}
	tracer.V(3).Trace(p.tracer, func() *tracer.Arguments {
		return &tracer.Arguments{
			Msgs: []string{fmt.Sprintf("Projecting %v", tmpBindings)},
		}
	})
	if err := p.tbl.ProjectBindings(tmpBindings); err != nil {
		return err
	}
	tracer.V(3).Trace(p.tracer, func() *tracer.Arguments {
		return &tracer.Arguments{
			Msgs: []string{"Reducing the table using configuration " + cfg.String()},
		}
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
	tracer.V(2).Trace(p.tracer, func() *tracer.Arguments {
		return &tracer.Arguments{
			Msgs: []string{"Ordering by " + order.String()},
		}
	})
	p.tbl.Sort(order)
}

// having runs the filtering based on the having clause if needed.
func (p *queryPlan) having() error {
	if p.stm.HasHavingClause() {
		tracer.V(2).Trace(p.tracer, func() *tracer.Arguments {
			return &tracer.Arguments{
				Msgs: []string{"Starting to process HAVING clause"},
			}
		})
		eval := p.stm.HavingEvaluator()
		ok := true
		var eErr error
		nRowsRemoved := p.tbl.Filter(func(r table.Row) bool {
			b, err := eval.Evaluate(r)
			if err != nil {
				ok, eErr = false, err
			}
			return !b
		})
		if !ok {
			tracer.V(2).Trace(p.tracer, func() *tracer.Arguments {
				return &tracer.Arguments{
					Msgs: []string{eErr.Error()},
				}
			})
			return eErr
		}
		tracer.V(2).Trace(p.tracer, func() *tracer.Arguments {
			return &tracer.Arguments{
				Msgs: []string{fmt.Sprintf("Finished processing HAVING clause, %d rows were removed from table", nRowsRemoved)},
			}
		})
	}
	return nil
}

// limit truncates the table if the limit clause if available.
func (p *queryPlan) limit() {
	if p.stm.IsLimitSet() {
		stmLimit := p.stm.Limit()
		tracer.V(2).Trace(p.tracer, func() *tracer.Arguments {
			return &tracer.Arguments{
				Msgs: []string{fmt.Sprintf("Limit results to %s", strconv.Itoa(int(stmLimit)))},
			}
		})
		p.tbl.Limit(stmLimit)
	}
}

// Execute queries the indicated graphs.
func (p *queryPlan) Execute(ctx context.Context) (*table.Table, error) {
	// Fetch and cache graph instances.
	inputGraphNames := p.stm.InputGraphNames()
	tracer.V(3).Trace(p.tracer, func() *tracer.Arguments {
		return &tracer.Arguments{
			Msgs: []string{fmt.Sprintf("Caching graph instances for graphs %v", inputGraphNames)},
		}
	})
	if err := p.stm.Init(ctx, p.store); err != nil {
		return nil, err
	}
	p.grfs = p.stm.InputGraphs()
	// Retrieve the data.
	lo := p.stm.GlobalLookupOptions()
	loStr := lo.String()
	tracer.V(2).Trace(p.tracer, func() *tracer.Arguments {
		return &tracer.Arguments{
			Msgs: []string{fmt.Sprintf("Setting global lookup options to %s", loStr)},
		}
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
func (p *queryPlan) String(ctx context.Context) string {
	b := bytes.NewBufferString("QUERY plan:\n\n")
	b.WriteString("using store(\"")
	b.WriteString(p.store.Name(nil))
	b.WriteString(fmt.Sprintf("\") graphs %v\nresolve\n", p.grfsNames))
	for _, c := range p.clauses {
		b.WriteString("\t")
		b.WriteString(c.String())
		b.WriteString("\n")
	}
	b.WriteString("with filters\n")
	for _, f := range p.filters {
		b.WriteString("\t")
		b.WriteString(f.String())
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

// constructPlan encapsulates the sequence of instructions that need to be
// executed in order to satisfy the execution of a valid construct or deconstruct
// BQL statement.
type constructPlan struct {
	stm       *semantic.Statement
	store     storage.Store
	tracer    io.Writer
	bulkSize  int
	queryPlan *queryPlan
	construct bool
}

// Type returns the type of plan used by the executor.
func (p *constructPlan) Type() string {
	if p.construct {
		return "CONSTRUCT"
	}
	return "DECONSTRUCT"
}

func (p *constructPlan) processPredicateObjectPair(pop *semantic.ConstructPredicateObjectPair, tbl *table.Table, r table.Row) (*predicate.Predicate, *triple.Object, error) {
	var err error
	rprd, robj := pop.P, pop.O
	if rprd == nil {
		if tbl.HasBinding(pop.PBinding) {
			// Try to bind the predicate.
			v, ok := r[pop.PBinding]
			if !ok {
				return nil, nil, fmt.Errorf("row %+v misses binding %q", r, pop.PBinding)
			}
			if v.P == nil {
				return nil, nil, fmt.Errorf("binding %q requires a predicate, got %+v instead", pop.PBinding, v)
			}
			rprd = v.P
		} else if pop.PTemporal && pop.PAnchorBinding != "" {
			// Try to bind the predicate anchor.
			v, ok := r[pop.PAnchorBinding]
			if !ok {
				return nil, nil, fmt.Errorf("row %+v misses binding %q", r, pop.PAnchorBinding)
			}
			if v.T == nil {
				return nil, nil, fmt.Errorf("binding %q requires a time, got %+v instead", pop.PAnchorBinding, v)
			}
			rprd, err = predicate.NewTemporal(pop.PID, *v.T)
			if err != nil {
				return nil, nil, err
			}
		}
	}
	if robj == nil {
		if tbl.HasBinding(pop.OBinding) {
			// Try to bind the object
			v, ok := r[pop.OBinding]
			if !ok {
				return nil, nil, fmt.Errorf("row %+v misses binding %q", r, pop.OBinding)
			}
			co, err := cellToObject(v)
			if err != nil {
				return nil, nil, err
			}
			robj = co
		} else if pop.OTemporal && pop.OAnchorBinding != "" {
			// Try to bind the object anchor.
			v, ok := r[pop.OAnchorBinding]
			if !ok {
				return nil, nil, fmt.Errorf("row %+v misses binding %q", r, pop.OAnchorBinding)
			}
			if v.T == nil {
				return nil, nil, fmt.Errorf("binding %q requires a time, got %+v instead", pop.OAnchorBinding, v)
			}
			rop, err := predicate.NewTemporal(pop.OID, *v.T)
			if err != nil {
				return nil, nil, err
			}
			robj = triple.NewPredicateObject(rop)
		}
	}
	return rprd, robj, nil
}

func (p *constructPlan) processConstructClause(cc *semantic.ConstructClause, tbl *table.Table, r table.Row) (*triple.Triple, error) {
	var err error
	sbj := cc.S
	if sbj == nil && tbl.HasBinding(cc.SBinding) {
		v, ok := r[cc.SBinding]
		if !ok {
			return nil, fmt.Errorf("row %+v misses binding %q", r, cc.SBinding)
		}
		if v.N == nil {
			return nil, fmt.Errorf("binding %q requires a node, got %+v instead", cc.SBinding, v)
		}
		sbj = v.N
	}
	prd, obj, err := p.processPredicateObjectPair(cc.PredicateObjectPairs()[0], tbl, r)
	if err != nil {
		return nil, err
	}
	t, err := triple.New(sbj, prd, obj)
	return t, err
}

func (p *constructPlan) Execute(ctx context.Context) (*table.Table, error) {
	tbl, err := p.queryPlan.Execute(ctx)
	if err != nil {
		return nil, err
	}
	// The buffered channel has capacity to accommodate twice the amount of triples stored in a single call.
	tripChan := make(chan *triple.Triple, 2*p.bulkSize)
	done := make(chan bool)

	go func() {
		var ts []*triple.Triple
		updateFunc := func(g storage.Graph, d []*triple.Triple) error {
			gID := g.ID(ctx)
			nTrpls := len(d)
			tracer.V(2).Trace(p.tracer, func() *tracer.Arguments {
				return &tracer.Arguments{
					Msgs: []string{fmt.Sprintf("Removing %d triples from graph %q", nTrpls, gID)},
				}
			})
			return g.RemoveTriples(ctx, d)
		}
		if p.construct {
			updateFunc = func(g storage.Graph, d []*triple.Triple) error {
				gID := g.ID(ctx)
				nTrpls := len(d)
				tracer.V(2).Trace(p.tracer, func() *tracer.Arguments {
					return &tracer.Arguments{
						Msgs: []string{fmt.Sprintf("Inserting %d triples to graph %q", nTrpls, gID)},
					}
				})
				return g.AddTriples(ctx, d)
			}
		}
		for elem := range tripChan {
			ts = append(ts, elem)
			if len(ts) >= p.bulkSize {
				update(ctx, ts, p.stm.OutputGraphNames(), p.store, updateFunc)
				ts = []*triple.Triple{}
			}
		}
		if len(ts) > 0 {
			update(ctx, ts, p.stm.OutputGraphNames(), p.store, updateFunc)
		}
		done <- true
	}()

	for _, cc := range p.stm.ConstructClauses() {
		for _, r := range tbl.Rows() {
			t, err := p.processConstructClause(cc, tbl, r)
			if err != nil {
				return nil, err
			}
			if len(cc.PredicateObjectPairs()) > 1 {
				// We need to reify a blank node.
				rts, bn, err := t.Reify()
				if err != nil {
					return nil, fmt.Errorf("triple.Reify failed to reify %v with error %v", t, err)
				}
				for _, trpl := range rts[1:] {
					tripChan <- trpl
				}
				for _, pop := range cc.PredicateObjectPairs()[1:] {
					rprd, robj, err := p.processPredicateObjectPair(pop, tbl, r)
					if err != nil {
						return nil, err
					}
					rt, err := triple.New(bn, rprd, robj)
					if err != nil {
						return nil, err
					}
					tripChan <- rt
				}
			} else {
				tripChan <- t
			}
		}
	}
	close(tripChan)
	// Wait until all triples are added to the store.
	<-done
	return tbl, nil
}

// String returns a readable description of the execution plan.
func (p *constructPlan) String(ctx context.Context) string {
	b := bytes.NewBufferString("DECONSTRUCT plan:\n\n")
	if p.construct {
		b = bytes.NewBufferString("CONSTRUCT plan:\n\n")
	}
	b.WriteString("Input graphs:\n")
	for _, gn := range p.stm.InputGraphNames() {
		b.WriteString(fmt.Sprintf("\t%v\n", gn))
	}
	b.WriteString("Output graphs:\n")
	for _, gn := range p.stm.OutputGraphNames() {
		b.WriteString(fmt.Sprintf("\t%v\n", gn))
	}
	b.WriteString("Construct clauses:\n")
	for _, cc := range p.stm.ConstructClauses() {
		b.WriteString(fmt.Sprintf("\t%v\n", cc))
	}
	b.WriteString(fmt.Sprintf("\n%v", p.queryPlan.String(ctx)))
	return b.String()
}

// showPlan creates a plan to show all the graphs available.
type showPlan struct {
	stm    *semantic.Statement
	store  storage.Store
	tracer io.Writer
}

// Type returns the type of plan used by the executor.
func (p *showPlan) Type() string {
	return "SHOW"
}

// Execute the show statement.
func (p *showPlan) Execute(ctx context.Context) (*table.Table, error) {
	t, err := table.New([]string{"?graph_id"})
	if err != nil {
		return nil, err
	}
	errs := make(chan error)
	names := make(chan string)
	go func() {
		errs <- p.store.GraphNames(ctx, names)
		close(errs)
	}()

	for name := range names {
		id := name
		t.AddRow(table.Row{
			"?graph_id": &table.Cell{
				S: &id,
			},
		})
	}
	if <-errs != nil {
		return nil, err
	}
	return t, nil
}

// String returns a readable description of the execution plan.
func (p *showPlan) String(ctx context.Context) string {
	return fmt.Sprintf("SHOW plan:\n\nstore(%q).GraphNames(_, _)", p.store.Name(ctx))
}

// New create a new executable plan given a semantic BQL statement.
func New(ctx context.Context, store storage.Store, stm *semantic.Statement, chanSize, bulkSize int, w io.Writer) (Executor, error) {
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
	case semantic.Construct:
		qp, _ := newQueryPlan(ctx, store, stm, chanSize, w)
		return &constructPlan{
			stm:       stm,
			store:     store,
			tracer:    w,
			bulkSize:  bulkSize,
			queryPlan: qp,
			construct: true,
		}, nil
	case semantic.Deconstruct:
		qp, _ := newQueryPlan(ctx, store, stm, chanSize, w)
		return &constructPlan{
			stm:       stm,
			store:     store,
			tracer:    w,
			bulkSize:  bulkSize,
			queryPlan: qp,
			construct: false,
		}, nil
	case semantic.Show:
		return &showPlan{
			stm:    stm,
			store:  store,
			tracer: w,
		}, nil
	default:
		return nil, fmt.Errorf("planner.New: unknown statement type in statement %v", stm)
	}
}
