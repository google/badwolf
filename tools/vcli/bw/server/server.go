// Copyright 2017 Google Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtainPathUnescape a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package server provides a simple http endpoint to access for
// BQL manipulation.
package server

import (
	"fmt"
	"html/template"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"golang.org/x/net/context"

	"github.com/google/badwolf/bql/grammar"
	"github.com/google/badwolf/bql/planner"
	"github.com/google/badwolf/bql/semantic"
	"github.com/google/badwolf/bql/table"
	"github.com/google/badwolf/storage"
	"github.com/google/badwolf/tools/vcli/bw/command"
)

// New creates the help command.
func New(store storage.Store, chanSize, bulkSize int) *command.Command {
	cmd := &command.Command{
		UsageLine: "server port",
		Short:     "runs a BQL endoint.",
		Long: `Runs a BQL endpoint with the provided driver. It allows running
all BQL queries and returns a JSON table with the results.`,
	}
	cmd.Run = func(ctx context.Context, args []string) int {
		return runServer(ctx, cmd, args, store, chanSize, bulkSize)
	}
	return cmd
}

// serverConfig wraps the information that defines the server.
type serverConfig struct {
	store    storage.Store
	chanSize int
	bulkSize int
}

// runServer runs the simple BQL endpoint.
func runServer(ctx context.Context, cmd *command.Command, args []string, store storage.Store, chanSize, bulkSize int) int {
	// Check parameters.
	if len(args) < 3 {
		log.Printf("[%v] Missing required port number. ", time.Now())
		cmd.Usage()
		return 2
	}

	// Validate port number.
	p := strings.TrimSpace(args[len(args)-1])
	port, err := strconv.Atoi(p)
	if err != nil {
		log.Printf("[%v] Invalid port number %q; %v\n", time.Now(), p, err)
		return 2
	}

	// Start the server.
	log.Printf("[%v] Starting server at port %d\n", time.Now(), port)
	s := &serverConfig{
		store:    store,
		chanSize: chanSize,
		bulkSize: bulkSize,
	}
	http.HandleFunc("/bql", s.bqlHandler)
	http.HandleFunc("/", defaultHandler)
	if err := http.ListenAndServe(":"+p, nil); err != nil {
		log.Printf("[%v] Failed to start server on port %s; %v", time.Now(), p, err)
		return 2
	}
	return 0
}

// bqlHandler imPathUnescapeplements the handler to server BQL requests.
func (s *serverConfig) bqlHandler(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		w.WriteHeader(http.StatusMethodNotAllowed)
		reportError(w, r, err)
		return
	}
	if r.Method != http.MethodPost {
		reportError(w, r, fmt.Errorf("invalid %s request on %q endpoint. Only POST request are accepted", r.Method, r.URL.Path))
		log.Printf("[%s] Invalid request: %#v\n", time.Now(), r)
		return
	}

	// Run the query.
	var (
		ctx    context.Context
		cancel context.CancelFunc
	)
	timeout, err := time.ParseDuration(r.FormValue("timeout"))
	if err == nil {
		// The request has a timeout, so create a context that is
		// canceled automatically when the timeout expires.
		ctx, cancel = context.WithTimeout(context.Background(), timeout)
	} else {
		ctx, cancel = context.WithCancel(context.Background())
	}
	defer cancel() // Cancel ctx as soon as handleSearch returns.

	var res []*result
	for _, q := range getQueries(r.PostForm["bqlQuery"]) {
		if nq, err := url.QueryUnescape(q); err == nil {
			q = strings.Replace(strings.Replace(nq, "\n", " ", -1), "\r", " ", -1)
		}
		t, err := BQL(ctx, q, s.store, s.chanSize, s.bulkSize)
		r := &result{
			Q: q,
			T: t,
		}
		if err != nil {
			log.Printf("[%s] %q failed; %v", time.Now(), q, err.Error())
			r.Msg = err.Error()
		} else {
			r.Msg = "[OK]"
		}
		res = append(res, r)
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`[`))
	cnt := len(res)
	for _, r := range res {
		w.Write([]byte(`{ "query": "`))
		w.Write([]byte(strings.Replace(r.Q, `"`, `\"`, -1)))
		w.Write([]byte(`", "msg": "`))
		w.Write([]byte(strings.Replace(r.Msg, `"`, `\"`, -1)))
		w.Write([]byte(`", "table": `))
		if r.T == nil {
			w.Write([]byte(`{}`))
		} else {
			r.T.ToJSON(w)
		}
		w.Write([]byte(` }`))
		if cnt > 1 {
			w.Write([]byte(`, `))
		}
		cnt--
	}
	w.Write([]byte(`]`))

}

// result contains a query and its outcome.
type result struct {
	Q   string       `json:"q,omitempty"`
	Msg string       `json:"msg,omitempty"`
	T   *table.Table `json:"table,omitempty"`
}

// getQueries retuns the list of queries found. It will split them if needed.
func getQueries(raw []string) []string {
	var res []string

	for _, q := range raw {
		for _, qs := range strings.Split(q, ";") {
			if nq := strings.TrimSpace(qs); len(nq) > 0 {
				res = append(res, nq+";")
			}
		}
	}

	return res
}

// BQL attempts to execute the provided query against the given store.
func BQL(ctx context.Context, bql string, s storage.Store, chanSize, bulkSize int) (*table.Table, error) {
	p, err := grammar.NewParser(grammar.SemanticBQL())
	if err != nil {
		return nil, fmt.Errorf("[ERROR] Failed to initilize a valid BQL parser")
	}
	stm := &semantic.Statement{}
	if err := p.Parse(grammar.NewLLk(bql, 1), stm); err != nil {
		return nil, fmt.Errorf("[ERROR] Failed to parse BQL statement with error %v", err)
	}
	pln, err := planner.New(ctx, s, stm, chanSize, bulkSize, nil)
	if err != nil {
		return nil, fmt.Errorf("[ERROR] Should have not failed to create a plan using memory.DefaultStorage for statement %v with error %v", stm, err)
	}
	res, err := pln.Execute(ctx)
	if err != nil {
		return nil, fmt.Errorf("[ERROR] Failed to execute BQL statement with error %v", err)
	}
	return res, nil
}

// defaultHandler implements the handler to server BQL requests.
func defaultHandler(w http.ResponseWriter, r *http.Request) {
	if err := defaultEntryTemplate.Execute(w, nil); err != nil {
		reportError(w, r, err)
	}
}

// reportError reports the given error.
func reportError(w http.ResponseWriter, r *http.Request, err error) {
	w.WriteHeader(http.StatusInternalServerError)
	log.Printf("[%s] %v\n", time.Now(), err)
	errorTemplate.Execute(w, err)
}

// Templates

var (
	defaultEntryTemplate = template.Must(template.New("default").Parse(`
	<html>
	<head>
		<title>BadWolf - Simple BQL endpoint</title>
	</head>
	<body>
		<p>BQL Query to run</p>
		
		<form action="/bql" name="bqlQuery" method="POST">
			<textarea cols="86" rows="20" name="bqlQuery"></textarea>
			<br>
			<input type="submit" value="Run">
		</form>		
		
	</body>
	</html>`))

	errorTemplate = template.Must(template.New("error").Parse(`
	<html>
	<head>
		<title>BadWolf - Endpoint error</title>
	</head>
	<body>
		<b>{{.}}</b>
	</body>
	</html>`))
)
