# Command line tool: bw

`bw` is the main command line tool used to access various functionalities.
The `bw` command line tool is built via the `tools/vcli/bw` package. You
can build the tool by just typing:

```
$ go test github.com/google/badwolf/... && go build github.com/google/badwolf/tools/vcli/bw
```

If the test pass successfully, the `bw` tool will be placed in the current
directory.

## Usage

Once built, you will be able to access the commands available by typing:

```
$ bw help
```

This command will list available options. Also, you can always type

```
$ bw help COMMAND
```

To list the help related to the provided command. There is a set of flags
available for the `bw` tool. To list them just type:

```
$ bw -h
```

> Keep in mind that all flags should be listed before you enter the command
> you want to run. If you attempt to set the flags after the command they
> will be treated as arguments of it and, hence, they will not be 
> properly set, likely introducing unexpected results.

## Command: Version

The version command prints the version of BadWolf being used. Below you can
find an example of the command output:

```
$ bw version
badwolf vCli (0.4.1-dev)
```

## Command: Run

The `run` command allows you to run all the BQL statements contained in a
given file. All lines in the file starting with # will be treated as comments
and will be discarded. An example of a file containing a set of executable
statements can be found at
[examples/bql/example_0.bql](../examples/bql/example_0.bql).
Below you can find the output of using the `run` command against this file.

```
$ bw run examples/bql/example_0.bql
Processing file examples/bql/example_0.bql

Processing statement (1/5):
CREATE GRAPH ?family;

Result:
OK

Processing statement (2/5):
INSERT DATA INTO ?family { /u<joe> "parent_of"@[] /u<mary> . /u<joe> "parent_of"@[] /u<peter> . /u<peter> "parent_of"@[] /u<john> . /u<peter> "parent_of"@[] /u<eve> };

Result:
OK

Processing statement (3/5):
SELECT ?name FROM ?family WHERE { /u<joe> "parent_of"@[] ?offspring ID ?name };

Result:
?name
mary
peter

OK

Processing statement (4/5):
SELECT ?grandchildren_name FROM ?family WHERE { /u<joe> "parent_of"@[] ?offspring . ?offspring "parent_of"@[] ?grandchildren ID ?grandchildren_name };

Result:
?grandchildren_name
john
eve

OK

Processing statement (5/5):
DROP GRAPH ?family;

Result:
OK
```

## Command: Assert

The `assert` command allows you to run all the stories contained in a given
folder. Stories are serialized as JSON files. Each story contains:

1. One or more sources. A source is a graph defined by the triples it contains.
2. One or more assertions. An assertion is a BQL query and the expected outcome.

An example of a simple story with only one assertion could be:

```
  "Name": "Family tree",
  "Sources": [
    {
      "ID": "?family",
      "Facts": [
        "/u<joe> \"parent_of\"@[] /u<mary>",
        "/u<joe> \"parent_of\"@[] /u<peter>",
        "/u<peter> \"parent_of\"@[] /u<john>",
        "/u<peter> \"parent_of\"@[] /u<eve>"
      ]
    }
  ],
  "Assertions": [
    {
      "Requires": "finding all Joe's offspring names",
      "Statement": "
      SELECT ?name
      FROM ?family
      WHERE {
        /u<joe> \"parent_of\"@[] ?offspring ID ?name
      }
      ORDER BY ?name;",
      "WillFail": false,
      "MustReturn": [
        {
          "?name": "mary"
        },
        {
          "?name": "peter"
        }
      ]
    }
  ]
}

```

The `assert` command will collect all story files and run every one of them
collecting the outcome and evaluating each of the assertions. Stories are heavily
used to validate BQL semantic behavior. All compliance stories can be found at
[examples/compliance](../examples/compliance). Compliance stories guarantee that
all backend storage drivers that implement `storage.Store` and `storage.Graph`
provide the exact same behavior. If a driver does not pass the compliance tests
in the aforementioned folder, it will be an indication of a serious bug and
the driver should not be used since it may lead to wrong results.

Below you can find the output of using the `assert` command against the
compliance folder that guarantees BQL returns the expected results.

```
$ bw assert examples/compliance
-------------------------------------------------------------
Processing folder "examples/compliance"...
	Processing file "cmpl_bql_example_1.json"... done.
	Processing file "cmpl_bql_example_3.json"... done.
	Processing file "cmpl_bql_minimal.json"... done.
	Processing file "cmpl_bql_example_0.json"... done.
	Processing file "cmpl_bql_example_2.json"... done.
	Processing file "cmpl_bql_graph_clauses.json"... done.
-------------------------------------------------------------
Evaluating 6 stories... done.
-------------------------------------------------------------
(1/6) Story "Family graph data example 1"...
	requires finding how many female grandchildren does Joe have [Assertion=TRUE]
	requires finding all male grandchildren does Joe have [Assertion=TRUE]
	requires finding the gender distribution of Joe's mammal grandchildren in our family graph? [Assertion=TRUE]
	requires finding how many offsprings Joe's has [Assertion=TRUE]
	requires finding all Joe's offspring names [Assertion=TRUE]
	requires finding all different mammal we know in the family graph [Assertion=TRUE]
	requires finding all genders of the members in the family graph [Assertion=TRUE]
	requires finding all the gender distribution in our family graph [Assertion=TRUE]

(2/6) Story "Family and car graph data example 4"...
	requires finding if any of Joe's grandchildren have the same name of his parent [Assertion=TRUE]
	requires finding who are Joe's grandchildren that do *not* have the same name of his parent [Assertion=TRUE]

(3/6) Story "A simple object manipulation"...
	requires retrieving the type [Assertion=TRUE]
	requires retrieving the id [Assertion=TRUE]
	requires retrieving the object [Assertion=TRUE]

(4/6) Story "Family graph data example 0"...
	requires finding all Joe's offspring name [Assertion=TRUE]
	requires finding all Joe's grandchildren [Assertion=TRUE]

(5/6) Story "Family and car graph data example 2"...
	requires finding how many grandchildren does Joe have [Assertion=TRUE]
	requires finding the different brands of car manufactures do we know [Assertion=TRUE]
	requires finding what cars does Joe's grandchildren drive in descending order? [Assertion=TRUE]
	requires finding any unique owner and manufacture, list the manufacture in descending order, and for each manufacture order the owners in ascending order [Assertion=TRUE]
	requires finding the manufactures in descending order by number of owners [Assertion=TRUE]

(6/6) Story "Family graph clauses traversal"...
	requires Joe has two children despite redundant clause [Assertion=TRUE]
	requires Joe's grandchildren combinations to be returned only if both do not have the same name [Assertion=TRUE]
	requires counting XX two as the number of grand children Joe has [Assertion=TRUE]
	requires Joe has two kids and one is Mary for sure [Assertion=TRUE]
	requires Joe has no two children and bot are children of Mary [Assertion=TRUE]
	requires returning the first Joe's grandchildren of the possible combinations [Assertion=TRUE]
	requires listing all the possible four combination of Joe's children and grandchildren only if the children has kids [Assertion=TRUE]
	requires Joe has at least two children and their names are Eve and John [Assertion=TRUE]
	requires Joe has at least two children and their names are Eve and John and both share the same parent [Assertion=TRUE]
	requires Joe's grandchildren should product four combinations [Assertion=TRUE]
	requires Joe's grandchildren combinations to be returned only if both have the same name [Assertion=TRUE]
	requires listing Joe as the only grandparent [Assertion=TRUE]
	requires no Joe's children gets returned if he does not have one called Zoe [Assertion=TRUE]
	requires Joe has at least two children and their names are Eve and John and both are children of Peter [Assertion=TRUE]
	requires Joe to have two grandchildren [Assertion=TRUE]
	requires four possible combinations of Joe's children [Assertion=TRUE]
	requires Joe's grandchildren combinations to be returned only if the first name is less than the second one [Assertion=TRUE]
	requires listing all the possible four combination of Joe's children and grandchildren [Assertion=TRUE]

-------------------------------------------------------------

done
```

If any of the assertions of a story fails it will be properly indicated, in which case
the obtained result table and the expected one will be both displayed.

## Command: BQL

The `bql` command starts a REPL that allows running BQL commands. The REPL can
provide basic help on usage as shown below. Currently, the REPL has limited
support for terminal input. BQL statements need to be in a single line and there
is currently no support for cursor keys or history of past BQL statements.

```
$ bw bql
Welcome to BadWolf vCli (0.9.1-dev)
Using driver VOLATILE/0.2.vcli. Type quit; to exit.
Session started at 2018-12-09T00:33:15.126414-08:00.
Memoization enabled. Type help; to print help.

bql> help;

help                                                  - prints help for the bw console.
disable memoization                                   - disables partial result memoization on query resolution.
enable memoization                                    - enables partial result memoization of partial query results.
export <graph_names_separated_by_commas> <file_path>  - dumps triples from graphs into a file path.
desc <BQL>                                            - prints the execution plan for a BQL statement.
load <file_path> <graph_names_separated_by_commas>    - load triples into the specified graphs.
run <file_with_bql_statements>                        - runs all the BQL statements in the file.
start tracing [-v verbosity_level] [trace_file]       - starts tracing queries, verbosity levels supported are 1, 2 and 3 (with 3 meaning maximum verbosity).
stop tracing                                          - stops tracing queries.
start profiling [-cpurate samples_per_second]         - starts pprof profiling for queries (customizable CPU sampling rate).
stop profiling                                        - stops pprof profiling for queries.
quit                                                  - quits the console.
```

### Tracing in BadWolf

BadWolf has its own tracer implemented, that can be enabled/disabled with the `start` and `stop` commands detailed above.
The current BadWolf tracer supports 3 levels of verbosity:

- **`1`** for **minimum** verbosity: only the most crucial messages to understand the query processing flow are sent to
the tracing output. Messages informing which BQL query is being executed at each moment are shown here, for example;

- **`2`** for **medium** verbosity: with this level set the user will see all messages from level `1` and also some additional others
that may be useful, such as messages informing the latency on clause processing for each clause inside `WHERE` and also messages
detailing the number of triples returned from each call to the driver.

- **`3`** for **maximum** verbosity: all available tracing messages will be sent to the output, including messages from levels `1` and `2`
and some others that come with additional details regarding the processing of the query.

### Profiling with `pprof`

As shown above, BadWolf also has this integration with [pprof](https://github.com/google/pprof) profiling
directly through the BQL REPL. After it is activated with the `start` command detailed above the user can `run`
BQLs that will be automatically profiled and, by the time of `stop profiling` or `quit`, two files will
appear: `cpuprofile` with the metrics on CPU consumption and `memprofile` with the metrics on memory.

Commands such as `go tool pprof -http=":8081" bw cpuprofile` open in the browser an interface to visualize the
CPU metrics: the user can see the graph of function calls made and the latencies involved (useful to identify
bottlenecks), they can visualize the correspondent [flame graph](https://github.com/google/pprof/issues/166) as
well, and also an ordered list of the heaviest function calls made. Another useful command is `go tool pprof -pdf bw cpuprofile > cpuprofile.pdf`
to generate pdfs from the profiling files. With `memprofile` the previous `go tool pprof` commands work similarly.

## Command: Benchmark

The `benchmark` commands will run a battery of tests to collect timing measures
against the chosen backend. The benchmarks focus on performance of:

1. Adding triples to a graph.
2. Removing triples from a graph.
3. BQL statements to bound backend performance.

All these benchmarks run against synthetic data using two graph generators:

1. _Tree graph generator_: Given an arbitrary branching factor it generates
   the requested number of triples by walking an imaginary tree in depth first
   search. The height of the tree is used to generate a set of triples is
   computed as log(number of triples)/log(branching factor).

2. _Random graph generator_: Given a number of nodes in a graph, this generator
   creates triples by picking two arbitrary nodes and creating a triple that
   relates them together. The sampling of the pair of nodes is done without
   replacement.

These two generators create graphs with very different structural properties.

All benchmarks consist of generating random triple sets using both generators
and using them as the graphs on which to run the operations. Each benchmark is
run 10 times and the average and standard deviation of the time spent to
run the operation is computed. Also, the benchmark runner computes an
approximation of how many triples per second were processed.

Each benchmark battery is run twice, sequentially and concurrently. The goal is
to also measure the impact of concurrent operations on the driver. Currently,
the command does not allow you to choose any of the parameters used.

There is an example below of how to run the benchmarks against the default
in-memory driver.

```
$ bw --driver=VOLATILE benchmark
DISCLAIMER: Running this benchmarks is expensive. Consider using a machine with at least 3G of RAM.

Creating adding non existing tree triples triples benchmark... 6 entries created
Run adding non existing tree triples benchmark sequentially... (26.459679326s) done
Run adding non existing tree triples benchmark concurrently... (16.846427042s) done

Stats for sequentially run adding non existing tree triples benchmark
Add non existing triples - tg branch_factor=0002, size=0000010, reps=10 - 95398.00 triples/sec - 104.824µs/27.906µs
Add non existing triples - tg branch_factor=0002, size=0001000, reps=10 - 73581.89 triples/sec - 13.590301ms/8.285216ms
Add non existing triples - tg branch_factor=0002, size=0100000, reps=10 - 68959.88 triples/sec - 1.450118621s/337.750666ms
Add non existing triples - tg branch_factor=0200, size=0000010, reps=10 - 115712.62 triples/sec - 86.421µs/21.354µs
Add non existing triples - tg branch_factor=0200, size=0001000, reps=10 - 104222.24 triples/sec - 9.594881ms/622.589µs
Add non existing triples - tg branch_factor=0200, size=0100000, reps=10 - 85291.48 triples/sec - 1.172450009s/236.037094ms

Stats for concurrently run adding non existing tree triples benchmark
Add non existing triples - tg branch_factor=0002, size=0000010, reps=10 - 55770.89 triples/sec - 179.305µs/62.85µs
Add non existing triples - tg branch_factor=0002, size=0001000, reps=10 - 53684.74 triples/sec - 18.627267ms/14.838745ms
Add non existing triples - tg branch_factor=0002, size=0100000, reps=10 - 59360.15 triples/sec - 1.684631972s/562.527734ms
Add non existing triples - tg branch_factor=0200, size=0000010, reps=10 - 106498.54 triples/sec - 93.898µs/40.346µs
Add non existing triples - tg branch_factor=0200, size=0001000, reps=10 - 54648.90 triples/sec - 18.298629ms/15.306505ms
Add non existing triples - tg branch_factor=0200, size=0100000, reps=10 - 66660.41 triples/sec - 1.500140787s/440.351089ms

Creating adding non existing graph triples triples benchmark... 6 entries created
Run adding non existing graph triples benchmark sequentially... (23.4303776s) done
Run adding non existing graph triples benchmark concurrently... (14.228067841s) done

Stats for sequentially run adding non existing graph triples benchmark
Add non existing triples - rg nodes=0317, size=0000010, reps=10 - 104793.24 triples/sec - 95.426µs/24.27µs
Add non existing triples - rg nodes=0317, size=0001000, reps=10 - 97213.00 triples/sec - 10.28669ms/6.146141ms
Add non existing triples - rg nodes=0317, size=0100000, reps=10 - 88717.51 triples/sec - 1.127173158s/306.955722ms
Add non existing triples - rg nodes=1000, size=0000010, reps=10 - 111456.63 triples/sec - 89.721µs/25.954µs
Add non existing triples - rg nodes=1000, size=0001000, reps=10 - 63780.22 triples/sec - 15.678841ms/54.988351ms
Add non existing triples - rg nodes=1000, size=0100000, reps=10 - 84055.60 triples/sec - 1.189688669s/219.294249ms

Stats for concurrently run adding non existing graph triples benchmark
Add non existing triples - rg nodes=0317, size=0000010, reps=10 - 46956.95 triples/sec - 212.961µs/141.453µs
Add non existing triples - rg nodes=0317, size=0001000, reps=10 - 99441.44 triples/sec - 10.05617ms/1.512437ms
Add non existing triples - rg nodes=0317, size=0100000, reps=10 - 71095.28 triples/sec - 1.406563151s/361.212636ms
Add non existing triples - rg nodes=1000, size=0000010, reps=10 - 42954.59 triples/sec - 232.804µs/165.604µs
Add non existing triples - rg nodes=1000, size=0001000, reps=10 - 95050.47 triples/sec - 10.520726ms/3.870627ms
Add non existing triples - rg nodes=1000, size=0100000, reps=10 - 70284.21 triples/sec - 1.422794779s/576.112238ms

...
```

## Command: Load

Loads all the triples stored in a file into the provided graphs.
Graph names need to be separated by commas with no whitespaces. Each triple
needs to be placed in a single line. Each triple needs to be formatted so it can be
parsed as indicated in the [documentation](./temporal_graph_modeling.md). 
Please, also feel free to check this [example text file](./presentations/2016/06/21/data/triples.txt)
and some examples of how to use it in this 
[presentation](http://go-talks.appspot.com/github.com/google/badwolf/docs/presentations/2016/06/21/ottawa-graph-meetup.slide#1).
All data in the file will be treated as triples. 
A line starting with # will be treated as a commented line. If the load fails you may 
end up with partially loaded data.

```
$ bw load ./triples.txt ?graph
```

It also supports importing into multiple graphs at once:

```
$ bw load ./triples.txt ?graph1,?graph2,?graph3
```


## Command: Export

Export all the triples from the provided graphs into the provided text file:

```
$ bw export ?graph ./triples.txt
```
As the load command, it supports exporting multiple graphs at once:

```
$ bw export ?graph1,?graph2,?grpah3 ./triples.txt
```

## Command: Server

The ```server``` command starts a simple HTTP endpoint for BQL commands on
the provided port. 

```
$ bw server 1234
```

This will start an enpoint on port ```1234```. You can just access the
endpoint by hitting [http://localhost:1234](http://localhost:1234). 
This will make it simple for you to enter multiple BQL queries.

The endpoint for queries can be accessed at 
[http://localhost:1234/bql](http://localhost:1234/bql) by posting a
form with ```bqlQuery``` parameter. The enpoint returns, in JSON format,
and array of results. Each element of the array contains:

* _query_: The original query passed.
* _msg_: A human readable message. It will contain error information if the
         query failed to execute correctly.
* _table_: If the query was run successfully and a query result was produced, 
           _table_ will contain an array with the output bindings under
		   _bindings_. The table data will be provided as an array of rows
		   under the _rows_ field. Each row entry represents an object where
		   the fields are the binding names and the value is an object containing
		   the cell value. The cell value is an object that may contain 
		   one of the following fields depending on the value type:
		   _string_, _node_, _pred_, _lit_, or _anchor_. All the values
		   are represented using the string format for them each time. Time 
		   anchors are formated following 
		   (RFC3339Nano)[https://godoc.org/time#pkg-constants].

For instance, you can pass the following queries to the endpoint:

```
create graph ?test;

insert data into ?test {
   /foo<id> "knows"@[] /bar<id>.
   /foo<id> "follows"@[] /bar<id>
};

select ?s,?p,?o,?k,?l,?m
from ?test
where {
  ?s ?p ?o.
  ?k ?l ?m
};
```

The endpoint will execute each of the statements in order and return an array
of JSON formatted objects, one for each query. For the above example you should
get the following JSON on a newly started server:

```
[{
	"query": "create graph ?test;",
	"msg": "[OK]",
	"table": {
		"bindings": [],
		"rows": []
	}
}, {
	"query": "insert data into ?test {     /foo<id> \"knows\"@[] /bar<id>.     /foo<id> \"follows\"@[] /bar<id>  };",
	"msg": "[OK]",
	"table": {
		"bindings": [],
		"rows": []
	}
}, {
	"query": "select ?s,?p,?o,?k,?l,?m  from ?test  where {    ?s ?p ?o.    ?k ?l ?m  };",
	"msg": "[OK]",
	"table": {
		"bindings": ["?s", "?p", "?o", "?k", "?l", "?m"],
		"rows": [{
			"?s": {
				"node": "/foo<id>"
			},
			"?p": {
				"pred": "\"knows\"@[]"
			},
			"?o": {
				"node": "/bar<id>"
			},
			"?k": {
				"node": "/foo<id>"
			},
			"?l": {
				"pred": "\"knows\"@[]"
			},
			"?m": {
				"node": "/bar<id>"
			}
		}, {
			"?s": {
				"node": "/foo<id>"
			},
			"?p": {
				"pred": "\"knows\"@[]"
			},
			"?o": {
				"node": "/bar<id>"
			},
			"?k": {
				"node": "/foo<id>"
			},
			"?l": {
				"pred": "\"follows\"@[]"
			},
			"?m": {
				"node": "/bar<id>"
			}
		}, {
			"?s": {
				"node": "/foo<id>"
			},
			"?p": {
				"pred": "\"follows\"@[]"
			},
			"?o": {
				"node": "/bar<id>"
			},
			"?k": {
				"node": "/foo<id>"
			},
			"?l": {
				"pred": "\"knows\"@[]"
			},
			"?m": {
				"node": "/bar<id>"
			}
		}, {
			"?s": {
				"node": "/foo<id>"
			},
			"?p": {
				"pred": "\"follows\"@[]"
			},
			"?o": {
				"node": "/bar<id>"
			},
			"?k": {
				"node": "/foo<id>"
			},
			"?l": {
				"pred": "\"follows\"@[]"
			},
			"?m": {
				"node": "/bar<id>"
			}
		}]
	}
}]
```
