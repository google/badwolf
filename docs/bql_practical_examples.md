# BQL Practical Examples

You may find a collection of examples on the [examples folder](../examples/bql).
Each example file targets a particular BQL feature. The list shown below enumerates
each of the example files with a brief description.

* [First steps](../examples/bql/example_0.bql): This example shows how to create
  a graph, insert data, query the graph, and drop the graph. The query shows
  basic functionality on how how to express your graph query using graph
  clauses. The example shows how to search for particular patterns and extract
  IDs from graph nodes.

* [Expressing graph patterns and grouping](../examples/bql/example_1.bql): This
  example shows how you can express complex graph queries by properly binding
  graph clauses together, `OPTIONAL` clauses included. You will also find examples
  on how to summary the results via the `GROUP BY` clause.

* [Summarizing and sorting data queries across graphs](../examples/bql/example_2.bql):
   Sometimes you want to query data that is stored across multiple graphs.
   This example shows how you can express queries across multiple graphs, by
   simply listing them in the `FROM` clause, and summarize the results
   accordingly. It also shows how to sort the resulting tables using the
   `ORDER BY` clause. It is important to highlight that the BQL `FROM` clause does
   not express table joins, but the union of the specified graphs.

* [Refining results](../examples/bql/example_3.bql):
   The `HAVING` clause is useful for filtering rows out from the resulting
   table. This file shows some simple examples of how you can easily achieve
   it by simply providing a boolean condition to the `HAVING` clause.

* [Customizing data retrieval](../examples/bql/example_4.bql):
   To customize the data retrieval directly in the storage/driver level you can
   make use of the `FILTER` keyword, passing additional instructions to the driver
   to help it access and return a more fine-grained portion of the data stored, improving
   performance. This file, then, shows some simple examples of queries with `FILTER` clauses,
   with different `FILTER` functions being applied to illustrate how they can be useful.
