# BQL Practical Examples

You may find a collection of examples on the [examples folder](../examples/bql).
Each example file targets a particular BQL feature. The list shown below list
each of the example files with a brief description.

* [First steps](../examples/bql/example_0.bql): This example show how to create
  a graph, insert data, query the graph, and drop the graph. The query shows
  basic functionality on how how to express your graph query using graph
  clauses. The example shows how to search for particular patterns and extract
  IDs from from graph nodes.

* [Expressing graph patterns and grouping](../examples/bql/example_1.bql): This
  example show how you can express complex graph queries by properly binding
  graph clauses together. You will also find examples on how to summary the
  results via the `GROUP BY` clause.

* [Summarizing and sorting data queries across graphs](../examples/bql/example_2.bql):
   Sometimes you want to queries data that is stored across multiple graphs.
   This example shows how you can express queries across multiple graphs, by
   simple listing them in the `FORM` clause, and summarize the results
   accordingly. It also shows how to sort the resulting tables using via the
   `ORDER BY` clause. It is important to highlight that BQL `FROM` clause does
   not express table joins, but the union of the specified graphs.

* [Filtering results](../examples/bql/example_3.bql):
   The HAVING clause is useful for filtering rows out from the resulting results
   table. This example shows some simple examples of how you can easily achieve
   it by simply providing the filtering criteria on the where clause. 
