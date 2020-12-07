# BQL: BadWolf Query Language

BadWolf provides a high level declarative query and update language. BQL
(or BadWolf Query Language) is a declarative language loosely modeled after
[SPARQL](https://en.wikipedia.org/wiki/SPARQL) to fit the temporal nature of
BadWolf graph data.

## BQL Grammar Organization

The BQL grammar is expressed as a LL1 and implemented using a recursively
descent parser. The grammar can be found in the
[grammar file](../bql/grammar/grammar.go).
The initial version of the grammar is available, as well as the lexical and
syntactical parser.

## Supported statements

BQL currently supports eight statements for data querying and manipulation in
graphs:

* _Create_: Creates a new graph in the store you are connected to.
* _Drop_: Drops an existing graph in the store you are connected to.
* _Shows_: Shows the list of available graphs.
* _Select_: Allows querying data from one or more graphs.
* _Insert_: Allows inserting data into one or more graphs.
* _Delete_: Allows deleting data from one or more graphs.
* _Construct_: Allows creating new statements into graphs by querying existing statements.
* _Deconstruct_: Allows removing statements from graphs by querying existing statements.

Currently _insert_ and _delete_ operations require you to explicitly state
the fully qualified triple. In its current form it is not intended to deal with
large data manipulation. Also, they do not allow to use queries as sources of
the triples to insert or delete.

## Creating a New Graph

All data in BadWolf is stored in graphs. Graphs need to be explicitly created.
The `CREATE` graph statement allows you to create a graph as shown below.

```
  CREATE GRAPH ?a;
```

The name of the graph is represented by a non interpreted binding (more on
this will be discussed in the next section). Hence, in the previous example
the statement would create a graph named `?a`. You can create multiple
graphs in a single statement as shown in the example below:

```
  CREATE GRAPH ?a, ?b, ?c;
```

If you try to create a graph that already exists, it will fail saying that
the graph already exists. You should not expect that creating multiple graphs
will be atomic. If one of the graphs fails, there is no guarantee that others
will have been created, usually failing fast and not even attempting to create
the rest.

## Dropping an Existing Graph

Existing graphs can be dropped via the `DROP` statement. Be *very*
*careful* when dropping graphs. The operation is assumed to be irreversible.
Hence, all data contained in the graph will be lost. You can drop a graph via:

```
  DROP GRAPH ?a;
```

You can also drop multiple graphs at once:

```
  DROP GRAPH ?a, ?b ?c;
```

The same consideration about failures on graph creation apply to dropping
graphs. If you try to drop a graph that does not exist, it will fail saying that
the graph does not exist. You should not expect dropping multiple graphs to be
atomic. If one of the graphs fails, there is no guarantee that others will have
been dropped, usually failing fast and not even attempting to drop the rest.

## Listing all the available graphs

There is a simple way to get a list of all the available graphs in a store.
Just run:

```
  SHOW GRAPHS;
```

This will return the list of graphs currently available in the store.

## Bindings and Graph Patterns

BQL relies on the concept of binding, or a placeholder to represent a value.
Bindings can be read as immutable variables given scoped context. Bindings
start with a `?` and are followed by letters or digits. Some examples of
bindings are: `?foo`, `?bar`, `?id12`.

Once a binding takes a value in a context, it cannot bind to a different
value. Bindings allow the expression of graph matching patterns. The simplest form
of a graph pattern is the fully specified triple:

```
  /user<joe> "color_of_eyes"@[] "brown"^^type:text
```

The above graph pattern would only match triples with the specified subject,
predicate, and object. A peculiarity of the above pattern is that since
the predicate is immutable, the above pattern is the equivalent of checking
if that triple exists on the graph. The equivalent of the above pattern for a
temporal predicate would look like the following:

```
  /user<Joe> "follows"@[2006-01-02T15:04:05.999999999Z07:00] /user<Mary>
```

The above pattern checks if that triple exists and if it is anchored at that
particular time. It is important not to confuse bindings with time ranges
for temporal predicates. A time range is specified as shown below:

```
  /user<Joe> "follows"@[,] /user<Mary>

  /user<Joe> "follows"@[2006-01-02T15:04:05.999999999Z07:00,] /user<Mary>

  /user<Joe> "follows"@[,2006-01-02T15:04:05.999999999Z07:00] /user<Mary>

  /user<Joe> "follows"@[2006-01-01T15:04:05.999999999Z07:00, 2006-01-02T15:04:05.999999999Z07:00] /user<Mary>
```

The first pattern asks if Joe, at any point in time, ever followed Mary. The
second pattern asks if Joe ever followed Mary after a certain date, as opposed
to the third pattern that asks if Joe ever followed Mary before a certain date.
Finally, the fourth pattern asks if Joe followed Mary between two specific dates.

Bindings represent potential values in a given context. For instance:

```
  /user<Joe> "follows"@[,] ?user
```

represents a pattern that matches against all the users that Joe ever followed.
As opposed to:

```
  ?user "follows"@[,] /user<Mary>
```

which represents all the users that ever followed Mary.

You could also ask about all the predicates about Joe related to Mary, we would just write:

```
  /user<Joe> ?p /user<Mary>
```

Where `?p` represents all possible predicates. Bindings become more interesting
when we start building complex graph patterns that contain more than one clause.
Imagine you want to get the list of all users that are grandparents. You could
express such pattern as:

```
  ?grandparent "parent_of"@[] ?x . ?x "parent_of"@[] ?grandchild
```

You can combine multiple graph patterns together using `.` to separate clauses.
The important thing to keep in mind is that the above composite clause
represents a single context. That means that `?x` is a binding that, once
instantiated, cannot change the value in that context. Imagine Joe is the
parent of Peter and Peter is the parent of Mary. Once the first part of the
clause is matched against Joe as the parent of Peter, `?grandparent` gets
bound against Joe and `?x` against Peter. To satisfy the second part of
the composite clause, we now need to find triples where the subject is Peter
(remember that once the value is bound in a context it cannot change) and the
predicate is "parent of". If one exists, then `?grandchild` would get bound
and take the value of Mary.

As we will see in later examples, bindings can also be used to identify
nodes, literals, predicates, or time anchors.

## Querying Data from graphs

Querying data in BQL is done via the `SELECT` statement. The simple form
of a query is expressed as follows:

```
  SELECT ?grandchild
  FROM ?family_tree
  WHERE {
    /user<Joe> "parent_of"@[] ?x . ?x "parent_of"@[] ?grandchild
  };
```

The above query would return all the grandchildren of Joe. BQL uses binding
notation to identify a graph to use. It uses the `?` to indicate the name
of the graph. In the above example that query would be run against a graph
whose ID is equal to `?family_tree`.

You can also query against multiple graphs:

```
  SELECT ?grandchild
  FROM ?family_tree, ?other_family_tree
  WHERE {
    /user<Joe> "parent_of"@[] ?x . ?x "parent_of"@[] ?grandchild
  };
```

There is no limit on how many variables you may return. You can return multiple
variables instead as shown below:

```
  SELECT ?grandparent, ?grandchild
  FROM ?family_tree
  WHERE {
    ?grandparent "parent_of"@[] ?x . ?x "parent_of"@[] ?grandchild
  };
```

The above query would return all grandparents together with their
grandchildren, one pair per row. Also, note that you can have multiple clauses in
the graph pattern inside `WHERE`, separated by `.`, and that the `.` after the last
clause is optional.

### Bindings extraction with keywords `ID`, `TYPE` and `AT`

Note that you could also extract just the names (only "Joe" instead of the entire `/user<Joe>`, for
example). For that you can use the `ID` keyword as follows:

```
  SELECT ?grandparent_name
  FROM ?family_tree
  WHERE {
    ?grandparent ID ?grandparent_name "parent_of"@[] ?x . ?x "parent_of"@[] ?grandchild
  };
```

Which would extract only the names of all grandparents in `family_tree`. 

For subjects and objects (when they are nodes) you can use the keywords `ID` and `TYPE` just like above.
For predicates, you can use the `ID` and `AT` keywords for extracting predicate IDs and time anchors,
respectively, as in:

```
  SELECT ?user, ?pred_id, ?pred_time
  FROM ?social_graph
  WHERE {
    ?user ?pred ID ?pred_id AT ?pred_time /user<Mary>
  };
```

### Aliases with `AS` keyword

In some cases it is useful to return a different
name for the variables, and not use the binding name used in the graph pattern
directly. This is achieved using the `AS` keyword as shown below:

```
  SELECT ?grandparent AS ?gp, ?grandchild AS ?gc
  FROM ?family_tree
  WHERE {
    ?grandparent "parent_of"@[] ?x . ?x "parent_of"@[] ?grandchild
  };
```

It is important to note that aliases defined outside the graph pattern scope, such as the one above,
cannot be used in graph patterns. You can also define aliases directly inside the graph pattern if
you want, even for values (if you also want them to be shown in the query result, for example), as in:

```
  SELECT ?parent, ?specified_child
  FROM ?family_tree
  WHERE {
    ?parent "parent_of"@[] /user<Bob> AS ?specified_child
  };
```

### Grouping and Aggregation

BQL supports basic grouping and aggregation. It is accomplished via
`group by`. The above query may return duplicates depending on the data
available on the graph. If we want to get rid of the duplicates we could just
group them as follows:

```
  SELECT ?grandparent AS ?gp, ?grandchild AS ?gc
  FROM ?family_tree
  WHERE {
    ?grandparent "parent_of"@[] ?x . ?x "parent_of"@[] ?grandchild
  }
  GROUP BY ?gp, ?gc;
```

As you may have expected, you can group by multiple bindings or aliases. Also,
grouping allows a small subset of aggregates. Those include `count`, its
variant with `distinct`, and `sum`. Other functions will be added as needed.
The queries below illustrate how these simple aggregations can be used:

```
  SELECT ?grandparent AS ?gp, count(?grandchild) AS ?gc
  FROM ?family_tree
  WHERE {
    ?grandparent "parent_of"@[] ?x . ?x "parent_of"@[] ?grandchild
  }
  GROUP BY ?gp;
```

Which would return the number of grandchildren per grandparent. However, it would
be better if the distinct version was used to guarantee that all duplicates
resulting from the graph data are removed. The query below illustrates how
the `distinct` variant works:

```
  SELECT ?grandparent AS ?gp, count(distinct ?grandchild) AS ?gc
  FROM ?family_tree
  WHERE {
    ?grandparent "parent_of"@[] ?x . ?x "parent_of"@[] ?grandchild
  }
  GROUP BY ?gp;
```

The sum aggregation only works if the binding is done against a literal of type
`int64` or `float64`, as shown on the example below:

```
  SELECT sum(?capacity) AS ?total_capacity
  FROM ?gas_tanks
  WHERE {
    ?tank "capacity"@[] ?capacity
  };
```

You can also use `sum` to do partial accumulations in the same manner as it was
done in the `count` examples above.

### Sorting query results

Results of the query can be sorted. By default, it is sorted in ascending
order based on the provided variables. The example below orders first by
grandparent name ascending (implicit direction), and for each equal values,
descending based on the grandchild name.

```
  SELECT ?grandparent, ?grandchild
  FROM ?family_tree
  WHERE {
    ?grandparent "parent_of"@[] ?x . ?x "parent_of"@[] ?grandchild
  }
  ORDER BY ?grandparent, ?grandchild DESC;
```

### `HAVING` clause

The `having` modifier allows us to refine the result data further, after it
was already returned from the storage/driver level. For instance, the query
below would only return tanks with a capacity bigger than 10.

```
  SELECT ?tank, ?capacity
  FROM ?gas_tanks
  WHERE {
    ?tank "capacity"@[] ?capacity
  }
  HAVING ?capacity > "10"^^type:int64;
```

Within this `having` clause, you can also build more complicated boolean expressions 
using operators such as `AND` or `OR`:

```
  SELECT ?tank, ?capacity
  FROM ?gas_tanks
  WHERE {
    ?tank "capacity"@[] ?capacity
  }
  HAVING (?capacity > "10"^^type:int64) AND (?capacity < "20"^^type:int64);
```

Also, inside the `having` clause you can compare `TYPE` and `ID` bindings with text literals.
In this case, the comparison will be done lexicographically as in:

```
  SELECT ?parent_name
  FROM ?family_tree
  WHERE {
    ?parent ID ?parent_name "parent_of"@[] ?child
  }
  HAVING ?parent_name < "mary"^^type:text;
```

Last, but not least, the `having` clause supports timestamp comparisons too, such as comparisons
between `AT` bindings and time literals. This is better detailed and exemplified in the section "Specifying
time bounds" below.

Remember that you can also compare one binding with another inside the `having` clause, but they
must be comparable for that: you can compare a `text` binding only with another `text` binding, an `int64`
binding only with another `int64` binding, and so on.

### `LIMIT` keyword

You could also limit the amount of data you will get back by simply appending
a limit to the number of rows to be returned.

```
  SELECT ?tank, ?capacity
  FROM ?gas_tanks
  WHERE {
    ?tank "capacity"@[] ?capacity
  }
  HAVING ?capacity > "10"^^type:int64
  LIMIT "20"^^type:int64;
```

The above query would return at most only 20 rows.

### Specifying time bounds

BQL also provides syntactic sugar to make it easy to specify time bounds. Imagine
you want to get all users who followed Joe and also followed Mary after a
certain date. You could write it as:

```
  SELECT ?user
  FROM ?social_graph
  WHERE {
    ?user "follows"@[2006-01-01T15:04:05.999999999Z07:00,] /user<Joe> .
    ?user "follows"@[2006-01-01T15:04:05.999999999Z07:00,] /user<Mary>
  };
```

You can also imagine that this can become tedious fast if your graph pattern
contains multiple clauses. BQL allows you to specify it in a more compact and
readable form using composable `before`, `after`, and `between`
keywords. They can be composed together using `not`, `and`, and
`or` operators.

```
  SELECT ?user
  FROM ?social_graph
  WHERE {
    ?user "follows"@[,] /user<Joe> .
    ?user "follows"@[,] /user<Mary>
  }
  AFTER 2006-01-01T15:04:05.999999999Z07:00;
```

This is easier to read. It also allows expressing complex global time bounds
that would require multiple clauses and extra bindings:

```
  SELECT ?user
  FROM ?social_graph
  WHERE {
    ?user "follows"@[,] /user<Joe> .
    ?user "follows"@[,] /user<Mary>
  }
  AFTER 2006-01-01T15:04:05.999999999Z07:00 OR
  BETWEEN 2004-01-01T15:04:05.999999999Z07:00, 2004-03-01T15:04:05.999999999Z07:00;
```

Note that the intervals defined by `before`, `after`, and `between` are always closed (the 
limits of the intervals are included). You can then understand the `after` as a `>=`, the 
`before` as a `<=` and the `between` as a combination of them. Also, note that this syntactic
sugar of using a comma inside the square brackets make sense only inside the `WHERE` clause,
you cannot use it out of the `WHERE` scope as inside a `HAVING` clause for example.

In addition to that, remember that bindings may take time anchor values too. Then, you could
also query for all users that first followed Joe and then followed Mary. Such query would look like:

```
  SELECT ?user, ?tj, ?tm
  FROM ?social_graph
  WHERE {
    ?user "follows"@[?tj] /user<Joe> .
    ?user "follows"@[?tm] /user<Mary>
  }
  HAVING ?tm > ?tj;
```

You can also choose to compare only one of these time anchor bindings with a given
time literal, as in:

```
  SELECT ?user, ?tj, ?tm
  FROM ?social_graph
  WHERE {
    ?user "follows"@[?tj] /user<Joe> .
    ?user "follows"@[?tm] /user<Mary>
  }
  HAVING ?tm > 2014-03-10T00:00:00-08:00;
```

These time comparisons inside the `having` clause also take into account the specific time zones of
the timestamps being compared, as one should expect. With `AT` bindings we can do the same as above since
the value extracted is also a timestamp.

As an additional observation, remember that when using the `before`, `after`, and `between` keywords
the final result may also include immutable triples along the temporal ones. To illustrate, given
the query below:

```
  SELECT ?p, ?o
  FROM ?supermarket
  WHERE {
    /u<peter> ?p ?o
  }
  BETWEEN 2016-02-01T00:00:00-08:00, 2016-03-01T00:00:00-08:00;
```

If you have in your `?supermarket` graph both immutable and temporal triples that have `/u<peter>`
as subject, then you will see those immutable triples in your result too, along with the temporal ones
that fit in the given interval specified by `between`. But, if you do want only the temporal ones in your
result you can make use of a `FILTER` clause with the `isTemporal` function (better explained below).

```
  SELECT ?p, ?o
  FROM ?supermarket
  WHERE {
    /u<peter> ?p ?o .
    FILTER isTemporal(?p)
  }
  BETWEEN 2016-02-01T00:00:00-08:00, 2016-03-01T00:00:00-08:00;
```

### `FILTER` clause

The `FILTER` keyword is a tool the user can leverage to improve query performance, being able to communicate
directly with the storage/driver level to specify further the data they want to retrieve. This way, the user
can avoid the search, retrieval and manipulation of heavy chunks of data that will not be used later, which would
add an unnecessary overhead to the query processing.

Note here the difference with the `HAVING` clause: the `HAVING` is mainly thought to work with conditions over
aggregated data (such as involving `sum` and `count`), being processed after all the data was already returned
from the driver level, while `FILTER` works directly at the moment of data retrieval in the driver, with instructions
to customize and specify the data to return (thus improving performance).

The `FILTER` clauses can be used as the last clauses inside `WHERE`, after the graph pattern was already specified.
To illustrate:

```
  SELECT ?p1, ?p2
  FROM ?supermarket
  WHERE {
    /u<peter> ?p1 ?o1 .
    /item/book<Sophie's World> ?p2 ?o2 .
    FILTER latest(?p1) .
    FILTER latest(?p2)
  };
```

On which the `latest` `FILTER` function will allow only the triples with the latest timestamp for the specified
predicate bindings to be returned by the driver (a common use case for time series in BadWolf).

Regarding trailing dots, the `FILTER` clauses are seen just like any other clauses inside `WHERE`. Remember that for
these clauses the trailing dot is mandatory at the end of each clause with the exception of the last one, for which
the dot is optional (as recommended by W3C).

At the moment, BadWolf already supports the `isTemporal`, `isImmutable` and `latest` `FILTER` functions, with an example
of their driver implementation for the volatile driver in `memory.go`. These functions can be applied to predicate bindings
and object bindings as well (being effective when they wrap predicates in a reification scenario), working for aliases too.

To add support for a new `FILTER` function in BadWolf, the instructions to follow step by step are detailed [here](./support_new_filter_function.md).

### More on graph pattern enforcement

A point worth clarifying is that the graph pattern specified inside the `WHERE` clause is a strong
constraint on the data to be returned, and must be followed in its entirety. To illustrate, given the
query below:

```
  SELECT ?s, ?p, ?o, ?o_type
  FROM ?supermarket
  WHERE {
    ?s ?p ?o TYPE ?o_type
  };
```

We have that the `TYPE` keyword is being used to refer to the object of the graph pattern. This will force that
this object must be a node. In other words, all the triples in the `?supermarket` graph for which the object is either
a literal or a predicate (in the case of reification) will be discarded and not shown in this query result. The
`TYPE` keyword is, then, part of the graph pattern to be matched.

In a similar way, if we had:

```
  SELECT ?s, ?p, ?o, ?o_time
  FROM ?supermarket
  WHERE {
    ?s ?p ?o AT ?o_time
  };
```

All the triples from `?supermarket` whose object is not a temporal predicate (reification) will be discarded and not shown
in the query result (the `AT` keyword is part of the graph pattern, in the position above it forces the object `?o` to have a
time anchor to be extracted to the binding `?o_time`).

### `OPTIONAL` clause

Given what is said in the section above, the graph pattern is rigid and must be followed. But, there are cases on which we want
to specify a given graph pattern with some clauses and bindings that may or may not be resolved, a graph pattern with "optional"
parts. For that, we can make use of the `OPTIONAL` keyword. To illustrate, we can take the example of the section above and do:

```
  SELECT ?s, ?p, ?o, ?o_type
  FROM ?supermarket
  WHERE {
    ?s ?p ?o .
    OPTIONAL { ?s ?p ?o TYPE ?o_type }
  };
```

Here we are querying for all triples `?s ?p ?o` from `?supermarket` and marking the `?o_type` binding as optional. This way, in
the case this binding is not resolved for a given triple, when its object `?o` is a literal for example, the triple will not be
discarded as before, it will still appear in the query result having its `?o_type` binding marked as `<NULL>` there.

### More BQL examples

For other useful BQL query examples, please refer to [BadWolf Query Language practical examples](./bql_practical_examples.md).

## Inserting data into graphs

Triples can be inserted into one or more graphs. This can be achieved by
running the following insert statements:

```
  INSERT DATA INTO ?family_tree, ?other_family_tree {
    /user<Joe>   "parent_of"@[] /user<Peter> .
    /user<Peter> "parent_of"@[] /user<Mary>
  };
```

You should not assume that the insert operation will be atomic. Most of the
driver implementations may provide such property, but you will have to check
with the driver implementation.

## Deleting data from graphs

Triples can be deleted from one or more graphs. That can be achieved by just
running the following delete statements:

```
  DELETE DATA FROM ?family_tree, ?other_family_tree {
    /user<Joe>   "parent_of"@[] /user<Peter> .
    /user<Peter> "parent_of"@[] /user<Mary>
  };
```

You should not assume that the delete operation will be atomic. Most of the
driver implementations may provide such property, but you will have to check
with the driver implementation too.

## Building new facts out of existing facts in graphs

In some cases you want to create new facts -- insert new triples -- into a graph or
graphs based on already existing facts. Constructing new facts requires
two steps: (1) querying for the information that is going to be used to
create new facts, and (2) how those new facts are going to be created. An
illustrative example is:

```
  CONSTRUCT {
    ?s ?p ?o
  }
  INTO ?dest
  FROM ?src
  WHERE {
    ?s ?p ?o
  };
```

The above statement would copy all facts from graph `?src` to graph `?dest`.
The `WHERE` clause will bind to all available predicates. Each binding will
then be used in the construct part to create the new triples.

A more elaborate example would be to create a new fact `grandparent` into
the destination graph by properly extracting graph patterns via the `WHERE`
clause.

```
  CONSTRUCT {
    ?ancestor "grandparent"@[] ?grandchildren
  }
  INTO ?dest
  FROM ?src
  WHERE {
    ?ancestor "parent"@[] ?c .
    ?c "parent"@[] ?grandchildren
  };
```

The above statement would add into graph `?dest` one triple for each binding
of `?ancestor` and `?grandchildren` found in the `?src` graph.

The `CONSTRUCT` statement supports using multiple graphs for `INTO` and `FROM`
clauses. All bindings from all graphs in the `FROM` clause are going to be
gathered together. The new facts will be inserted into all the graphs
indicated in the `INTO` list.

Also, you can create multiple new facts based on the bindings:

```
  CONSTRUCT {
    ?ancestor "grandparent"@[] ?grandchildren .
    ?ancestor "is_grandparent"@[] "true"^^type:bool
  }
  INTO ?dest1, ?dest2
  FROM ?src1, ?src2
  WHERE {
    ?ancestor "parent"@[] ?c .
    ?c "parent"@[] ?grandchildren
  };
```

Sometimes, inserting individual triples is not enough to express some facts.
Some concepts require reification to be able to be expressed. `CONSTRUCT`
supports reification in two different flavors.

```
  CONSTRUCT {
    ?ancestor "grandparent"@[] ?grandchildren ;
              "both_live_in"@[] ?city
  }
  INTO ?dest1, ?dest2
  FROM ?src1, ?src2
  WHERE {
    ?ancestor "parent"@[] ?c .
    ?c "parent"@[] ?grandchildren .
    ?ancestor "live_in"@[] ?city .
    ?grandchildren "live_in"@[] ?city
  };
```

This query would create only a new reified fact if both family members
live in the exact same city. Please note the `;` syntax. All partial
statements after it will be attached to the reified statement.

Another way to express the same fact is to use the explicit blank node
notation. `CONSTRUCT` supports the following:

```
  CONSTRUCT {
    ?ancestor "grandparent"@[] ?grandchildren .
    _:v "_subject"@[] ?ancestor .
    _:v "_predicate"@[] "grandparent"@[] .
    _:v "_object"@[] ?grandchildren .
    _:v "both_live_in"@[] ?city
  }
  INTO ?dest1, ?dest2
  FROM ?src1, ?src2
  WHERE {
    ?ancestor "parent"@[] ?c .
    ?c "parent"@[] ?grandchildren .
    ?ancestor "live_in"@[] ?city .
    ?grandchildren "live_in"@[] ?city
  };
```

The above query is equivalent to the query above it, but it explicitly does the
reification by using `_:v`, which expresses a unique blank node linking the
reification together. The `CONSTRUCT` clause supports creating an arbitrary
number of blank nodes. The syntax is always the same, they all start with
the prefix `_:` followed by a logical ID. On insertion of each new fact,
BQL guarantees a new unique blank node will be generated by each of them.
Examples of multiple blank nodes generated at once are `_:v0`, `_:v1`, etc.


## Removing complex facts out of existing graphs using existing statements

In some cases you want to remove facts -- remove existing triples -- in an
existing graph or graphs based on facts existing on the same or other graphs.
Deconstructing facts requires two steps: (1) querying for the information that
is going to be used to remove facts, and (2) how the facts to be removed are
going to be assembled. An illustrative example is:

```
  DECONSTRUCT {
    ?s ?p ?o
  }
  IN ?dest
  FROM ?src
  WHERE {
    ?s ?p ?o
  };
```

The above example would remove all immutable triples in the `?dest` graph that
were found in the `?src` graph. As the `CONSTRUCT` statement, the `DECONSTRUCT`
statement supports multiple graphs on the `IN` and `FROM` clauses of the
statement.

Following the example used in the previous section, we could use the
`DECONSTRUCT` to remove `grandparent` facts in a destination graph built from
a source graphs by properly extracting graph patterns via the `WHERE`
clause:

```
  DECONSTRUCT {
    ?ancestor "grandparent"@[] ?grandchildren
  }
  IN ?dest
  FROM ?src
  WHERE {
    ?ancestor "parent"@[] ?c .
    ?c "parent"@[] ?grandchildren
  };
```

The `DECONSTRUCT` statement does not support neither the blank node notation
nor the reification syntax. Those are used to refer to newly created nodes
introduced by the statement, which makes sense on `CONSTRUCT` statements.
However, `DECONSTRUCT` statements already have all the required information
to assemble the triples to remove.
