# BQL: BadWolf Query language

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

BQL currently supports three statements for data querying and manipulation in
graphs:

* _Create_: Creates a new graph in the store you are connected to.
* _Drop_: Drops an existing graph in the store you are connected to.
* _Shows_: Shows the list of available graphs.
* _Select_: Allows querying data from one or more graphs.
* _Insert_: Allows inserting data from one or more graphs.
* _Delete_: Allows deleting data from one or more graphs.
* _Construct_: Allows creating new statements into graphs by querying existing statements.
* _Destruct_: Allows removing statements from graphs by querying existing statements.

Currently _insert_ and _delete_ operations require you to explicitly state
the fully qualified triple. In its current form it is not intended to deal with
large data manipulation. Also they do not allow to use queries as sources of
the triples to insert or delete.

## Creating a New Graph

All data in BadWolf is stored in graphs. Graph need to be explicitly created.
The `CREATE` graph statement allows you to create a graph as shown below.

```
CREATE GRAPH ?a;
```

The name of the graph is represented by a non interpreted binding (more on
this will be discussed in the next section.) Hence, in the previous example
the statement would create a graph named `?a`. You can create multiple
graphs in a single statement as shown in the example below.

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
Hence, all data contained in the graph with be lost. You can drop a graph via:

```
DROP GRAPH ?a;
```

Or you can drop multiple graphs at once.

```
DROP GRAPH ?a, ?b ?c;
```

The same consideration about failures on graph creation apply to dropping
graphs. If you try to drop a graph that does not exist, it will fail saying that
the graph does not exist. You should not expect dropping multiple graphs to be
atomic. If one of the graphs fails, there is no guarantee that others will have
been dropped, usually failing fast and not even attempting to drop the rest.

## Listing all the available graphs

There is a simple way to get a list of all the available graph in a store.
Just run:

```
SHOW GRAPHS;
```

This will return the list af available graphs currently available in the
store.

## Bindings and Graph Patterns

BQL relies on the concept of binding, or a placeholder to represent a value.
Bindings can be read as immutable variables given scoped context. Bindings
start with a `?` and are followed by letters or digits. Some examples of
bindings are: `?foo`, `?bar`, `?id12`.

Once a binding takes a value in a context, cannot bind to a different
value. Bindings allow expression of graph matching patterns. The simplest form
of a graph pattern is the fully specified triple.

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
for temporal predicates. A time range is specified as shown below.

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

represents a pattern that matches against all the users that Joe followed ever.
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
  ?grandparent "parent_of"@[] ?x . ?x "parent_of"@[] ?grand_child
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
predicate is parent of. If one exists, then `?grand_child` would get bound
and take the value of Mary.

As we will see in later examples, bindings can also be used to identify
nodes, literals, predicates, or time anchors.

## Querying Data from graphs

Querying data in BQL is done via the `SELECT` statement. The simple form
of a query is expressed as follows:

```
  SELECT ?grand_child
  FROM ?family_tree
  WHERE {
    /user<Joe> "parent_of"@[] ?x . ?x "parent_of"@[] ?grand_child
  };
```

The above query would return all the grandchildren of Joe. BQL uses binding
notation to identify a graph to use. It uses the `?` to indicate the name
of the graph. In the above example that query would be run against a graph
which ID is equal to `?family_tree`.

You can also query against multiple graphs:

```
  SELECT ?grand_child
  FROM ?family_tree, ?other_family_tree
  WHERE {
    /user<Joe> "parent_of"@[] ?x . ?x "parent_of"@[] ?grand_child
  };
```

There is no limit on how many variables you may return. You can return multiple
variables instead as shown below.

```
  SELECT ?grandparent, ?grand_child
  FROM ?family_tree
  WHERE {
    ?grandparent "parent_of"@[] ?x . ?x "parent_of"@[] ?grand_child
  };
```

The above query would return all grandparents together with the name of their
grandchildren, one pair per row. In some cases it is useful to return a different
name for the variables, and not use the biding name used in the graph pattern
directly. This is achieved using the `as` keyword as shown below.

```
  SELECT ?grandparent as ?gp, ?grand_child as ?gc
  FROM ?family_tree
  WHERE {
    ?grandparent "parent_of"@[] ?x . ?x "parent_of"@[] ?grand_child
  };
```

It is important to note that aliases are defined outside the graph pattern scope.
Hence, aliases cannot be used in graph patterns.

BQL supports basic grouping and aggregation. It is accomplished via
`group by`. The above query may return duplicates depending on the data
available on the graph. If we want to get rid of the duplicates we could just
group them as follows.

```
  SELECT ?grandparent as ?gp, ?grand_child as ?gc
  FROM ?family_tree
  WHERE {
    ?grandparent "parent_of"@[] ?x . ?x "parent_of"@[] ?grand_child
  }
  GROUP BY ?gp, ?gc;
```

As you may have expected, you can group by multiple bindings or aliases. Also,
grouping allows a small subset of aggregates. Those include `count` its
variant with `distinct`, and `sum`. Other functions will be added as needed.
The queries below illustrate how these simple aggregations can be used.

```
  SELECT ?grandparent as ?gp, count(?grand_child) as ?gc
  FROM ?family_tree
  WHERE {
    ?grandparent "parent_of"@[] ?x . ?x "parent_of"@[] ?grand_child
  }
  GROUP BY ?gp;
```

Would return the number of grandchildren per grandparent. However, it would
be better if the distinct version was used to guaranteed that all duplicates
resulting on the graph data are removed. The query below illustrates how
the `distinct` variant work.

```
  SELECT ?grandparent as ?gp, count(distinct ?grand_child) as ?gc
  FROM ?family_tree
  WHERE {
    ?grandparent "parent_of"@[] ?x . ?x "parent_of"@[] ?grand_child
  }
  GROUP BY ?gp;
```

The sum aggregation only works if the binding is done against a literal of type
`int64` or `float64`, as shown on the example below.

```
  SELECT sum(?capacity) as ?total_capacity
  FROM ?gas_tanks
  WHERE {
    ?tank "capacity"@[] ?capacity
  };
```

You can also use `sum` to do partial accumulations in the same manner as was
done in the `count` examples above.

Results of the query can be sorted. By default, it is sorted in ascending
order based on the provided variables. The example below orders first by
grandparent name ascending (implicit direction), and for each equal values,
descending based on the grandchild name.

```
  SELECT ?grandparent, ?grand_child
  FROM ?family_tree
  WHERE {
    ?grandparent "parent_of"@[] ?x . ?x "parent_of"@[] ?grand_child
  }
  ORDER BY ?grandparent, ?grand_child DESC;
```

The `having` modifier allows us to filter the returned data further. For
instance, the query below would only return tanks with a capacity bigger
than 10.

```
  SELECT ?tank, ?capacity
  FROM ?gas_tanks
  WHERE {
    ?tank "capacity"@[] ?capacity
  }
  HAVING ?capacity > "10"^^type:int64;
```

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

BQL also provides syntactic sugar to make ease specifying time bounds. Imagine
you want to get all users who followed Joe and also followed Mary after a
certain date. You could write it as:

```
  SELECT ?user
  FROM ?social_graph
  WHERE {
    ?user "folows"@[2006-01-01T15:04:05.999999999Z07:00,] /user<Joe> .
    ?user "folows"@[2006-01-01T15:04:05.999999999Z07:00,] /user<Mary>
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
    ?user "folows"@[,] /user<Joe> .
    ?user "folows"@[,] /user<Mary>
  }
  AFTER 2006-01-01T15:04:05.999999999Z07:00;
```

This is easier to read. It also allow expressing complex global time bounds
that would require multiple clauses and extra bindings.

```
  SELECT ?user
  FROM ?social_graph
  WHERE {
    ?user "folows"@[,] /user<Joe> .
    ?user "folows"@[,] /user<Mary>
  }
  AFTER 2006-01-01T15:04:05.999999999Z07:00 OR
  BETWEEN 2004-01-01T15:04:05.999999999Z07:00, 2004-03-01T15:04:05.999999999Z07:00;
```

Also remember that bindings may take time anchor values so you could also query
for all users that first followed Joe and then followed Mary. Such query would
look like:

```
  SELECT ?user, ?tj, ?tm
  FROM ?social_graph
  WHERE {
    ?user "folows"@[?tj] /user<Joe> .
    ?user "folows"@[?tm] /user<Mary>
  }
  HAVING ?tm > ?tj;
```

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
with the driver implementation.

## Building new facts out of existing facts in graphs

In some cases you want to create new facts--insert new triples---into a graph or
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
    _:v "subject"@[] ?ancestor .
    _:v "predicate"@[] "grandparent"@[] .
    _:v "object"@[] ?grandchildren .
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
reification by using `_:v`, which express a unique blank node linking the
reification together. The `CONSTRUCT` clause supports creating an arbitrary
number of blank nodes. The syntax is always the same, they all start with
the prefix `_:` followed by a logical ID. On insertion of each new fact,
BQL guarantees a new unique blank node will be generated by each of them.
Example of multiple blank nodes generated at once are `_:v0`, `_:v1`, etc.


## Removing complex facts out of existing graphs using existing statements

In some cases you want to create remove facts--remove existing triples---in an
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
were found in the `?src` graph. As the `CONSTRUCT` statement, the `DECONTRUCT`
statement supports multiple graph on the `IN` and `FROM` clauses of the
statement.

Following the example used in the previous section, we could use the
`DECONSTRUCT` to remove `grandparent` facts in a destination graph built from
a source graphs by properly extracting graph patterns via the `WHERE`
clause.

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
