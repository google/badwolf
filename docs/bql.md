# BQL: BadWolf Query language

BadWolf provides a high level declarative query and update language. BQL
(or BadWolf Query Language) is a declarative language losely modeled after
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
* _Select_: Allows querying data form one or more graphs.
* _Insert_: Allows inserting data form one or more graphs.
* _Delete_: Allows deleting data form one or more graphs.

Currently _insert_ and _delete_ operations require you to explicitly state
the fully qualified triple. In its current form it is not intended to deal with
large data manipulation. Also they do not allow  use queries as sources of
the triples to insert or delete.

## Creating a New Graph

All data in BadWolf is stored in graphs. Graph need to be explicitly created.
The ```CREATE``` graph statement allows you to create a graph as shown below.

```
CREATE GRAPH ?a;
```

The name of the graph is represented by a non interpreted binding (more on
this will be discussed in the next section.) Hence, on the previous example
the statement would create a graph named ```?a```. You can create multiple
graphs in a single statement as shown in the example below.

```
CREATE GRAPH ?a, ?b, ?c;
```

If you try to create a graph that already exist, it will fail saying that
the graph already exist. You should not expect that creating multiple graphs
will be atomic. If one of the graphs fails, there is no guarantee that others
will have been created, usually failing fast and not even attempting to create
the rest.

## Dropping an Existing Graph

Existing graphs can be dropped via the ```DROP``` statement. Be *very*
*careful* when dropping graphs. The operation is assume to be irreversible.
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
been created, usually failing fast and not even attempting to create the rest.


## Bindings and Graph Patterns

BQL relies on the concept of binding, or a place holder to represent a value.
Bindings can be read as immutable variables given scoped context. Bindings
starting with a '?' and it is followed by letters or digits. Some examples of
bindings are: ```?foo```, ```?bar```, ```?id12```.

Bindings, once they take a value in a context, they cannot bind to a different
value. Bindings allow to express graph matching patterns. The simplest form
of a graph pattern is the fully specified triple.

```
  /user<joe> "color_of_eyes"@[] "brown"^^type:text
```

The above graph pattern would only match triples with the specified subject,
predicated, and object. A peculiarity of the above pattern is that since
the predicate is immutable, the above pattern is the equivalent of checking
if that triple exist on the graph. The equivalent of the above pattern for a
temporal predicate would look like as:

```
  /user<Joe> "follows"@[2006-01-02T15:04:05.999999999Z07:00] /user<Mary>
```

The above pattern checks if that triple exist and it is anchored at that
particular time. It is important not to confuse bindings with time ranges
for temporal predicates. A time range is specified as shown below.

```
  /user<Joe> "follows"@[,] /user<Mary>

  /user<Joe> "follows"@[2006-01-02T15:04:05.999999999Z07:00,] /user<Mary>

  /user<Joe> "follows"@[,2006-01-02T15:04:05.999999999Z07:00] /user<Mary>

  /user<Joe> "follows"@[2006-01-01T15:04:05.999999999Z07:00, 2006-01-02T15:04:05.999999999Z07:00] /user<Mary>
```

The first pattern asks if Joe at any point in time ever followed Mary. The
second pattern asks if Joe ever followed Mary after a certain date, as opposite
to the third pattern that asks if Joe ever followed Mary before a certain date.
Finally, the fourth pattern ask if Joe followed Mary between two specific dates.

Bindings represent potential values in a given context. For instance,

```
  /user<Joe> "follows"@[,] ?user
```

represent a pattern that matches against all the users that Joe followed ever.
As opposed to

```
  ?user "follows"@[,] /user<Mary>
```

which represents all the users that ever followed Mary. You could also ask about
all the predicate about Joe related to Mary, we would just write

```
  /user<Joe> ?p /user<Mary>
```

Where ?p represents all possible predicates. Bindings become more interesting
when we start building complex graph patterns that contain more than one clause.
Imagine you want to get the list of all users that are grand parents. You could
express such pattern as

```
  ?grand_parent "parent_of"@[] ?x . ?x "parent_of"@[] ?grand_child
```

You can combine multiple graph patterns together using '.' to separate clauses.
The important thing to keep in mind is that the above composite clause
represents a single context. That means that ?x is a binding that once
instantiated, it cannot change the value in that context. Imagine Joe is the
parent of Peter and Peter is the parent of Mary. Once the first part of the
clause is match againt Joe is the parent of Peter, ```?grand_parent``` gets
binded against Joe and ```?x``` against Peter. To satisfy the second part of
the composite clause we now need to find triples where the subject is Peter
(remember that once the value is binded in a context it cannot change) a
predicate saying it is the parent of, and then if that exist then
```?grand_child``` would get binded and take the value of Mary.

As we will see in later examples, bindings can be use to also identify
nodes, literals, predicates, or time anchors.

## Querying Data from graphs

Querying data in BQL is done via the ```select``` statement. The simple form
of a query is expressed as follows

```
  SELECT ?grand_child
  FROM ?family_tree
  WHERE {
    /user<Joe> "parent_of"@[] ?x . ?x "parent_of"@[] ?grand_child
  };
```

The above query would return all the grand children of Joe. BQL users binding
notation to identify a graph to use. It over uses the '?' and it just indicates
the name of the graph. In the above example that query would be run against
a graph which ID is equal to "?family_tree". You can also query against multiple
graphs.

```
  SELECT ?grand_child
  FROM ?family_tree, ?other_family_tree
  WHERE {
    /user<Joe> "parent_of"@[] ?x . ?x "parent_of"@[] ?grand_child
  };
```

There is no limit on how many variable you may return. You can return multiple
variables instead as shown below.

```
  SELECT ?grand_parent, ?grand_child
  FROM ?family_tree
  WHERE {
    ?grand_parent "parent_of"@[] ?x . ?x "parent_of"@[] ?grand_child
  };
```

The above query would return all grand parents together with the name of their
grand kids, one pair per row. In some cases it is useful to return a different
name for the variables, and not use the biding name used in the graph pattern
directly. This is achieved using the as keyword as shown below.

```
  SELECT ?grand_parent as ?gp, ?grand_child as ?gc
  FROM ?family_tree
  WHERE {
    ?grand_parent "parent_of"@[] ?x . ?x "parent_of"@[] ?grand_child
  };
```

It is important to note that alias are defined outside the graph pattern scope.
Hence, alias cannot be used in graph patterns.

BQL supports basic grouping and aggregation. To achieve this, it is accomplished
via ```group by```. The above query may return duplicates depending on the data
available on the graph. If we want to get rid of the duplicates we could just
group them as follows to remove the duplicates.

```
  SELECT ?grand_parent as ?gp, ?grand_child as ?gc
  FROM ?family_tree
  WHERE {
    ?grand_parent "parent_of"@[] ?x . ?x "parent_of"@[] ?grand_child
  }
  GROUP BY ?gp, ?gc;
```

As you may have expected, you can group by multiple bindings or aliases. Also,
grouping allows a small subset of aggregates. Those include ```count``` its
variant with distinct, and ```sum```. Other functions will be added as needed.
The queries below illustrate how this simple aggregations can be used.

```
  SELECT ?grand_parent as ?gp, count(?grand_child) as ?gc
  FROM ?family_tree
  WHERE {
    ?grand_parent "parent_of"@[] ?x . ?x "parent_of"@[] ?grand_child
  }
  GROUP BY ?gp;
```

Would return the number of grand childre per grand parent. However, it would
be better if the distinct version was used to guaranteed that all duplicates
resulting on the graph data are removed. The query below illustrates how
the distinct variant work.

```
  SELECT ?grand_parent as ?gp, count(distinct ?grand_child) as ?gc
  FROM ?family_tree
  WHERE {
    ?grand_parent "parent_of"@[] ?x . ?x "parent_of"@[] ?grand_child
  }
  GROUP BY ?gp;
```

The sum agreegation only works if the binding is done against a literal of type
```int64``` or ```float64```, as shown on the example below.

```
  SELECT sum(?capacity) as ?total_capacity
  FROM ?gas_tanks
  WHERE {
    ?tank "capacity"@[] ?capacity
  }
```

You can also use ```sum``` to do partial accumulations in the same maner as was
done in the ```count``` examples above.

Results of the query can be sorted. By default on ascending order based on
the provided variables. The example below orders first by grand parent name
ascending (implicit direction), and then for each equal value descending based
on the grand child name.

```
  SELECT ?grand_parent, ?grand_child
  FROM ?family_tree
  WHERE {
    ?grand_parent "parent_of"@[] ?x . ?x "parent_of"@[] ?grand_child
  }
  ORDER BY ?grand_parent, ?grand_child DESC;
```

The having modifier allows to filter the returned data further. For instance,
the query below would only return tanks with a capacity bigger than 10.

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
certain date. You could write it as

```
  SELECT ?user
  FROM ?social_graph
  WHERE {
    ?user "folows"@[2006-01-01T15:04:05.999999999Z07:00,] /user<Joe> .
    ?user "folows"@[2006-01-01T15:04:05.999999999Z07:00,] /user<Mary>
  }
```
You can also imagine that this can become tedious fast if you graph pattern
contains multiple clauses. BQL allows you to specify it a more compact and
readable form using composable ```before```, ```after```, and ```between```
keywords. They can be composed together using ```not```, ```and```, and ```or```
operators.

```
  SELECT ?user
  FROM ?social_graph
  WHERE {
    ?user "folows"@[,] /user<Joe> .
    ?user "folows"@[,] /user<Mary>
  }
  AFTER 2006-01-01T15:04:05.999999999Z07:00
```

Which is easier to read. It also allow expressing complex global time bounds
that would require multiple clauses and extra bindings.

```
  SELECT ?user
  FROM ?social_graph
  WHERE {
    ?user "folows"@[,] /user<Joe> .
    ?user "folows"@[,] /user<Mary>
  }
  AFTER 2006-01-01T15:04:05.999999999Z07:00 OR
  BETWEEN 2004-01-01T15:04:05.999999999Z07:00, 2004-03-01T15:04:05.999999999Z07:00
```

Also remember that bindings may take time anchor values so you could also query
for all users that first followed Joe and then followed Mary. Such query would
look like

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

Triples can be inserted into one or more graphs. That can be achieve by just
running the following insert statements.

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

Triples can be deleted from one or more graphs. That can be achieve by just
running the following delete statements.

```
  DELETE DATA FROM ?family_tree, ?other_family_tree {
    /user<Joe>   "parent_of"@[] /user<Peter> .
    /user<Peter> "parent_of"@[] /user<Mary>
  };
```

You should not assume that the delete operation will be atomic. Most of the 
driver implementations may provide such property, but you will have to check
with the driver implementation.
