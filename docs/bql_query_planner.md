# BadWolf Query Language planner

The BadWolf query language is built around a low-level abstraction to manage
and retrieve triples from a fairly arbitrary store. All access to the data goes
through the interfaces defined in the [storage.go](../storage/storage.go) file
of the ```storage``` package. One example of an simple naive implementation
of those interfaces can be found on the ```storage/memory``` package. It
provides a volatile memory-only implementation of both ```storage.Store``` and
```storage.Graph``` interfaces.

The BQL planner that is described here focuses on what happens after the a
```select``` query is properly parsed and it is ready to go. It mostly focuses
on explaining how the data is access given a graph pattern.  As described in
the [BQL](./bql.md) introduction, the graph is queried using graph patterns.
The graph pattern is a collection of clauses separated by '.'. Each clause takes
the form of a generalized triple, where parts of it can be replaced by
'bindings'. You can interpret them as variables that a adopt a non mutable
value during the course of a graph traversal.

## Resolving single clause patterns

Some examples of a graph pattern defined by a single clause are shown below.
To keep it simple only immutable predicates will be used.

* ```/user<Joe> "parent-of"@[] /user<Mary>``` is a fully specified clause with
  no bindings.
* ```/user<Joe> "parent-of"@[] ?child``` is a clause that will match all the
  objects ```?child``` that satisfy having ```/user<Joe>``` as subject and
  ```"parent-of"@[]``` as a predicate.

Resolving clauses with no bindings is equivalent to checking if that fact exists
in the store. The fist clause will be ```true``` if there is a triple supporting
that fact in the graph being queried on the store. It will return ```false```
otherwise. It is important that keep this part of the evaluation in mind. When
we get to how patterns with multiple clauses are evaluated. Intuitively you can
think of them as follows, a composed patter will be ```true``` if all its
clauses are ```true```. It will be ```false``` otherwise. This first clause
would translate into planning to execute a simple call to the interface method
```Exist``` to satisfy it.

The second clause is not as specific as the first one. In the example, what
would be the object has been replaced by the binding ```?child```. This
indicates that we care about the objects that satisfy having ```/user<Joe>```
as subject and ```"parent-of"@[]``` as a predicate. If such triples exist on
the queried graph, the clause would be ```true```. If there are no triple that
would match such a clause, it will evaluate to ```false```.

The binding will take all the possible values available on the graph. This mean
that for a given matching iteration ```?child``` will only have one value across
the pattern. The planner will decide that to resolve such a clause, it will
require to use the ```TriplesForSubjectAndPredicate``` interface method.

Let's assume that for the rest of this document our graph will
contain the following triples:

```
/user<Joe> "parent-of"@[] /user<Mary>
/user<Joe> "parent-of"@[] /user<Peter>
/user<Peter> "parent-of"@[] /user<Jane>
/user<Peter> "parent-of"@[] /user<Mary Anne>
```

Given the above data, the clause ```/user<Joe> "parent-of"@[] ?child``` is
```true```. Also, resolving the clause triggered two binding iterations where
```?child``` would be binded to ```/user<Mary>``` and ```/user<Peter>``` in
each iteration.

## Specificity of a clause.

Given a clause, we define its specificity by the number of bindings present.
The _specificity_ of a clause,or _S(c)_, can only take 4 possible values: 0, 1,
2, 3. Below you have a table of all the possible specificity values based on
the bindings present.

| Clause _c_                                  | _S(c)_ |
|:--------------------------------------------|:------:|
| ```/user<Joe> "parent-of"@[] /user<Mary>``` |    3   |
| ```?s "parent-of"@[] /user<Mary>```         |    2   |
| ```/user<Joe> ?p /user<Mary>```             |    2   |
| ```/user<Joe> "parent-of"@[] ?o```          |    2   |
| ```?s ?p /user<Mary>```                     |    1   |
| ```?s "parent-of"@[] ?o```                  |    1   |
| ```/user<Joe> ?p ?o```                      |    1   |
| ```?s ?p ?o```                              |    0   |

Clauses with _S(c)_=0 indicate clauses that would match the entire graph.

## Resolving multi clause patterns

Imagine that given the example graph you would like who are the grandchild of
Joe. You can express that query by using a compound pattern form by two clauses.

```
/user<Joe> "parent-of"@[] ?child .
?child "parent-of"@[] ?grand_child
```

This pattern contains two clauses. Both need to be satisfied in order to satisfy
the pattern. Another interesting point is that this clauses have a binding
dependency. The first and the second clause in the pattern share the
```?child``` binding. Remember that a binding takes a non mutable value during
a binding iteration. This means that we have a few options on how we plan to
query the graph at hand to see if we can satisfy this pattern.

1. We get all the children that we can find for Joe, then for each child we
   try to see if they are the parent of any other kid.
2. We look for all kids and get the list of parents, then for each of those
   parents we try to see if they are the kids of Joe.
3. We get all the children of Joe. We also get all parents and children in the
   graph. We take both sets of data and filter our any children in the Graph
   that whose parent is not a children of Joe.

All three options are valid options that the planner could chose to try to
satisfy the given graph pattern. The first one has the benefits of trying to
narrow the amount of data to swift through. The second option would likely yield
large amounts of data to on parents, and then use all that data to find which
of this parents is a child of Joe. Finally, the third option would allow us to
concurrently get the data to satisfy both clauses, but then we will have to
reduce it to find the final answer.

If we look a bit more about each of this clauses, we can see that _S(c)_ of the
first one is 2, whereas the specificity of the second one is 1. It is reasonable
to assume that more specific clauses will return less data. This assumption may
not always hold true, since it depends on the branching factor of our graph, but
it is a good intuition on which build the planner.

## Nave Specificity-Based Query planner

BQL uses a pretty simple planner. It does not use any statistics about the graph
queried. The main reason for it is that it may not be available. Remember that
the details of the storage are abstracted away by the ```storage``` package.
However, even with such constrains, we can build a pretty efficient planner
that will work efficiently across a wide variety of reasonable connected graphs.

The planner (P) will try to satisfy a pattern following the steps described below:

1. P will create a graph where nodes clauses.
2. P will add an edge between two clauses if they share a binding. The edge will
   be directed if and only if the two clauses have different specificity. The
   direction will go from higher specificity clause to lower specificity one.
3. It will start with specificity level set to 3.
4. Using the constructed graph, it will concurrently attempt to satisfy all
   clauses with _S(c)_ equal to the current specificity level.
5. Collect all the binding values.
6. If any of the clauses could not be satisfied, the plan will dictate query
   finalization and pattern unsatisfiability.
7. If multiple bindings are available all possible combinations will be
   considered during the binding iteration process.
8. If the specificity level is greater than 0, it will be decrease by 1.
9. If the specificity level is greater or equal than 0, the planner will proceed
   to step 5.

Once if the process is not aborted, the pattern is satisfied and the query will
return all the values that were binded in the process as a simple table.
