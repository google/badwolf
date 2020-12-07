# How to add support for a new `FILTER` function in BadWolf

The steps to add support for a new `FILTER` function are:

1. Add a new enum item in the list of supported filter Operations in `filter.go`;

2. Add a new entry in the `SupportedOperations` map in `filter.go` to map the **lowercase**
string of the filter function being added to its correspondent `filter.Operation` element;

3. If this new filter function requires a `Value` parameter, add the correspondent
`filter.Operation` to the `OperationRequiresValue` hash set in `filter.go`;

4. Update the `String` method of `Operation` in `filter.go`;

5. Add a new switch case inside `compatibleBindingsInClauseForFilterOperation` in `planner.go` to
specify for which fields and bindings of a clause the newly added `filter.Operation` can be applied to;

6. Implement the appropriate behavior on the driver side (for the volatile driver, in `memory.go`).


## Notes on implementing the driver behavior

The `FILTER` specifications always arrive to the driver level in the form of a `FilterOptions` struct
inside `storage.LookupOptions`. The `Operation` represents the `FILTER` function being applied (eg: `IsTemporal`),
while `Field` represents the position of the graph clause it shall be applied to (subject, predicate or object)
and `Value` encapsulates the second argument of the `FILTER` function (not applicable for all `Operations` - some
like `IsTemporal` do not use it while others like `GreaterThan` do, see [Issue 129](https://github.com/google/badwolf/issues/129)).

Then, to implement a given `FILTER` behavior in the driver level all the user shall do is use the instructions for data
retrieval received inside `FilterOptions` and proceed accordingly for the specific storage infrastructure they have at hand.

**N.B.** When implementing the driver side of the `FILTER` functions, pay attention to the order to process the information
from `LookupOptions`, as it may influence the final result. For the volatile driver in `memory.go`, for example, on which the
processing is sequential, the order is:

1) Global time bounds first (`LowerAnchor` and `UpperAnchor`);

2) `FilterOptions`;

3) `MaxElements`, to limit the number of elements to return.

This way, if we have both a `BETWEEN` and a `FILTER latest` in a given query, the `latest` operation will be applied
only to the triples that are already in the time interval specified by `BETWEEN`, as one should expect. The order with
the `FILTER` being processed first could give wrong results as the latest triple filtered beforehand could not necessarily
be in the interval specified by `BETWEEN`, which would wrongly return an empty result by the end.
