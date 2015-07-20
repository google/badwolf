# BadWolf port for Go

BadWolf was born as a loosely modeled graph store after RDF. Its main difference
was that it triples were expanded to quads to allow simpler temporal reasoning.
Most of the web related parts of RDF were never used. Instead time reasoning
become the main reason for its existence. This port represents the evolution
of the original BadWolf temporal graph store. It targets to remove some of the
arbitrary elements inherited from RDF, but maintain its simplicity.

# Basic data abstractions

This section describes the three basic data abstractions that BadWolf provides.
It is important to keep in mind, that all data described using BadWolf
abstractions is immutable. In other words, once you have created one of them,
there is no way to mutated its value, however, you will always be able to create
new entities as needed.

## Node

Nodes represents unique entities in a graph. Entities are represented by two
elements: (1) the ID that identifies it entity, and (2) the type of
the entity. You may argue that collapsing both in a single element would achieve
similar goals. However, there are benefits to keep explicit type information
(e.g. indexing, filtering, etc.).

### Node Type

BadWolf does not provide type ontology. Types are left to the owner of the
data to express.  Having that said, BadWolf requires type assertions to be
expressed using hierarchies express as paths separated by forward slashes.
Example of possible types are:

```
   /organization
   /organization/country
   /organization/company
```

Types follow a simple file path syntax. Only two operations are allowed on
types.
* _Equality_: Given two types A and B, A == B if and only if they have the exact
              same path representation. In other words, if strings(A)==string(B)
              wher == is the case sensitive equal.
* _Covariant_: Given two types A and B, A
              [covariant](https://en.wikipedia.org/wiki/Covariance_and_contravariance_(computer_science\))
              B if B _is a_ A. In other word, A _covariant_ B if B is a prefix
              of A.

### Node ID

BadWolf does not make any assumption about ID structure. IDs are represented
as UTF8 strings. No spaces, tabs, LF or CR are allowed as part of the ID to
provide efficient node marshaling and unmarshaling. The only restriction for
node IDs is that they cannot contain for efficient marshaling reasons neither
'<' nor '>'.

### Marshaled representation of a node

Nodes can be marshaled and unmarshaled from a simple text representation. This
representation follows this simple structure ```type<id>``` for efficient
processing. Some example of marshaled into text nodes are listed below.

```
   /organization/country<United States of America>
   /organization/company<Google>
```

### Node equality

Two nodes are equal if their ID and type are equal.

## Literals

Literals are data containers. BadWolf has only a few primitive types that are
allowed to be boxed in a literal. These types are:

* _Bool_ indicates that the type contained in the literal is a bool.
* _Int64_ indicates that the type contained in the literal is an int64.
* _Float64_ indicates that the type contained in the literal is a float64.
* _String_ indicates that the type contained in the literal is a string.
* _Blob_ indicates that the type contained in the literal is a []byte.

It is important to note that a container contains one value, and one value only.
Also, as mentioned earlier, all values and, hence, literals are immutable.
_String_ and _Blob_ can contain elements of arbitrary length. This can be
problematic depending on the storage backend being used. For that reason,
the ```literal``` package provides mechanisms to enforce maximum lenght limits
to protect storage backends.

Two literal builders are provided to create new literals:

* _DefaultBuilder_ allows building valid literals of unbounded size.
* _NewBoundeBuilder_ allows building valid literals of a bounded specified size.

Literals can be pretty printed into a string format. The pretty printing retains
the type and value of the literal. The format of the pretty printing formed
by the string representation of the value between quotes followed by ```^^``` and
the type assertion ```type:``` with the corresponding type appended. This
pretty printing convention loosely follows the
[RDF specification for literals](http://www.w3.org/TR/rdf11-concepts/#section-Graph-Literal)
also simplifying the parsing of such string formated literals. Some examples
of pretty printed literals are shown below.

```
  "true"^^type:bool
  "false"^^type:bool
  "-1"^^type:int64
  "0"^^type:int64,
  "1"^^type:int64
  "-1"^^type:float64
  "0"^^type:float64
  "1"^^type:float64
  ""^^type:text
  "some random string"^^type:text`
  "[]"^^type:blob
  "[115 111 109 101 32 114 97 110 100 111 109 32 98 121 116 101 115]"^^type:blob
```

The above representation can also be used to create a literal.

## Predicates

Predicates allow predicating properties of nodes. BadWolf provide two different
kind of predicates:

* _Immutable_ or predicates that are always valid regardless of when they were
              created. For instance, they are useful to describe properties
              that never change, for instance, the color of someone's eyes.
* _Temporal_ predicates that are anchored at some point along the time
             continuum. For instance, the predicate _met_ describing when
             two nodes met is anchored at a particular time.

It is important to note here that temporal predicates are descriptive of a
property in relation to time. The granularity (or window) of validity of that
predicate is left to the temporal reasoning module. This is important, since
allow to reason against arbitrary time granularities. All time calculations
and reasoning in BadWold assume a Gregorian calendar.

### Predicate ID

Similar to the node IDs, predicate IDs in BadWolf do not make any assumption
about ID structure. IDs are represented as UTF8 strings. No spaces, tabs, LF or
CR are allowed as part of the ID to provide efficient node marshaling and
unmarshaling.

### Time anchors

When parsing or printing dates into time anchors for temporal predicates,
Badwolf follows the [RFC3339](https://www.ietf.org/rfc/rfc3339.txt) variant
[RFC3339Nano](http://golang.org/pkg/time/#pkg-constants) as specified in the
GO programming language to provide reliable granularity to express anchors in
nanoseconds. An example of time anchor express in RFC3339Nano format is shown
below.

```
   2006-01-02T15:04:05.999999999Z07:00
```

So for instance the fully pretty printed predicate for an immutable and  
a temporal triple are shown below.

```
   "colow_of_eyes"@[]
   "met"@[2006-01-02T15:04:05.999999999Z07:00]
```

## triple

The basic unit of storage on BadWolf is the triple. A triple is a three tuple
*<s p o>* defined as follows:

* _s_, or subject, is a BadWolf node.
* _p_, or predicate, is a BadWolf predicate.
* _o_, or object, is either a BadWolf node or a Badwolf literal.
