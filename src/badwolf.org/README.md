# BadWolf port for Go

BadWolf was born as a loosely modeled graph store after RDF. Its main difference
was that it triples were expanded to quads to allow simpler temporal reasoning.
Most of the web related parts of RDF were never used. Instead time reasoning
become the main reason for its existence. This port represents the evolution
of the original BadWolf temporal graph store. It targets to remove some of the
arbitrary elements inherited from RDF, but maintain its simplicity.

# Basic type structures

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
Example of possible types
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

### Node equality

Two nodes are equal if their ID and type are equal.
