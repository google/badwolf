# Querying the graph: BQL, or the BadWolf Query language

BadWolf provides a high level declarative query language. BQL (or BadWolf
Query Language) is a declarative language losely modeled after
[SPARQL](https://en.wikipedia.org/wiki/SPARQL) to fit the temporal nature of
BadWolf graph data.

## BQL Grammar

The BQL grammar is expressed as a LL1 and implemented using a recursively
descent parser. The grammar can be found in the
[grammar file](https://github.com/google/badwolf/bql/grammar/grammar.go).
The initial version of the grammar is available, as well as the lexical and 
syntactical parser.

Semantic, planner, optimizer, and executer for BQL are currently work in
progress.
