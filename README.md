# BadWolf

BadWolf was born as a loosely modeled graph store after RDF. However, triples
were expanded to quads to allow simpler temporal reasoning. Most of the web
related parts of RDF were never used. Instead time reasoning become the main
reason for its existence. This project represents the evolution of the original
BadWolf temporal graph store. Most of the original RDF structs have been
removed, however BadWolf targets to retain its simplicity and flexibility.

In case you are curious about the name, BadWolf is named after the
[BadWolf entity](http://tardis.wikia.com/wiki/Bad_Wolf_(entity) ) as it appeared
in Dr. Who series after Rose Tyler looked into the Time Vortex itself. The
BadWolf entity scattered events in time as self encode messages, creating a
looped ontological paradox. Hence, naming a temporal graph store after the
entity seemed appropriate.

You can find more detail information on each of the components of BadWolf below:

* [Data Abstractions for Temporal Graph Modeling](./docs/temporal_graph_modeling.md).
* [Storage Abstraction Layer](./docs/storage_abstraction_layer.md).
* [BadWolf Query Language](./docs/bql.md).
