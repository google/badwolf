# BadWolf

BadWolf is a graph store modeled after a [Resource Description Framework (RDF)](https://en.wikipedia.org/wiki/Resource_Description_Framework). It presents
a flexible storage and data-interchange model for representing a directed graph that 
accommodates the storage and linking of arbitrary objects without the need for a rigid schema. 

BadWold began as a [triplestore](https://en.wikipedia.org/wiki/Triplestore), but triples have
been expanded to quads to allow simpler temporal reasoning. Because BadWolf is designed
for generalized relationship storage, most of the web-related parts of RDF are not used. 
Instead time reasoning became the main reason for its existence. 

In case you are curious about the name, BadWolf is named after the
[BadWolf entity](http://tardis.wikia.com/wiki/Bad_Wolf_(entity) ) as it appeared
in Dr. Who series after Rose Tyler looked into the Time Vortex itself. The
BadWolf entity scattered events in time as self encode messages, creating a
looped ontological paradox. Hence, naming a temporal graph store after the
entity seemed appropriate.

You can find more detail information on each of the components of BadWolf below:

* [Data Abstractions for Temporal Graph Modeling](./docs/temporal_graph_modeling.md).
* [Storage Abstraction Layer](./docs/storage_abstraction_layer.md).
* [Graph Marshaling/Unmarshaling](./docs/graph_serialization.md).
* [BadWolf Query Language](./docs/bql.md).
