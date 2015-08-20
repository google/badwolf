# Graph Marshalin/Unmarshaling

Graph can be marshaled to text or unmarshaled back from text into a graph
using the ```io``` package. The package provides two simple functions in
[io](../io/io.go).

* ```ReadIntoGraph``` reads triples from a text reader into the provided
                      graph. The expected format is one triple per line, and
                      subject, predicate, and object separated by tabs.
* ```WriteGraph``` writes the triples of the provided graph into a text writer.
                   Each triple is written into a separate line where subject,
                   predicate, and object are separated by tabs.
