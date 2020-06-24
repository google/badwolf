# Storage Abstraction Layer

BadWolf does not provide any storage. Instead, it provides a low level API for
data persistence. This allows us to provide different storage implementations
(also known sometimes as drivers), but still maintain the same data
abstractions and data manipulation. This property allows you to use your
favorite backend for data storage, or just implement a new one for your next
project.

BadWolf release comes along only with a simple volatile, RAM-based implementation
of the storage abstraction layer to illustrate how the API can be implemented.

The storage abstraction layer is built around two simple interfaces:

* ```storage.Store``` interface: Allows to create new named graphs.
* ```storage.Graph``` interface: Provides low level API to manipulate and lookup
                      the data stored in the graph. It is important not to
                      to confuse the data lookup capabilities with the BadWolf
                      Query Language.

The goal of these interfaces is to allow writing specialized drivers for
different storage back-ends. For instance, BadWolf provides a simple
volatile memory-only implementation of these two interfaces in the ```storage/memory```
package. All relevant interface definitions can be found in the
[storage.go](../storage/storage.go) file of the ```storage``` package.
