# SLM DB README

SLM DB (Single-Level Merge DB) is a single-machine key-value database inspired by [SLM-DB](https://www.usenix.org/conference/fast19/presentation/kaiyrakhmet) presented at USENIX FAST '19, [lotudb](https://github.com/flower-corp/lotusdb), and [Wisckey](https://www.usenix.org/system/files/conference/fast16/fast16-papers-lu.pdf).

## Features

SLM DB combines the advantages of B+ trees and LSM trees to provide excellent read and write performance. Some of its features include:

- Maintains an in-memory B+ tree to store record file IDs and offsets, enabling fast queries and range queries.
- Supports sequential and append writes similar to LevelDB to ensure high performance for bulk data writes.
- Implements a write-ahead log to ensure data durability.
- Uses a multi-level memtable based on Skiplist to guarantee O(log N) query performance.
- Implements a single-level SSTable and uses selective compaction to manage garbage collection.

For detailed performance benchmark results, please refer to [benchmark/report.md](https://github.com/egotist0/SLM-DB/blob/master/benchmark/report.md).

## Getting Started

### As an Embedded Database

SLM DB can be used as an embedded database. Please refer to [usage](https://github.com/egotist0/SLM-DB/blob/master/usage) for detailed instructions.

### As a Server

To use SLM DB as a server, first start the server by running:

```go
go run cmd/server/server.go
```

Then, you can perform operations as a client by running:

```go
go run cmd/server/client.go
```

The server provides four main interfaces:

- `PUT key value`: Set the string value of a key.
- `GET key`: Get the value of a key.
- `DELETE key`: Delete a key.
- `HELP`: Get help about the DB client commands.

## License

SLM DB is licensed under the MIT license. See the [LICENSE](https://github.com/egotist0/SLM-DB/blob/master/LICENSE) file for details.
