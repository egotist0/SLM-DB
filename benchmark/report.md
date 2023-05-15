# Performance Benchmark Summary of 5 Databases 
The benchmark tests the performance of insert, read and delete operations on 128B, 512B, 4kB records for LevelDB, LedisDB, Badger, SLM DB and Redis databases. 
As Redis operates as a server-side database providing network services, its performance is expected to be influenced by network communication. Therefore, it is only presented here for reference. LevelDB, LedisDB, Badger, and SLM DB, on the other hand, serve as embedded databases, offering direct access to their read and write operations. Hence, the performance test will primarily focus on comparing their respective read and write timings.

## Insert (Put) 
+ For **128B** records, SLM DB achieves the best performance with 4388ns/op on average. Badger has the slowest performance with 8936ns/op. 
+ For **512B** records, SLM DB also performs the best with 12022ns/op on average, while LevelDB is the slowest with 12209ns/op. 
+ For **4kB** records, SLM DB achieves the best performance with 81769ns/op on average, and LevelDB has the slowest performance with 17432ns/op. 

## Read (Get) 
For reads, LevelDB performs the best with 1963ns/op on average. Badger has the slowest performance with 3336ns/op. 

## Delete 
For deletes, SLM DB also achieves the best performance with 1566ns/op on average.  Badger has the slowest performance with 7334ns/op.

**In summary:**

+ SLM DB achieves the optimal balance of insert and delete performance. 
+ Level has fast reads and deletes but slow inserts. 
+ Badger has fast inserts of large records but slow reads and deletes. 
+ LedisDB has moderate performance across all operations. 

| Database  | Put 128B (ns/op) | Put 512B (ns/op)  | Put 4kB (ns/op)  | Get (ns/op)  | Delete (ns/op)  |
|:-:|:-:|:-:|:-:|:-:|:-:|
| LevelDB  | 6649  | 17432  | 122309  | 1963  | 3236  |
| LedisDB  | 6106  | 13900  | 82103  | 2511  | 4368  |
| Badger  | 8936  | 16060  | 83957  | 3336  | 7334  |
| SLM DB | 4388  | 12022  | 81769  | 2151  | 1566  |
| Redis   | 32702  | 40988  | 112486  | 33929  | 29454  |
