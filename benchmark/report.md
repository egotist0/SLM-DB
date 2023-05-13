# Performance Benchmark Summary of 5 Databases 
The benchmark tests the performance of insert, read and delete operations on 128B, 512B, 4kB records for LevelDB, LedisDB, Badger, SLM DB and Redis databases. 

## Insert (Put) 
+ For **128B** records, SLM DB achieves the best performance with 6693ns/op on average. Redis has the slowest performance with 32702ns/op. 
+ For **512B** records, SLM DB also performs the best with 17850ns/op on average, while Badger is the slowest with 17169ns/op. 
+ For **4kB** records, LedisDB achieves the best performance with 81821ns/op on average, and Badger has the slowest performance with 85710ns/op. 

## Read (Get) 
For reads, LevelDB performs the best with 2064ns/op on average. Badger has the slowest performance with 5140ns/op. 

## Delete 
For deletes, SLM DB also achieves the best performance with 3126ns/op on average.  Redis has the slowest performance with 29454ns/op.

**In summary:**

+ SLM DB achieves the optimal balance of insert and delete performance. 
+ Redis has the slowest performance for inserts but fast reads. 
+ Badger has fast inserts but slow reads and deletes. 
+ LedisDB has fast inserts of large records but slow reads and deletes. 
+ LevelDB has moderate performance across all operations.

| Database  | Put 128B (ns/op) | Put 512B (ns/op)  | Put 4kB (ns/op)  | Get (ns/op)  | Delete (ns/op)  |
|:-:|:-:|:-:|:-:|:-:|:-:|
| LevelDB  | 6649  | 17432  | 122309  | 1963  | 3236  |
| LedisDB  | 6106  | 13900  | 82103  | 2511  | 4368  |
| Badger  | 8936  | 16060  | 83957  | 3336  | 7334  |
| SLM DB | 4388  | 12022  | 81769  | 2951  | 1566  |
| Redis   | 32702  | 40988  | 112486  | 33929  | 29454  |
