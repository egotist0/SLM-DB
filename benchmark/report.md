# Performance Benchmark Summary of 5 Databases 
The benchmark tests the performance of insert, read and delete operations on 128B, 512B, 4kB records for LevelDB, LedisDB, Badger, SLM DB and Redis databases. 

## Insert (Put) 
+ For **128B** records, LevelDB achieves the best performance with 6693ns/op on average. Redis has the slowest performance with 32702ns/op. 
+ For **512B** records, LevelDB also performs the best with 17850ns/op on average, while Badger is the slowest with 17169ns/op. 
+ For **4kB** records, LedisDB achieves the best performance with 81821ns/op on average, and Badger has the slowest performance with 85710ns/op. 

## Read (Get) 
For reads, LevelDB performs the best with 2064ns/op on average. Badger has the slowest performance with 5140ns/op. 

## Delete 
For deletes, LevelDB also achieves the best performance with 3126ns/op on average.  Badger has the slowest performance with 8067ns/op.

**In summary:**

+ LevelDB achieves the optimal balance of insert, read and delete performance. 
+ Redis has the slowest performance for inserts but fast reads. 
+ Badger has fast inserts but slow reads and deletes. 
+ LedisDB has fast inserts of large records but slow reads and deletes. 
+ SLM DB has moderate performance across all operations.

| Database  | Put 128B (ns/op) | Put 512B (ns/op)  | Put 4kB (ns/op)  | Get (ns/op)  | Delete (ns/op)  |
|:-:|:-:|:-:|:-:|:-:|:-:|
| LevelDB  | 6693  | 17850  | 118149  | 2064  | 3126  |
| LedisDB  | 6314  | 13532  | 81821  | 2542  | 4074  |
| Badger  | 9138  | 17169  | 85710  | 5140  | 8067  |
| SLM DB | 4494  | 12483  | 83007  | 6887  | 1764  |
| Redis   | 32702  | 40988  | 112486  | 33929  | 29454  |
