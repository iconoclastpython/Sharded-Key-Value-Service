# Sharded-Key-Value-Service

Implemented a Sharded key/value database service, where each shard replicates its state based on Paxos algorithm,
which handles network partitions and is fault tolerant, scalability, and availability.

Deals with Put(), Append(), and Get() requests concurrently and supports MapReduce application on the database.

The primary/backup replication, assisted by a view service that decides which machines are alive. The view service allows the primary/backup service to work correctly in the presence of network partitions. The view service itself is not replicated, and is a single point of failure.

The sharded key/value database, where each shard replicates its state using Paxos. This key/value service can perform Put/Get operations in parallel on different shards, allowing it to support applications such as MapReduce that can put a high load on a storage system. Also, it has a replicated configuration service, which tells the shards for what key range they are responsible. It can change the assignment of keys to shards, for example, in response to changing load. Lab 4 has the core of a real-world design for 1000s of servers.
