
------------

### Cassandra useful resources

- Data modeling tips
  - [Cassandra](https://cassandra.apache.org/doc/latest/cassandra/data_modeling/index.html)
  - [Datastax](https://www.datastax.com/blog/basic-rules-cassandra-data-modeling)
  - [Instclustr best practices](https://www.instaclustr.com/blog/cassandra-data-modeling/)
- Videos
  - [Datastax Academy DS220, Data Modeling with Apache
    Cassandra](https://www.youtube.com/playlist?list=PL2g2h-wyI4SqIigskyJNAeL2vSTJZU_Qp) (26 videos)
  - [Cassandra Day 2021, Advanced Data Modeling](https://www.youtube.com/watch?v=_o1OAKpSoVk&t=14625s)
- Tricky
  - tombstones / deleting
    - [1](https://docs.datastax.com/en/archived/cassandra/3.0/cassandra/dml/dmlAboutDeletes.html)
    - [2](http://thelastpickle.com/blog/2016/07/27/about-deletes-and-tombstones.html)
    - [3](http://thelastpickle.com/blog/2018/07/05/undetectable-tombstones-in-apache-cassandra.html)
    - [4](https://www.slideshare.net/DataStax/deletes-without-tombstones-or-ttls-eric-stevens-protectwise-cassandra-summit-2016)
  - [compression](http://thelastpickle.com/blog/2018/08/08/compression_performance.html)
  - where and filtering
    - [1](https://www.datastax.com/blog/deep-look-cql-where-clause)
    - [2](https://lostechies.com/ryansvihla/2014/09/22/cassandra-query-patterns-not-using-the-in-query-for-multiple-partitions/)
  - keyspaces limitations
    - [1](https://docs.aws.amazon.com/keyspaces/latest/devguide/functional-differences.html)
    - [2](https://docs.aws.amazon.com/keyspaces/latest/devguide/cassandra-apis.html)
    - [3](https://www.javagists.com/difference-between-apache-casandra-and-amazon-keyspaces)

### Keep in mind

Some pitfalls of approach:

- Pro: Good for writing a lot of events
- Con: Specific data model, bad for full-scans

Important things:

- Query-Centered Design
  - (and use prepared statements)
- Spread data evenly around the cluster
- Selecting an Effective Partition Key
  - Minimize the number of partitions read
  - Partition size.
    - `~1 Mb` (5-10 Mb max)
    - token-aware drivers
- No need:
  - Minimize the Number of Writes (Writes cheap, optimized for high write throughput)
  - Minimize Data Duplication (no JOINs: tradeoff disk space vs CPU/memory)

#### NOT to do

- full scan
- allow filtering
- paging with a lot of data
- reverse order reads
- consistency: CL ANY/CL ALL/CL ONE (try `CL LOCAL QUORUM`)

#### Recommended limits

- _( from [ref limits](https://docs.datastax.com/en/cql-oss/3.x/cql/cql_reference/refLimits.html) )_
  - Cells in a partition: `~2 billion (2^31)`
    - single column value size: `2 GB`
      - Recommended: `1 MB`
  - Clustering column value, length of: `65535 (2^16-1)`
  - Key length: `65535 (2^16-1)`
  - Table / CF name length: `48` characters
  - Keyspace name length: `48` characters
  - Query parameters in a query: `65535 (2^16-1)`
  - Statements in a batch: `65535 (2^16-1)`
  - Fields in a tuple: `32768 (2^15)`
  - Recommended: just a few fields, such as 2-10
  - Collection
  - collection limit: ~2 billion (`2^31`);
    - List: values size: `65535 (2^16-1)`
    - Set: values size: `65535 (2^16-1)`
    - Map: number of keys: `65535 (2^16-1)`; values size: `65535 (2^16-1)`
  - Blob size: `2 GB`
  - recommended: less than `1 MB`
  - check if AWS Keyspaces is actually support `IN` in where?
    - [1](https://docs.aws.amazon.com/keyspaces/latest/devguide/working-with-queries.html)
    - [2](https://stackoverflow.com/questions/73197046/is-there-an-alternative-for-the-unsupported-cql-in-operator-in-amazon-keyspaces)

- (from jaeger):
  - `gc_grace_seconds = 10800` (_3 hours of downtime acceptable on nodes_)
  - compaction
  
    ```cassandraql
    WITH compaction = { 
    'compaction_window_size': '1', 'compaction_window_unit': 'HOURS', 
    'class': 'org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy' }
    ```
