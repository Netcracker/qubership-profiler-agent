
## CDT Data model analysis


- [sorting & zip](https://www.uber.com/en-IN/blog/cost-efficiency-big-data)
- [encryption](https://github.com/apache/parquet-format/blob/master/Encryption.md)

Appendix:

- [Apache Pinot vs ELK](https://www.uber.com/en-IN/blog/real-time-analytics-for-mobile-app-crashes)
- [InfluxDb IOx (Parquets as dataset)](https://www.youtube.com/watch?v=Zaei3l3qk0c)
- [InfluxDb IOx compactor](https://github.com/influxdata/influxdb/blob/main/docs/compactor.md)

------------

### Problems

From users perspective:

- _Resources_:
  - Heavy impact on Cassandra/OpenSearch during write operations
    - a lot of reads before write (pod state, dictionary) => Cache
    - invalid index mapping for OpenSearch
      - (no time-range partitions, by days)
      - skip freq. updated fields from reindex
  - A lot of data stored data (500 **Gb**)
    - No observability (metrics, stats who take most space)
    - No ability to delete abnormalities from UI (old days, suspicious pods)
    - Time-range partitions, delete by TTL automatically?
    - May be bugs on `StreamCleaner`?
- _Usability_:
  - Long queries from UI
    - Unnecessary reads from meta (pod state, dictionary) => cache
    - Ineffective reads (no time-range partitions, by days)
    - Calls
      - Reads archives for calls:
        - `OOM` => read ALL calls and sorting/filtering in memory
          - limit by duration, ban durations `<1ms` ?
      - `OpenSearch`: a lot of requests instead one with `IN` (_impossible for `Cassandra`_)
      - => persist separated table/structure for calls
  - Old UI for call tree

------------

### Persistence options

#### Cassandra

1. Pro: Good for writing a lot of events
2. Con: Specific data model, bad for full-scans

#### OpenSearch

1. Pro: Good for search, especially full-text
2. Con: Bad for insert/updates

also:

- create new record for whole document for smallest update!
- trigger re-indexing (including heavy full-text!) for writes
  - should switch off for blobs - (`binary` type)
  - should switch off for freq updates (`activeSince`, etc.)
- "partitions" by days for indexes

#### S3

1. Pro:
   - Should be good for writing at high speed
   - Should be good to load specified (pod+time) archives
2. Con:
   - No possible to create specialized table `calls`
   - custom Loki-like indexes?

#### Clickhouse

1. Pro:
   - Good for writing at high speed (especially, timeseries append)
   - Best choice to create specialized table `calls`
   - No limitation for partitions (unlike Cassandra)
2. Con:
   - Yet another database for installation

#### Analysis

##### Current problems

1. `pod_details` with timestamp as name/key, no TTL?
2. `pod_name` does not have `namespace` in name
3. no `date` in partition keys for timebased tables
4. no `TimeWindowCompactionStrategy` compaction for timebased tables
5. `gc_grace_seconds` = 60 vs `gc_grace_seconds` = 864000
6. `default_time_to_live` = 0 ?
7. `//there seems to be a bug in cassandra driver. when connections are overloaded,
 it returns empty results instead of errors` ???
8. a lot of reads in `collector`

See also current schema analysis: [old.schema.analysis.md](old.schema.analysis.md)

------------

##### TODO

1. `gc_grace_seconds` = 10800 (3 hours of downtime acceptable on nodes) (from Jaeger)
2. for time-based tables:
   - introduce `date` as column for partition key
   - `WITH compaction = { 'compaction_window_size': '1', 'compaction_window_unit': 'HOURS',
   'class': 'org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy' }`
   - specify TTL (max of available) `default_time_to_live`  (save `StreamCleaner` for configured cleaning)

------------

See also new schema design and proposal: [db_schema_design.md](db_schema_design.md) and  [db_schema_new.md](db_schema_new.md)
