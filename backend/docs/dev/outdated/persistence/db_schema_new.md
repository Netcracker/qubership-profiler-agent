
### Schema

#### Pods

- table `pods`
  - list of all pods deployments
  - from collector:
    - seldom inserts at connections' start in case cache miss [`W.A` query]
    - frequent updates for `last_active`  [`W.C` query]
  - from UI:
    - frequent reads during user session  [`W.C` query]
    - cached?
  - compact strategy: ?

    | part                              | type        | description                            |
    |-----------------------------------|-------------|----------------------------------------|
    | namespace text, service_name text | partition   |                                        |
    | active_since timestamp            | clustering  | [DESC] start of first deployment (pod) |
    | pod_name text                     | clustering  | [ASC] simple pod name (without ts)     |
    | pod_id text                       |             | pod id (as FK: `ns.service.pod`)       |
    | last_restart timestamp            |             | pod id of last restart (FK)            |
    | last_active timestamp             |             | latest ack from pod                    |

- table `pod_restarts`
  - list of all pods restarts
  - from collector:
    - seldom inserts at connections' start in case cache miss [`W.A` query]
    - frequent updates for `last_active`  [`W.C` query]
  - from UI:
    - frequent reads during user session  [`W.C` query]
    - cached?
  - compact strategy: ?

  | part                                             | type       | description                     |
  |--------------------------------------------------|------------|---------------------------------|
  | namespace text, service_name text, pod_name text | partition  |                                 |
  | pod_id text                                      | partition  | uniq id of restart (FK)         |
  | restart_time timestamp                           | clustering | [DESC] start of this restart    |
  | active_since timestamp                           |            | start of first deployment (pod) |
  | last_active timestamp                            |            | latest ack from pod             |

- table `pod_statistics`
  - statistics by minutes for each live pod (`granularity=1m`)
  - from collector/job: [`W.A` query]
    - one INSERT for minute
    - no UPDATES for past rows
  - from UI: [`R.B` query]
    - several scan (by timerange) during user session
  - compact strategy: `TimeWindowCompactionStrategy`?

  | part                                   | type       | description                              |
  |----------------------------------------|------------|------------------------------------------|
  | date date                              | partition  | `date` or `date+hour?`                   |
  | pod_id text                            | partition  | FK                                       |
  | time timestamp                         | clustering | `granularity=1m`, aggregation at time    |
  | data_accumulated map<text, bigint>     |            | stat grouped by stream (gzip in DB)      |
  | original_accumulated map<text, bigint> |            | stat grouped by stream (orig from agent) |

---

#### Meta (for call tree/list)

- table `pod_dictionary`
  - one row per pod with cached dictionary
  - from collector: [`W.A` query]
    - one big INSERT
    - seldom updates (_if missed cache, working with actual pods_)
  - from UI:
    - one SELECT per pod [`R.B` query]
    - +cache? (_most of the time working with actual pods_)
  - compact strategy: ?

  | part                     | type       | description                                                |
  |--------------------------|------------|------------------------------------------------------------|
  | pod_id text              | partition  | FK                                                         |
  | tag_info map<text, json> |            | name -> (index bool, list bool, order int, signature text) |
  | literals map<int, text>  |            | dictionary of literals (to decode)                         |

- table `pod_suspend`
  - one row per pod per minute (`granularity=1m`)
  - from collector: [`W.A` query]
    - several INSERT+UPDATE (5-6 total for 1m)
    - no updates for past rows
  - from UI: [`R.B` query]
    - SELECT for selected pods in timerange
  - compact strategy: `TimeWindowCompactionStrategy`?

| part                                  | type       | description                           |
|---------------------------------------|------------|---------------------------------------|
| date date                             | partition  | `date` or `date+hour?`                |
| pod_id text                           | partition  | FK                                    |
| time timestamp                        | clustering | [DESC] `granularity=1m`               |
| suspends map<timestamp, suspend_time> |            | dictionary of found suspend in minute |

---

#### Blobs

> - **separate** tables for different stream types!

One active memtable per table. Flushed onto disk (become immutable SSTables) by triggered:

- The memory usage of the memtables exceeds the configured threshold (see `memtable_cleanup_threshold`)
- The commit-log approaches its maximum size, and forces memtable flushes in order to allow commitlog segments to be
  freed

See also

- [storage engine](https://cassandra.apache.org/doc/latest/cassandra/architecture/storage_engine.html)
- [cassandra-memtable](https://abiasforaction.net/apache-cassandra-memtable-flush/)

> - enable partition/row cache for hot time updates ?
>   - `5-10s`? should investigate

- [opsConfiguringCaches](https://docs.datastax.com/en/cassandra-oss/3.0/cassandra/operations/opsConfiguringCaches.html)
- [opsSetCaching](https://docs.datastax.com/en/cassandra-oss/3.0/cassandra/operations/opsSetCaching.html)

> - **date** as part of primary key!!
>   - `TimeWindowCompactionStrategy` as compact strategy

See also
[cassandra compaction](https://cassandra.apache.org/doc/latest/cassandra/operating/compaction/index.html)

---

- table `stream_meta`
  - statistics by minutes for each live pod
  - update every minute (from collector)
  - several scan (by timerange) during user session (from UI)
  - compact strategy: `TimeWindowCompactionStrategy`?

  | part                     | type        | description            |
  |--------------------------|-------------|------------------------|
  | date date                | partition   | `date` or `date+hour?` |
  | pod_id text              | partition   | FK                     |
  | stream_name text         | clustering  | [ASC]                  |
  | rolling_sequence_id int  | clustering  | [DESC] `timeuuid` ?    |
  | create_when timestamp    |             |                        |
  | modified_when timestamp  |             |                        |
  | data_accumulated bigint  |             |                        |

- table `stream_td` (_same for `top`, `xml`, `sql`, `trace`, `gc`_)
  - from collector: [`W.B`]
    - update while working (in hot time)
    - never updates for past rows
  - from UI
    - several scan (by timerange) during user session ()
  - compact strategy: `TimeWindowCompactionStrategy`?

  | part                    | type       | description            |
  |-------------------------|------------|------------------------|
  | date date               | partition  | `date` or `date+hour?` |
  | pod_id text             | partition  | FK                     |
  | rolling_sequence_id int | partition  | FK                     |
  | chunk map<int,blob>     |            |                        |
  | length bigint           |            | for stat               |

- table `stream_calls`
  - archives with calls for each pod
  - merge chunks for same `seqId` in one row (by map)
  - add `durations` histogram (`>10ms`,`>100ms`,`>1s`,`>5s`,`>10s`)
    - mark if calls with such durations contains in stream
    - helps against small streams archives
  - from collector: [`W.B`]
    - update while received time
    - never updates for past rows
    - **performance optimization**:
      - update `durations` set to help search from UI
      - only for latest 5-10 min (in hot time, thus MemoryTable)
  - from UI [`R.C`]
    - **HEAVY** (**OFTEN!**) scans (by timerange) during user session
  - compact strategy: `TimeWindowCompactionStrategy`?

  | part                    | type       | description                        |
  |-------------------------|------------|------------------------------------|
  | date date               | partition  | `date` or `date+hour?`             |
  | pod_id text             | partition  | FK                                 |
  | rolling_sequence_id int | partition  | FK                                 |
  | chunk map<int,blob>     |            |                                    |
  | length bigint           |            | for stat                           |
  | _durations?_ set\<int>  |            | _histogram_? for filtering from UI |

- table `stream_heap_dump`
  - big archive file (`> 100-400 Mb`), several chunks
  - from collector: [`W.B`]
    - only insert during upload
    - never updates for past rows
  - from UI:
    - one select during download
  - compact strategy: `TimeWindowCompactionStrategy`?

  | part                    | type       | description            |
  |-------------------------|------------|------------------------|
  | date date               | partition  | `date` or `date+hour?` |
  | pod_id text             | partition  | FK                     |
  | rolling_sequence_id int | partition  | FK                     |
  | start_pos int           | clustering | [ASC]                  |
  | chunk blob              |            |                        |
  | length bigint           |            | for stat               |

---

### Appendix A. Investigation of other systems

---

#### Jaeger schema

```sql

--------------------------------------------------------------------------------

create table jaeger.tag_index
(
    service_name text,
    tag_key      text,
    tag_value    text,
    start_time   bigint,
    trace_id     blob,
    span_id      bigint,
    primary key ((service_name, tag_key, tag_value), start_time, trace_id, span_id)
)
    with clustering order by (start_time desc, trace_id asc, span_id asc)
     and compaction = {'class': 'org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy', 'compaction_window_size': '1', 'compaction_window_unit': 'HOURS', 'max_threshold': '32', 'min_threshold': '4'}
     and default_time_to_live = 172800
     and speculative_retry = 'NEVER'
     and gc_grace_seconds = 10800;

-- tagIndex
INSERT INTO tag_index(trace_id, span_id, service_name, start_time, tag_key, tag_value);

-- queryByTag
SELECT trace_id FROM tag_index
    WHERE service_name = ? AND tag_key = ? AND tag_value = ? and start_time > ? and start_time < ?
    ORDER BY start_time DESC LIMIT ?;

--------------------------------------------------------------------------------

create table jaeger.traces
(
    trace_id       blob,
    span_id        bigint,
    span_hash      bigint,
    duration       bigint,
    flags          int,
    logs           list<frozen<log>>,
    operation_name text,
    parent_id      bigint,
    process        frozen<process>,
    refs           list<frozen<span_ref>>,
    start_time     bigint,
    tags           list<frozen<keyvalue>>,
    primary key (trace_id, span_id, span_hash)
)
    with compaction = {'class': 'org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy', 'compaction_window_size': '1', 'compaction_window_unit': 'HOURS', 'max_threshold': '32', 'min_threshold': '4'}
     and default_time_to_live = 172800
     and speculative_retry = 'NEVER'
     and gc_grace_seconds = 10800;

-- insertSpan
INSERT INTO traces(trace_id, span_id, span_hash, parent_id, operation_name, flags, start_time, duration, tags, logs, refs, process)

-- querySpanByTraceID
SELECT trace_id, span_id, parent_id, operation_name, flags, start_time, duration, tags, logs, refs, process
    FROM traces WHERE trace_id = ?;

--------------------------------------------------------------------------------

create table jaeger.service_name_index
(
    service_name text,
    bucket       int,
    start_time   bigint,
    trace_id     blob,
    primary key ((service_name, bucket), start_time)
)
    with clustering order by (start_time desc)
     and compaction = {'class': 'org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy', 'compaction_window_size': '1', 'compaction_window_unit': 'HOURS', 'max_threshold': '32', 'min_threshold': '4'}
     and default_time_to_live = 172800
     and speculative_retry = 'NEVER'
     and gc_grace_seconds = 10800;


-- serviceNameIndex
INSERT INTO service_name_index(service_name, bucket, start_time, trace_id);

-- queryByServiceName
SELECT trace_id FROM service_name_index
    WHERE bucket IN (bucketRange) AND service_name = ? AND start_time > ? AND start_time < ?
    ORDER BY start_time DESC LIMIT ?;

--------------------------------------------------------------------------------

create table jaeger.service_operation_index
(
    service_name   text,
    operation_name text,
    start_time     bigint,
    trace_id       blob,
    primary key ((service_name, operation_name), start_time)
)
    with clustering order by (start_time desc)
     and compaction = {'class': 'org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy', 'compaction_window_size': '1', 'compaction_window_unit': 'HOURS', 'max_threshold': '32', 'min_threshold': '4'}
     and default_time_to_live = 172800
     and speculative_retry = 'NEVER'
     and gc_grace_seconds = 10800;


-- serviceOperationIndex
INSERT INTO service_operation_index(service_name, operation_name, start_time, trace_id)

-- queryByServiceAndOperationName
SELECT trace_id FROM service_operation_index
    WHERE service_name = ? AND operation_name = ? AND start_time > ? AND start_time < ?
    ORDER BY start_time DESC LIMIT ?;

--------------------------------------------------------------------------------

create table jaeger.duration_index
(
    service_name   text,
    operation_name text,
    bucket         timestamp,
    duration       bigint,
    start_time     bigint,
    trace_id       blob,
    primary key ((service_name, operation_name, bucket), duration, start_time, trace_id)
)
    with clustering order by (duration desc, start_time desc, trace_id asc)
     and compaction = {'class': 'org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy', 'compaction_window_size': '1', 'compaction_window_unit': 'HOURS', 'max_threshold': '32', 'min_threshold': '4'}
     and default_time_to_live = 172800
     and speculative_retry = 'NEVER'
     and gc_grace_seconds = 10800;


-- durationIndex
INSERT INTO duration_index(service_name, operation_name, bucket, duration, start_time, trace_id)

-- queryByDuration
SELECT trace_id FROM duration_index
    WHERE bucket = ? AND service_name = ? AND operation_name = ? AND duration > ? AND duration < ?
    LIMIT ?;

--------------------------------------------------------------------------------
