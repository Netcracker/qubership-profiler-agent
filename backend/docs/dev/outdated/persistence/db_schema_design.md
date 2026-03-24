
### Design

---

#### Conceptual data modeling

![conceptual schema](../../images/diagrams/schema_conceptual.png)

#### Logical data modeling

- microservices
  - **Namespace** `[1]---[M]` **Microservices**
  - **Microservices** `[1]---[M]` **PodD** (deployment)
  - **PodD** `[1]---[M]` **PodR** (restarts)
- data
  - dumps
    - **PodR**  `[1]---[M]` **Dumps** uploaded dumps (by types and timestamp)
    - **Dumps**  `[1]---[M]` **Chunk** chunks of dump
  - metadata
    - **PodR**  `[1]---[M]` **Tags** (tags, headers, trace-id?, etc)
    - **PodR**  `[1]---[M]` **Literals** (lines of code, etc)
- statistics
  - **PodR**  `[1]---[M]` **PodStat** upload dump statistics (by minutes + by types)

![logical schema](../../images/diagrams/schema_logical.png)

#### Assumptions

Based on best practices for Cassandra we want to declare some assumptions / goals which we want to reach:

- For select calls we want to use one query to one table
- Collector-service execute only write in Cassandra (except cleanup)
- UI service should show in UI the number of calls limited by `1500`/`3000`/other value
(with hard limit of subqueries `10000`?)

#### Suggestion

0. use cache for active pods
   - in UI: list of pods/services + data (last_active, meta: tags, literals)
   - in collector:
     - get rid of reads, only writes
     - specialized cache warming on start (for last known active pods)
1. keep metadata (tags, literals) for a pod in one row
   - `UPDATE` in CQL with [maps](https://docs.datastax.com/en/cql-oss/3.x/cql/cql_using/useInsertMap.html)
     - `SET params = params + {'param' : '%json%'}`
     - `SET literal = literal + {'position' : 'text'}`
   - Pro:
     - load meta for pod in one query
     - can cache in `collector`
     - after first upload for pod - seldom updates (with new keys)
   - Con:
     - several updates for same row (_tombstones?_)

- keep pod statistics with `date` in partition (and `TimeWindowCompactionStrategy`)
- keep pod statistics as aggregated by types
- updates pod statistics and suspends with `granularity=1m` (background jobs)
- use `Map` and `SET` for pod statistics and meta
  - `UPDATE`s in hot time: last 1-5 min, should be got in `MemTable`
  - often `SELECT`s from UI

#### Query types

Types of writes:

- Type `W.A`: append only
- Type `W.B`: a batch of updates in hot time (for 2-3 min), no updates after that
- Type `W.C`: frequent updates during pod live

Types of read:

- Type `R.A`: download data for selected pods (by a partition key)
- Type `R.B`: scan for selected pods (by partition key**S**)
- Type `R.C`: frequent scan (current state of system)

#### Queries modeling

- List of active pods (`R.C`)

![active pods](../../images/diagrams/active_pods.png)

- Statistics for selected services in timerange (`R.B`)

![pod statistics](../../images/diagrams/pod_stats.png)

- Download dumps for selected services (`R.B`)

![pod dumps](../../images/diagrams/pod_dumps.png)

- Filtered list of calls (for selected services, in timerange) (several `R.B` with a lot of results)
  - with additional **filtering** and **sorting** on backend/UI

![calls list](../../images/diagrams/calls_list.png)

- Expanded tree for selected call  (several `R.A`)

![call tree](../../images/diagrams/call_tree.png)

### Estimates

---

- Dictionary size:
  - from TOMS - `new MethodDictionary(10000)`
  - Should expect `1000-2000` per microservice

#### CPU and Memory

For huge environments with `500+` pods, you can use the following hardware resources:

| Type       | Ns | Services | Pods | Retention | Restart | Meta   | Max/p   | Dumps | avg | max   |
|------------|----|----------|------|-----------|---------|--------|---------|-------|-----|-------|
| tiny       | 1  | 5        | 1    | 1d        | 0.1     | `10Mb` | `200Mb` | 1/p/m |     |       |
| dev-team   | 1  | 30       | 2    | 5d        | 0.9     | `20Mb` | `200Mb` | 2/p/m |     |       |
| prod       | 3  | 100      | 3    | 14d       | 0.2     | `30Mb` | `200Mb` | 1/p/m |     |       |
| dev-shared | 20 | 60       | 2    | 3d        | 0.5     | `20Mb` | `200Mb` | 1/p/m |     |       |

- `Meta/p` = Meta per pod
- `Dumps` = Freq of downloading dumps (per minute per pod)
- `Max/p` = `LOG_MAX_SIZE_KB` (`200 Mb`) CDT allow to accumulate per pod
  - `Restarts` (`<restart factor>`) = count of pods restarts for a retention period
  
      ```bash
      <restart factor> = max(1, <retention period> / <the average pod time life> )
      ```

    ```bash
    Small development cloud:
      50 pods * 200 Mb * 14 restarts ~= 136 Gb, for 2 weeks
    Big development cloud:
      50 pods * 200 Mb * 14 restarts ~= 1 Tb, max size for 2 weeks
      50 pods * 20 Mb * 14 restarts ~= 100 Gb, average size for 2 weeks
    Production cloud:
    200 pods * 200 Mb * 1 restart ~= 40 Gb, for 2 weeks
    ```

NDO: 20 namespace * 70 service * 2 HA = uniq 300 pods

---

#### NFR

| entity    | min | max     | description                            |
|-----------|-----|---------|----------------------------------------|
| namespace | 1   | 200     |                                        |
| service   | 10  | 4000    | `~70` per NS                           |
| pod       | 3   | 280000  | `1-5-70` redeploy per service in month |
| restarts  | 1   | 3250000 | `1-60000` restarts per service in week |

---
