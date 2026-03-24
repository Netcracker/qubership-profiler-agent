
------------

### Schema

------------

See also original CQL:

> See `initial_schema.cql`

![current schema](../../images/diagrams/schema_current.png)

#### Most expensive tables

write - append
meta

| Table                | 20.06.23    | 03.07.23    | 10.07.23       | comment               |
|----------------------|-------------|-------------|----------------|-----------------------|
| stream_chunks        | 224.398 GiB | 385.082 GiB | 🔺 507.800 GiB | blobs                 |
| stream_dictionary    | 27.614 GiB  | 28.092 GiB  | 🔺 39.594 GiB  | bug with duplicates?  |
| stream_suspend       | 10.763 GiB  | 15.488 GiB  | ⬆️ 18.676GiB   | 2-5/min/service       |
| pod_statistics       | 2.879 GiB   | 4.099 GiB   | ⬆️ 5.974 GiB   |                       |
| stream_registry      | 1.825 GiB   | 2.300 GiB   | ⬆️ 3.645GiB    |                       |
| stream_handles       | 4.856 GiB   | 4.792 GiB   | ▧ 4.792GiB     | not used?             |
| stream_modifications | 18.237 GiB  | 2.878 GiB   | ▧ 2.878GiB     | ?? no inserts in code |

![img.png](../../images/diagrams/schema_current_num.png)

* bug with dictionary:
  * pod_id (good) ===  1900-2300 (unique "com.netcracke.romc" 1k)
  * pod_id (bad)  ===  700,000 instead of 2000 (because of collector's OOM restarts)

------------

### Estimates

------------

* Dictionary size:
  * from TOMS - `new MethodDictionary(10000)` (seems, like `10000` is max)
  * Should expect `1000-2000` per microservice

#### CPU and Memory

For huge environments with `500+` pods, you can use the following hardware resources: *TBD...*

| Type       | Ns | Services | Pods | Retention | Restart | Meta   | Max/p   | Dumps | avg | max   |
|------------|----|----------|------|-----------|---------|--------|---------|-------|-----|-------|
| tiny       | 1  | 5        | 1    | 1d        | 0.1     | `10Mb` | `200Mb` | 1/p/m |     |       |
| dev-team   | 1  | 30       | 2    | 5d        | 0.9     | `20Mb` | `200Mb` | 2/p/m |     |       |
| prod       | 3  | 100      | 3    | 14d       | 0.2     | `30Mb` | `200Mb` | 1/p/m |     |       |
| dev-shared | 20 | 60       | 2    | 3d        | 0.5     | `20Mb` | `200Mb` | 1/p/m |     |       |

* `Meta/p` = Meta per pod
* `Dumps` = Freq of downloading dumps (per minute per pod)
* `Max/p` = `LOG_MAX_SIZE_KB` (`200 Mb`) CDT allow to accumulate per pod
  * `Restarts` (`<restart factor>`) = count of pods restarts for a retention period
  
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

------------

### Experiments


#### k6 results

##### I. Available services

Response: json

```json
{
  "profiler": [
    "esc-collector-service",
    "esc-test-service",
    "esc-ui-service"
  ]
}
```

* Old schema (retrieve all, group by on backend)

    ```sql
    SELECT * FROM pod_details
    ```
  
  * results:

      ```bash
            scenarios: (100.00%) 1 scenario, 10 max VUs, 40s max duration (incl. graceful stop):
                     * default: 10 looping VUs for 10s (gracefulStop: 30s)
    
               data_received..................: 36 kB  3.2 kB/s
               http_req_blocked...............: avg=2.11ms   min=0s       med=0s       max=13ms    p(90)=11.99ms  p(95)=11.99ms
               http_req_connecting............: avg=312.06µs min=0s       med=0s       max=1.99ms  p(90)=997.1µs  p(95)=1.18ms
               http_req_duration..............: avg=723.78ms min=320.98ms med=413.45ms max=2.1s    p(90)=2.08s    p(95)=2.09s
                 { expected_response:true } 🔴: avg=488.03ms min=320.98ms med=401.7ms  max=1.94s   p(90)=797.42ms p(95)=861.42ms
               http_req_failed..............🔴: 14.75% ✓ 9        ✗ 52
               http_req_receiving.............: avg=171.51µs min=0s       med=0s       max=1.12ms  p(90)=703.6µs  p(95)=990.1µs
               http_req_sending...............: avg=20.99µs  min=0s       med=0s       max=342.3µs p(90)=0s       p(95)=288.6µs
               http_req_tls_handshaking.......: avg=0s       min=0s       med=0s       max=0s      p(90)=0s       p(95)=0s
               http_req_waiting...............: avg=723.58ms min=320.98ms med=413.45ms max=2.1s    p(90)=2.08s    p(95)=2.09s
               http_reqs......................: 61     5.524263/s
               iteration_duration.............: avg=1.73s    min=1.32s    med=1.42s    max=3.13s   p(90)=3.11s    p(95)=3.11s
               iterations.....................: 61     5.524263/s
               vus............................: 1      min=1      max=10
               vus_max........................: 10     min=10     max=10
      ```

      (fails for `10` VU, 100% success for `3` virtual user )
  
* New schema (retrieve one *per partition*, group by on backend)

    ```sql
    SELECT * FROM cdt_v2_pods PER PARTITION LIMIT 1
    ```
  
  * results:
  
    ```bash
        scenarios: (100.00%) 1 scenario, 10 max VUs, 40s max duration (incl. graceful stop):
                 * default: 10 looping VUs for 10s (gracefulStop: 30s)

           data_received..................: 13 kB  1.2 kB/s
           data_sent......................: 8.4 kB 788 B/s
           http_req_blocked...............: avg=1.37ms   min=0s       med=0s       max=11ms     p(90)=11ms     p(95)=11ms
           http_req_connecting............: avg=124.88µs min=0s       med=0s       max=999.1µs  p(90)=999.1µs  p(95)=999.1µs
           http_req_duration..............: avg=312.58ms min=276.22ms med=289.14ms max=566.03ms p(90)=302.3ms  p(95)=562.4ms
             { expected_response:true } 🟢: avg=312.58ms min=276.22ms med=289.14ms max=566.03ms p(90)=302.3ms  p(95)=562.4ms
           http_req_failed..............🟢: 0.00%  ✓ 0        ✗ 80
           http_req_receiving.............: avg=60.06µs  min=0s       med=0s       max=832.7µs  p(90)=166.35µs p(95)=534.44µs
           http_req_sending...............: avg=0s       min=0s       med=0s       max=0s       p(90)=0s       p(95)=0s
           http_req_tls_handshaking.......: avg=0s       min=0s       med=0s       max=0s       p(90)=0s       p(95)=0s
           http_req_waiting...............: avg=312.52ms min=276.22ms med=289.14ms max=566.03ms p(90)=302.28ms p(95)=562.4ms
           http_reqs......................: 80     7.505323/s
           iteration_duration.............: avg=1.32s    min=1.27s    med=1.29s    max=1.58s    p(90)=1.31s    p(95)=1.58s
           iterations.....................: 80     7.505323/s
           vus............................: 10     min=10     max=10
           vus_max........................: 10     min=10     max=10
    ```

##### II. Active pod statistics

* Request: query `dateFrom=1688567785973 & dateTo=1688568685973 & podFilter = json`
  * podFilter:
  
    ```json
        {"operation":"or","conditions":[
          {"operation":"and","conditions":[{"lValue":{"word":"namespace"},"comparator":"=","rValues":[{"word":"ndo-dev-2"}]}]},
          {"operation":"and","conditions":[{"lValue":{"word":"namespace"},"comparator":"=","rValues":[{"word":"ndo-dev-ndo-eso"}]}]},
          {"operation":"and","conditions":[{"lValue":{"word":"pod_name"},"comparator":"=","rValues":[{"word":"optical-manager-v1-b7897954b-vvkz8_1682740519471"}]},{"lValue":{"word":"service_name"},"comparator":"=","rValues":[{"word":"optical-manager"}]},{"lValue":{"word":"namespace"},"comparator":"=","rValues":[{"word":"ndo-dev-3"}]}]},
          {"operation":"and","conditions":[{"lValue":{"word":"pod_name"},"comparator":"=","rValues":[{"word":"optical-manager-v1-76d4b858d4-z55s7_1684555050614"}]},{"lValue":{"word":"service_name"},"comparator":"=","rValues":[{"word":"optical-manager"}]},{"lValue":{"word":"namespace"},"comparator":"=","rValues":[{"word":"ndo-dev-3"}]}]}
        ]}
    ```

* Response: list of found pods with statistics

    ```json
    [{
        "namespace": "profiler", "serviceName": "esc-ui-service", "podName": "esc-ui-service-7b9c679fbf-z6vbm_1687155400163",
        "activeSinceMillis": 1688564398533, "firstSampleMillis": 1688567820000, "lastSampleMillis": 1688568660000,
        "dataAtStart": 0, "dataAtEnd": 197107734, "currentBitrate": 0.02067,
        "hasGC": true, "hasTops": true, "hasTD": true, "onlineNow": true, "heapDumps": []
    }]
    ```

------------

* Old schema (`/esc/listActivePODs` in code)
  * find list of active pods by filter in timerange
    * `select pod_name, service_name, namespace, pod_info, rc_info, dc_info from pod_details`
    * `select * from active_pods where active_during_hour = ?`, **hours**
  * find statistics for pods in timerange  **why all records?!**
    * `select * from pod_statistics where pod_name = ? and stream_name = ? and cur_date >= ? and cur_date <= ?`
    * group by and accumulate on backend
  * enrich(?!) found statistics:
    * `select pod_name, data_accumulated from stream_registry where pod_name = ? allow filtering` !!
    * `select * from pod_details where pod_name = ?` **why?!**
    * `select pod_name from pod_active_time where pod_name = ? and last_active > ? allow filtering` !!  **why?!**
    * list of heap dumps:
      * `select rolling_sequence_id, create_when, modified_when from stream_registry
      where modified_when >= ? and create_when <= ? and pod_name = ? and stream_name = ? allow filtering` !!
      * `select * from stream_registry where pod_name = ? and stream_name = ? and rolling_sequence_id = ?`

------------

* test runs (**without** calls to `stream_registry`):

  ```bash
    scenarios: (100.00%) 1 scenario, 3 max VUs, 40s max duration (incl. graceful stop):
             * default: 3 looping VUs for 10s (gracefulStop: 30s)
       data_received..................: 12 kB  1.1 kB/s
       data_sent......................: 7.0 kB 642 B/s
       http_req_blocked...............: avg=1.59ms  min=0s       med=0s       max=7.99ms  p(90)=7.99ms  p(95)=7.99ms
       http_req_connecting............: avg=199.1µs min=0s       med=0s       max=995.5µs p(90)=995.5µs p(95)=995.5µs
       http_req_duration..............: avg=1.14s   min=632.87ms med=980.69ms max=2.1s    p(90)=1.97s   p(95)=2.09s
         { expected_response:true } 🟠: avg=1.14s   min=632.87ms med=980.69ms max=2.1s    p(90)=1.97s   p(95)=2.09s
       http_req_failed..............🟢: 0.00%  ✓ 0        ✗ 15
       http_req_receiving.............: avg=64.56µs min=0s       med=0s       max=968.5µs p(90)=0s      p(95)=290.54µs
       http_req_sending...............: avg=34.62µs min=0s       med=0s       max=519.4µs p(90)=0s      p(95)=155.81µs
       http_req_tls_handshaking.......: avg=0s      min=0s       med=0s       max=0s      p(90)=0s      p(95)=0s
       http_req_waiting...............: avg=1.14s   min=632.87ms med=980.69ms max=2.1s    p(90)=1.97s   p(95)=2.09s
       http_reqs......................: 15     1.368009/s
       iteration_duration.............: avg=2.15s   min=1.63s    med=1.99s    max=3.11s   p(90)=2.99s   p(95)=3.11s
       iterations.....................: 15     1.368009/s
       vus............................: 3      min=3      max=3
       vus_max........................: 3      min=3      max=3
  ```

------------

* New schema
  * find list of active pods
    * option A
      1. find list of active pods (*can be cached*)

         ```sql
            SELECT * FROM cdt_v2_pods
         ```

      2. filter by names and active_time (on backend)
    * option B (*less data transferring for often pod restarts?*)
      1. find list of active pods by filter

         ```sql
          SELECT * FROM cdt_v2_pods WHERE namespace IN ? AND service_name IN ? AND start_time <= :end_range ALLOW FILTERING
         ```

      2. filter by active_time (on backend)
  * find statistics for pods in timerange
    * option A - list

      ```sql
          SELECT * FROM cdt_v2_pod_statistics WHERE date=? AND namespace = ? and service = ? and pod_name = ? and cur_date >= ? and cur_date <= ?
      ```

    * option B -- only **first** and **latest** in timerange for each pod

      ```sql
           SELECT * FROM cdt_v2_pod_statistics where date=? AND namespace = ? and service = ? and pod_name = ? and cur_date >= ? ORDER BY cur_date ASC PER PARTITION LIMIT 1;
      ```

      ```sql
           SELECT * FROM cdt_v2_pod_statistics where date=? AND namespace = ? and service = ? and pod_name = ? and cur_date <= ? ORDER BY cur_date DESC PER PARTITION LIMIT 1;
      ```

------------

* test runs:

  ```bash
  scenarios: (100.00%) 1 scenario, 3 max VUs, 40s max duration (incl. graceful stop):
           * default: 3 looping VUs for 10s (gracefulStop: 30s)
     data_received..................: 8.9 kB 778 B/s
     data_sent......................: 9.3 kB 817 B/s
     http_req_blocked...............: avg=1.14ms   min=0s       med=0s       max=7.99ms   p(90)=7.99ms   p(95)=7.99ms
     http_req_connecting............: avg=142.92µs min=0s       med=0s       max=1ms      p(90)=1ms      p(95)=1ms
     http_req_duration..............: avg=613.13ms min=571.31ms med=588.38ms max=856.01ms p(90)=617.14ms p(95)=853.64ms
       { expected_response:true } 🔵: avg=613.13ms min=571.31ms med=588.38ms max=856.01ms p(90)=617.14ms p(95)=853.64ms
     http_req_failed..............🟢: 0.00%  ✓ 0        ✗ 21
     http_req_receiving.............: avg=268.76µs min=0s       med=0s       max=1.08ms   p(90)=851.8µs  p(95)=1ms
     http_req_sending...............: avg=33.41µs  min=0s       med=0s       max=641.5µs  p(90)=0s       p(95)=60.2µs
     http_req_tls_handshaking.......: avg=0s       min=0s       med=0s       max=0s       p(90)=0s       p(95)=0s
     http_req_waiting...............: avg=612.83ms min=571.31ms med=588.38ms max=855.15ms p(90)=616.58ms p(95)=852.86ms
     http_reqs......................: 21     1.835375/s
     iteration_duration.............: avg=1.62s    min=1.57s    med=1.59s    max=1.86s    p(90)=1.62s    p(95)=1.86s
     iterations.....................: 21     1.835375/s
     vus............................: 3      min=3      max=3
     vus_max........................: 3      min=3      max=3
  ```
