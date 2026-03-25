
## Test Plan

### Regression tests

#### Functionality

##### Collector2

* use env variable from old deployment (ESC collector)
  * `rolling_period` parameter (for java agents) updated : `1h` => `5m`
* parses and persists data correctly (backward compatibility: no errors and reconnect)
* all data persisted to new tables, no new data in old
* there are no frequent reconnects (because of collector)
* Grafana dashboards present:
  * how many agents, how much data were accumulated, how many DB requests
* no errors and exception in logs
* (TBD?) installation file with agent version downloaded by link

##### UI*Service-2

* UI v2 ??!
* both OLD and NEW data presented in UI
  * (TBD, for test: how to understand which is one, by `_ts` suffix?)
  * Calls List
    * can filter by services, timerange & duration
    * backend can sort by columns (except title)
    * can click and open links from rows (to Call Tree)
    * response in 1-5 s
      * at least partial with message (TBD: progress bar?)
      * message if too many calls found (present only first 10000)
      * message if too many raw data and search was not finished in 25 sec (TBD)
      * refreshing the same search (change of sorting, etc.) shows results from cache
        * with updated data if last search was in progress
    * should not fail if search was for BIG timerange and BIG amount of services (just message)
    * clear error messages on UI (in case 500 or gateway timeout)
  * Pods Info
    * can download several dumps in one batch request
      * TBD: MANY dumps - gateway timeout?
    * can see and download pprof dumps too (for OpenSearch)
  * Heap dumps (separate tab)
    * can download one dump (by link)
    * can delete the selected dump too
  * Call Tree
    * can see дерево показывается (direct + reverse - hotspot)
    * TBD: no Gantt / database tabs (no data in CDT)
    * TBD: Download page as single page archive
    * TBD: Can open merged tree for several selected calls
* Grafana dashboards present:
  * how much data were request, how much data were downloaded, how many DB requests
* no errors and exception in logs

##### Java agent

* Collector2 parses and persists data correctly (backward compatibility)
  * no errors in agent's logs
  * no reconnects
  * Java agents got new `rolling_period` from collector (should be `5m` instead of `1h`)
* Different java apps can be profiled:
  * Spring Boot 2-3, Quarkus 2-3
  * Java [TOMS: 7, 8, ] 11, 17, [future: 21] LTS
  * webservice: netty / jetty / weblogic

##### pprof-collector

* can retrieve data from all Go services in namespace
* can get pprof data on regular basis (according to config)
* pprof dumps are persisted to separated tables (from java agents)
  * including statistics (`data_accumulated`)

##### Static-Service

* TBD: to delete?!

##### Test-Service

* TBD: should enable main `/launch` endpoint for complex test run
  * problems: it clears DB during test (Collector2 doesn't support it now)
