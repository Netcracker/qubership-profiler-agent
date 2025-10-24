# Data

Data should be prepared in saved in `data` folder:

- `java` dumps related for Java applications
  - `dumps.td` - caught thread dumps from java application
  - `dumps.top` - caught dumps with `top`
- `dumps.tcp` - caught communication between CDT collector and profiler java agent on `1715` port.
- `go` dumps related for Golang applications
  - `dumps.alloc` - pprof dumps for memory allocations
  - `dumps.goroutine` - pprof dumps for goroutine lifecycle
  - `dumps.heap` - pprof dumps for heap allocations
  - `dumps.profile` - pprof dumps for CPU profiling

Every agent will be assigned to one of available files for each type randomly at start.

Check examples in `doc\examples` folder

## Run Wireshark

- Install [Wireshark](https://www.wireshark.org/download.html)
  - add its folder with binaries `./Wireshark/App/Wireshark` to `PATH`
- add CDT plugin [profiler.lua](..%2Fscripts%2Fwireshark.lua%2Fprofiler.lua) to `./Wireshark\App\Wireshark\plugins`
  - reload WireShark or press`Ctrl+Shift+L` (Menu > Analyze > Reload Lua plugins)
- Open file with tcpdump

> NOTE: recommend to catch `15-30`Mb tcpdump file in (`1m-5m-10m` of work, depending on load) to
> filter `Follow -> TCP Stream`.
>
> Otherwise, WireShark starting work really slow.

## Prepare TCP data

- Filter all tcp packets related to collector-agent communication:
  - `cdt2 and tcp.len > 10` (how to check: should be on `tcp.port`=1715)
- Filter all data related to collector-agent communication:
  - `cdt2 and tcp.len > 10` (to filter ACK, etc.)
- Filter initialization packets (when agent start communication with collector)
  - `cdt2 and cdt.command_id == 0x14` (it means `COMMAND_GET_PROTOCOL_VERSION_V2`)
- To filter all communication between found agent and collector, use `Follow TCP Stream` feature:
  - right-click on found package
  - `Follow -> TCP Stream` (Ctrl+Shift+Alt+T)
  - In modal window select `Show data as: Raw` and `Entire conversation`
  - `Save as` as some file (`test.bin`, for example)

> NOTE: to prepare data for load generator, you MUST prepare binary data with `COMMAND_GET_PROTOCOL_VERSION_V2` command.
>
> Agent sends `COMMAND_GET_PROTOCOL_VERSION_V2` at the start of application, so make sure to restart java service AFTER
> starting gathering tcpdump from collector

## Prepare top/td data

- Filter all tcp packets related to uploading dumps:
  - `http and http.request.method == "PUT"`
- Export interesting dump
  - Select `Menu > File > Export objects > HTTP`
  - Filter by content type `application/octet-stream`
  - Select interesting file, press `Save`
