## CDT load generator

* K6 scripts and compiled k6-module to emulate load
  for [Cloud Diagnostic Toolset](https://github.com/Netcracker/qubership-profiler-backend)
* CLI tool to generate calls and dumps in specified time range as Parquet files and upload them to S3-compatible storage

### Content

* `k6` javascript files to emulate profiler load from N java applications
    * `scripts/scenario.js` - main file for emulation should
        * `scripts/scenario.dumps.js`
            * send `top`/`thread dump`/ ... dumps every minute for each emulated agent
        * `scripts/scenario.tcp.js`
            * emulate java agent communication (sending calls) by TCP connection
        * `scripts/common.js` - settings for load
            * can be overridden from env variables
            * it was tested for N=`200-1000`, but should work even for higher values
* `k6` module to make writing js scripts more convenient
    * load pre-captured load data at begin
    * share it between emulated "agents" and randomly assign load from available options
    * thread-safe
* `Lua` scripts for `WireShark` to make it easier to capture TCP/CDT communication between agents and the collector

HWe:

* Current CPU load: `<1vCPU` for `800` emulated agents
* Current memory footprint: `~1.5-2Mb` per emulated agent

### Usage

#### Gathering data

See `doc/gathering_data.md` for information about catching actual services communication with `tcpdump`

See `doc/preparing_data.md` for information about extracting load generator data from tcp dumps

#### Usage

See `doc/usage.md` for more information about scenarios

See `doc/settings.md` for test run parameters

#### Build

See `doc/build.md` for more information

---

> NOTE:  
> When writing k6 tests, keep in mind that HTTP functions such as `http.put` require an `ArrayBuffer` in the `body` parameter when transferring files as a sequence of bytes. Since Td and Top dump data is stored as a byte array, you should first convert it into an `ArrayBuffer` (e.g., using `Uint8Array`) before sending it.  
>
> Example:  
>
> Instead of:  
> ```go
> const resp = http.put(url, pod.dumps.td.data, uploadDumpRequestParams);
> ```
>
> Write this:  
> ```go
> const arrayBuffer = new Uint8Array(pod.dumps.td.data);
> const resp = http.put(url, arrayBuffer, uploadDumpRequestParams);
> ```
