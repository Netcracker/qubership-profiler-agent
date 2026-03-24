# Preparing Data

## Table of Content

<!-- TOC -->
* [Preparing Data](#preparing-data)
  * [Table of Content](#table-of-content)
  * [Used folders](#used-folders)
  * [Run Wireshark](#run-wireshark)
  * [Prepare TCP data](#prepare-tcp-data)
  * [Prepare top/td dumps](#prepare-toptd-dumps)
<!-- TOC -->

## Used folders

The prepared data must be saved in the ‘data’ folder:

* `dumps.td`  - caught thread dumps from java application
* `dumps.top` - caught dumps with `top`
* `dumps.tcp` - caught communication between profiler's collector and java agent on `1715` port.

Every agent will be assigned to one of available files for each type randomly at start.

## Run Wireshark

* Install Wireshark ( <https://www.wireshark.org/download.html> )
  * add its folder with binaries `./Wireshark/App/Wireshark` to `PATH`
* Install CDT plugin [profiler.lua](../scripts/wireshark.lua/profiler.lua) to `./Wireshark\App\Wireshark\plugins`
  * reload WireShark or press`Ctrl+Shift+L` (Menu > Analyze > Reload Lua plugins)
* Open the collected .pcapng file with Wireshark

## Prepare TCP data

1. Filter out all `COMMAND_GET_PROTOCOL_VERSION_V2` commands:
   * `cdt2 and cdt.command_id == 0x14`

2. Find the packet from the service you want to collect the dump from.
   * Click on the packages, then expand the
   "CDT Profiler Protocol Data" item and look for the microservice field.

3. To filter all communication between this agent and collector, use `Follow TCP Stream` feature:
   * Filter all tcp packets related to collector-agent communication:
     * `cdt2 and tcp.len > 10` (how to check: should be on `tcp.port`=1715)
   * Filter all data related to collector-agent communication:
     * `cdt2 and tcp.len > 10` (to filter ACK, etc.)
   * Filter initialization packets (when agent start communication with collector)
     * `cdt2 and cdt.command_id == 0x14` (it means `COMMAND_GET_PROTOCOL_VERSION_V2`)
   * To filter all communication between found agent and collector, use `Follow TCP Stream` feature:
     * Right-click on found packet
     * `Follow -> TCP Stream` (Ctrl+Shift+Alt+T)
     * In modal window select `Show data as: Raw` and `Entire conversation`
     * `Save as` as some file (`tcp-dump.protocol`, for example)

4. Move created tcp dump to `data/dumps.tcp` and rebuild the load generator
so that the new dump is used in tests

## Prepare top/td dumps

1. Filter all tcp packets related to uploading dumps:
   * `http and http.request.method == "PUT"`

2. Export interesting dump
   * Select `Menu > File > Export objects > HTTP`
   * Filter by content type `application/octet-stream`
   * Select interesting file, press `Save`
