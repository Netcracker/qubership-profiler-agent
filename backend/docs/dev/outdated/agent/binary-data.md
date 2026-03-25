# Table of Content

- [Table of Content](#table-of-content)
  - [Overview](#overview)
  - [Common](#common-notes)
  - [Dictionary](#dictionary)
    - [Binary description](#binary-description-of-dictionary)
    - [Example of Binary file](#example-of-binary-file-for-dictionary)
    - [Extracted value](#extracted-dictionary-value)
  - [Param](#param)
    - [Binary Representation](#binary-representation-of-param)
    - [Example of Binary file](#example-of-binary-file-for-param)
    - [Extracted value](#extracted-param-value)
  - [Suspend](#suspend)
    - [Binary Representation](#binary-representation-of-suspend)
    - [Example of Binary file](#example-of-binary-file-for-suspend)
    - [Extracted value](#extracted-suspend-value)
  - [Call](#call)
    - [Binary Representation](#binary-representation-of-call)
    - [Example of Binary file](#example-of-binary-file-for-call)
  - [Trace](#trace)
    - [Binary Representation](#binary-representation-of-trace)
    - [Example of Binary file](#example-of-binary-file-for-trace)
    - [Extracted value](#extracted-trace-value)

## Overview

This document provides information about the binary representation of data structures used to store profiler data.<br>
These data strutures are used in all java-related parts of profiler (an agent, a collector service and ui-service).

the following are major data structures:

- Dictionary
- Suspend
- Param
- Call
- Trace

## Common notes

- Data structure are generally stream of values of different types
  - `byte` (will be presented as only `1` byte)
  - `bool` (`1` byte)
  - `int` (`4` bytes)
  - `long` (`8` bytes)
  - `uuid` (`16` bytes)
  - `varint` (can take `1`-`4` bytes depending on actual value)
  - `string` (or `varstring`) -- presented as `varint` (=length of string) following by `2*length` bytes.
    <br> _(so every letter in string presented by `2` bytes)_

- Raw stream for `dictionary`, `param` and `suspend` divided in blocks named `phrase`
  (just for transferring reason, no actual value)

- `timestamp` is time in `UTC` presented as unix-time (`long`)

## Dictionary

### Binary description of Dictionary

![dictionary.png](../../images/entities/dictionary.png)

`Dictionary` consist of keys and values where the position of the value in binary file is
considered as key (`id`) and value is a string data.

### Example of Binary file for Dictionary

![dict_example.png](../../images/entities/dict_example.png)

### Extracted Dictionary value

| Id | Offset | Dictionary value             | Phrase |
|----|--------|------------------------------|--------|
| 0  | 4      | call.info                    | 1      |
| 1  | 24     | call.red                     | 1      |
| .. | ...    | ........                     |
| 94 | 10138  | java.lang.String com.......  | 2      |
| 95 | 10494  | com.netcracker.profiler .... | 2      |

- `Id` will be used later (in `calls` and `traces`) instead of actual string
- `Offset` -- offset in bytes from the beginning of stream/file _(for illustration purposes only)_
- `Dictionary value` -- actual captured string from the CDT agent
- `Phrase` -- number of block from incoming stream/file  _(for illustration purposes only)_

## Param

### Binary Representation of Param

![params.png](../../images/entities/params.png)

Param consist of parameters stored in a following order:

- parameter name (`varstring`)
- is parameter indexed (`bool`)
- is parameter is enumerated (`bool`)
- parameter order (`varint`)
- signature (`varstring`)

### Example of Binary file for Param

![param_example.png](../../images/entities/param_example.png)

### Extracted Param value

| Offset | Name                | pIndex | pList | pOrder | Signature | Phrase |
|--------|---------------------|--------|-------|--------|-----------|--------|
| 5      | exception           | false  | true  | 100    | null      | 1      |
| 29     | tmus.transaction.id | true   | true  | 100    | null      | 1      |

- `Offset` -- offset in bytes from the beginning of stream/file _(for illustration purposes only)_
- `Name` -- actual captured string from the CDT agent
- `pIndex` -- should be that parameter be indexed by storage? _(don't used now)_
- `pList` -- does this parameter has one or several values?
- `pOrder` -- order to distinguish different parameters with different priority/importance
- `Signature` -- additional signature if any (for parameters related to methods)
- `Phrase` -- number of block from incoming stream/file  _(for illustration purposes only)_

## Suspend

### Binary Representation of Suspend

![suspend.png](../../images/entities/suspend.png)

Suspend consist of captured delays stored in a following order:

- suspend delay (`varint`) -- actual captured delay (in ms)
- suspend delta (`varint`) -- delta in ms (added to the previous _timestamp_ to get exact timestamp)

For the first record `start_time` is considered as _previous timestamp_.

### Example of Binary file for Suspend

![suspend_example.png](../../images/entities/suspend_example.png)

### Extracted Suspend value

| Pos | Offset | Timestamp     | Delta | Delay    | Phrase |
|-----|--------|---------------|-------|----------|--------|
| 1   | 12     | 1690201577743 | 86    | 67       | 1      |
| 2   | 14     | 1690201577843 | 100   | 64       | 1      |

- `Pos` -- position in stream _(for illustration purposes only)_
- `Offset` -- offset in bytes from the beginning of stream/file _(for illustration purposes only)_
- `Timestamp` -- Actual timestamp of delay (unix-time in UTC) _(for illustration purposes only)_
- `Delta` -- Delta (in ms) from previous timestamp
- `Delay` -- Actual captured delay (in ms)
- `Phrase` -- number of block from incoming stream/file  _(for illustration purposes only)_

## Call

### Binary Representation of Call

There were several formats for call representation.
<br> Right now latest one, which Cloud Profiler used, is `4`.

![calls.png](../../images/entities/calls.png)

Call consist of parameters stored in a following order:

| Parameter         | type       | size      | file format | Description                                     |
|-------------------|------------|-----------|-------------|-------------------------------------------------|
| time              | var int    | 1-4 bytes | 1, 2, 3, 4  | Offset for timestamp                            |
| method            | var int    | 1-4 bytes | 1, 2, 3, 4  | Id of method name in dictionary                 |
| duration          | var int    | 1-4 bytes | 1, 2, 3, 4  | Duration of method (in ms)                      |
| Calls             | var int    | 1-4 bytes | 1, 2, 3, 4  | Count of internal calls                         |
| threadIndex       | var int    | 1-4 bytes | 1, 2, 3, 4  | thread id                                       |
| threadNames       | var string | variable  | 1, 2, 3, 4  | thread name (only if not already parsed before) |
| LogsWritten       | var int    | 1-4 bytes | 1, 2, 3, 4  | bytes written by method                         |
| LogsGenerated     | var int    | 1-4 bytes | 1, 2, 3, 4  | bytes generated by method                       |
| TraceFileIndex    | var int    | 1-4 bytes | 1, 2, 3, 4  | Id of trace file for the call tree              |
| BufferOffset      | var int    | 1-4 bytes | 1, 2, 3, 4  | Byte Offset in trace file for record block      |
| RecordIndex       | var int    | 1-4 bytes | 1, 2, 3, 4  | Actual record id in record block for call       |
| CpuTime           | var long   | 1-8 bytes | 2, 3, 4     | actual time for work (in ms)                    |
| WaitTime          | var long   | 1-8 bytes | 2, 3, 4     | common wait (in ms)                             |
| MemoryUsed        | var long   | 1-8 bytes | 2, 3, 4     | memory used by method (in bytes)                |
| FileRead          | var long   | 1-8 bytes | 3, 4        | disk bytes read by method                       |
| FileWritten       | var long   | 1-8 bytes | 3, 4        | disk bytes written by method                    |
| NetRead           | var long   | 1-8 bytes | 3, 4        | I/O read by method (in bytes)                   |
| NetWritten        | var long   | 1-8 bytes | 3, 4        | I/O write by method (in bytes)                  |
| Transactions      | var int    | 1-4 bytes | 4           | count of DB transactions                        |
| QueueWaitDuration | var int    | 1-4 bytes | 4           | wait in queue (in ms)                           |
| _parameters_      | _struct_   | ? bytes   | 4           | _see below_                                     |

- `parameters` are multiple chunks of parameter values:

  | Parameter     | type       | size       | format |
  |---------------|------------|------------|--------|
  | nparams       | var int    | 1-4 bytes  | 4      |
  | `paramId`     | var int    | 1-4 bytes  | 4      |
  | `paramsCount` | var int    | 1-4 bytes  | 4      |
  | `paramNValue` | varstring  | variable   | 4      |

- `nparams` -- count of captured parameters for the call
- it follows by `nparams` structs:
  - `paramId` -- id parameter from `params` table
  - `paramCount` -- count of values for the parameter (could be `0`, `1` or greater)
  - it follows by `paramCount` of actual values:
    - `paramNValue` -- N-th value of parameter as `varstring` (i.e. `varint` for length + actual data string)

### Example of Binary file for Call

![call_example.png](../../images/entities/call_example.png)

- start_time of `1691167328395` is `04.08.2023 16:42:08.395 GMT`

| time offset | ts              | human          | method   | duration | calls | threadIndex | threadNames        | logsWritten | logsGenerated | traceFileIndex | bufferOffset | recordIndex | cpuTime | waitTime | memoryUsed | fileRead | fileWritten | netRead | netWritten | transactions | queueWaitDuration | nparams |
|-------------|-----------------|----------------|---------|----------|-------|-------------|--------------------|-------------|---------------|----------------|--------------|-------------|---------|----------|------------|----------|-------------|---------|------------|--------------|-------------------|---------|
| -679        | _1691167327716_ | `16:42:07.716` |  9        | 415      | 4     | 0           | `main`               | 0           | 0             | 1              | 8            | 0           | 1184    | 0        | 0          | 0        | 0           | 0       | 0          | 0            | 0                 | `map[]`   |
| 2908        | _1691167330624_ | `16:42:10.624` |  174      | 1        | 3     | 1           | `background-preinit` | 0           | 0             | 1              | 997          | 0           | 93      | 0        | 0          | 0        | 0           | 0       | 0          | 0            | 0                 | `map[]`   |

## Trace

### Binary Representation of Trace

![trace_example.png](../../images/entities/traces.png)

Trace consist of parameters stored in a following order: threadId, realTime, and chunks of method entries consist
of <br>parameters header, tagId, paramType, value, traceIndex and offset respectively.

### Example of Binary file for Trace

![trace_example.png](../../images/entities/trace_example.png)

### Extracted Trace value

| pos | thread id | real time     | level | header | etime | tag id | value                          | trace index | offset |
|-----|-----------|---------------|-------|--------|-------|--------|--------------------------------|-------------|--------|
| 24  | 1         | 1691167327716 | 0     | 180    | 34    | 9      | -                              | -           | -      |
| 27  | 1         | 1691167327716 | 1     | 180    | 2     | 35     | -                              | -           | -      |
| 30  | 1         | 1691167327716 | 2     | 0      | -     | 33     | -                              | -           | -      |
| 32  | 1         | 1691167327716 | 2     | 13     | -     | -      | -                              | -           | -      |
| 33  | 1         | 1691167327716 | 1     | 1      | -     | -      | -                              | -           | -      |
| 34  | 1         | 1691167327716 | 1     | 136    | 3     | 148    | -                              | -           | -      |
| 37  | 1         | 1691167327716 | 1     | 1      | -     | -      | -                              | -           | -      |
| 38  | 1         | 1691167327716 | 0     | 53     | -     | -      | -                              | -           | -      |
| 39  | 1         | 1691167327716 | 0     | 130    | 7     | 0      | null                           | -           | -      |
| 44  | 1         | 1691167327716 | 0     | 2      | -     | 18     | 1691167327716                  | -           | -      |
| 74  | 1         | 1691167327716 | 0     | 2      | -     | 20     | esc-ui-service-8dd5b49fd-2gr2g | -           | -      |
| 138 | 1         | 1691167327716 | 0     | 2      | -     | 21     | main                           | -           | -      |
| 150 | 1         | 1691167327716 | 0     | 2      | -     | 24     | 1184                           | -           | -      |
| 155 | 1         | 1691167327716 | 0     | 1      | -     | -      | -                              | -           | -      |

```text
threadId=1 real_time=1691167327716
header=180 etime=34 tagId=9
  | -> header=180 etime=2 tagId=35
    | -> header=0 etime= - tagId=33
    | <- header=13 etime= - tagId= -
  | <- header=1 etime= - tagId= -
  | -> header=136 etime=3 tagId=148
  | <- header=1 etime= - tagId= -
header=53 etime= - tagId= -
header=130 etime=7 tagId=0 value=null
header=2 etime= - tagId=18 value=1691167327716
header=2 etime= - tagId=20 value=esc-ui-service-8dd5b49fd-2gr2g
header=2 etime= - tagId=21 value=main
header=2 etime= - tagId=24 value=1184
header=1 etime= - tagId= -
```
