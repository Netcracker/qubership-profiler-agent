This document provide information how to use Web Interface of Cloud Profiler (CDT).

# Table of Content

* [Overview](#overview)
* [Web Interface](#web-interface)
  * [Calls List](#calls-list)
    * [Calls List Columns](#calls-list-columns)
  * [Call Tree](#call-tree)
    * [Call tree modes](#call-tree-modes)
    * [Operations](#operations)
    * [Hotspots](#hotspots)
    * [Parameters](#parameters)
  * [Services](#services)
    * [Thread Dumps](#thread-dumps)
    * [TOP dumps](#top-dumps)
    * [Garbage Collection logs](#garbage-collection-logs)
    * [Heap dumps](#heap-dumps)
      * [VisualVM](#visualvm)
      * [Eclipse Memory Analyzer](#eclipse-memory-analyzer)

# Overview

This document provides information about the Cloud Profiler (CDT) user interface.

Cloud Diagnostic Toolset (CDT) is a component that allows to collect diagnostic information about how microservices
processed some requests or any internal queries, background tasks. It collects the following information:

* Thread dumps, heap dumps, GC logs of Java applications
* Runtime calls details and statistics (for Java applications)
* `pprof`-collected profiles from Golang applications

CDT UI provides a convenient UI tool  filter runtime calls captured during work of Java microservices.

# Web Interface

## Main page

![main page](../images/user_guide/main_page.png)

User interfaces contains three main parts:

* Namespace/services selector in sidebar
* Time range selector and tabs in top of the page
* Main area (depends on the tab)

The sidebar (`Namespaces`) shows all services (grouped by namespaces) who has information in CDT.
User can select one or several namespaces/services to view calls or dumps for applications associated with the specific services.

Available tabs:

* `Calls` (Calls list)
* `Pods Info`
* `Heap Dumps`

For first two tabs (`Calls` and `Pods Info`) it is necessary to select several services
from sidebar to restrict search.
Otherwise, system displays the following message: _"Select the Namespace/Service and Period"_.

## Calls List

The Main page ("Calls List") helps search calls (traces) according to search conditions.

* **Runtime Call** (_trace_) is a log entity containing technical information about business operation’s execution
  statistics (_for example, http requests processed by Java application_).

Calls can be associated in a single chain from multiple sources.
So, if user clicked 10 times on some button (and it causes `10` http request), profiler shows 10 lines in the results.

> **Limitations**:
>
> * Classes from the `com.netcracker.**` packages are profiled by default.
  If it is necessary to profile any other classes, you should change the agent configuration.
> * Profiler's production mode displays only the methods that took more than `0 ms` or contained SQL queries.
> *Production mode saves less information, but it is suitable for production usage*
> * The call tree displays only the methods marked for profiling in the configuration.
  Usually, it means displaying the `com.netcracker.**` methods with no more than 7-10 lines.

The system displays Calls table related on selected items from Namespace tree and Period filtration.
<br> If some of these parameters are changed, the system does not begin
loading the new data into the Calls Overview table until the user clicks `Apply` button.
While the data is being loaded into the system user interface, the system displays a loading indicator

![Calls list](../images/user_guide/calls-list/calls_list.png)

User can provide the following filter conditions:

* **Date time range:** (_mandatory_ parameter) \
  User can select time range between `Last 15 min`, `Last 1H`, `Last 2H` or `Last 4H` or specifying exact start and time
  to view calls for applications made

* **Duration:** \
  User can select or enter expression for the duration (`<=100ms`, for example) to view calls for applications matching
  with the specific duration expression.
* **Custom Filters:** \
  User can provide custom search phrases or keywords to search desired calls. For example, `+/- keyword/phrase`

> **NOTE:**
> Big time ranges and long list of selected microservices can lead to long searches,
> so it recommends to be as precise as it possible.

### Calls List Columns

The system allows the user to do following action with table columns:

* Filter by duration
* Filter by query (parameters)
* Sorting by column (except "Title" and "Pod" columns). By default, the data sorted by Start Timestamp column.
* Show or hide columns, reorder them. Change column width.

![img.png](../images/user_guide/calls-list/columns.png)

<!-- markdownlint-disable line-length -->
| Name             | Example                          | Description                                                                                                                                                                                                                                |
| ---------------- | -------------------------------- |--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Start Timestamp  | `12:55:20.212`                   | Event start time <br> _Note: It is displayed in the time zone of browser_                                                                                                                                                                  |
| Duration         | `5008ms`                         | Specifies total call duration (including cpu, idle and queue time). <br> Duration = `cpu_time + non_cpu_time + queue_wait_time + suspension`. In other words, duration should be close to the wall-clock time.                             |
| CPU Time         | `6ms`                            | Time spent on CPU by the call                                                                                                                                                                                                              |
| Suspension       | `0ms`                            | Specifies the server hang-up duration (time spend for GC, for example). <br> In case this value is very big, you need check for its root cause, whether the recommended parameters are met, see if the server has gone to swap, and so on. |
| Queue wait time  | `0ms`                            | The time the query is waiting in the queue (waiting for processor thread, for example). <br> It is applicable for the HTTP queries only and for other queries, the column value is 0.                                                      |
| Calls            | `54`                             | Calls specifies the total number of java-calls, located inside the call-tree <br> Indicates the number of actions that were required for processing and how thoroughly CDT is configured to work                                           |
| Transactions     | `0`                              | Transactions specifies the number of transactions that accessed the database. <br> If this indicator is more than 10, the probability of an existing problem is very high.                                                                 |
| Disk IO          | `0B`                             | Disk IO rate during the call.                                                                                                                                                                                                              |
| Network IO       | `0B`                             | Network IO rate during the call.                                                                                                                                                                                                           |
| Memory allocated | `0`                              | Total amount of memory allocated during the call.                                                                                                                                                                                          |
| Pod              | `pod-service-77d6_1690630881973` | Name of the Kubernetes pod to which particual call is associated with.                                                                                                                                                                     |
| Title            | `ProfilerAgentConnection.run`    | Description of what happened in the event understandable for user                                                                                                                                                                          |
<!-- markdownlint-enable line-length -->

Additional comments for columns:

* **Start Timestamp** -- CDT UI shows timestamps in the time zone of browser.
 <br> For example, if the server is located in the U.S. and
you open the CDT from Moscow (_which means, the Moscow time zone is configured on your computer_),
you can see the date and time here in the Moscow time zone.
* **Duration** -- value in this column is displayed as a link.
The user is able to navigate to "**Calls Tree**" by clicking on this link.
* **Queue wait time** -- applicable for the HTTP queries only (for other queries, the column value is 0).
<br>An HTTP query is executed in two steps: placing a task to the queue and executing the query.
If there are no free threads, the query can stuck in the queue for a long time.

* **Calls** -- indicates the number of actions that were required for processing and
how thoroughly CDT is configured to work. <br>
Normal values are `1'000` for the blank page and `1'000'000` for a somehow filled page
*(This characteristic is used for evaluating CDT overhead: 1 to 10 mln calls is approximately 1 second of overhead)*

* **Title** -- specifies the description of what happened in the event.
  <br> It displays understandable user information such as addresses, names and hides synthetic information
  such as `process.id`, `jsessionid`, address and so on.

> **Suspension**
> There are cases when time is spent for no apparent reason. For instance:
>
> * cpu starvation (100% cpu utilization, so application just does not get CPU core)
> * hypervisor scheduler not providing vCPU
  (e.g. if a virtual machine exceeded its CPU quantum, so hypervisor takes the VM out of CPU)
> * gc pause (that is java application is doing nothing work-related, but it is waiting for the GC to complete)
> * kernel page defragmentation (that is java application is waiting for OS kernel to defragment the memory pages)
> * whatever else pauses
>
> In those cases, it is required to distinguish if the particular slowness was caused
> by application code and/or by some external "pause" event.
> So `suspension` (aka `gc`) value tells you how much time did the "external pause" take.
>
> Profiler is unable to tell the nature of the pause.
> It cannot tell if the pause was OS and/or hypervisor and/or GC specific.

## Call Tree

Call Tree is a list of call parameters and an underlying hierarchy of java methods invoked successively in the same
execution thread.

![img.png](../images/user_guide/call-tree/call_tree.png)

It shows how the total working time was distributed on the concrete java methods. The call tree
displays only the methods marked for profiling in the configuration, and it shows a full tree reflecting the time
distribution. In simple cases, it is enough to understand the reason for the slow work.

* Explanation of fields:

  ![img_3.png](../images/user_guide/call-tree/tree_items_desc.png)

* Hold `Ctrl` and hover over duration field to get additional statistics about call:

  ![img.png](../images/user_guide/call-tree/hover_duration.png)

* Hold `Ctrl` and hover over class name to see full name and start row number:

  ![img_1.png](../images/user_guide/call-tree/hover_classname.png)

### Call tree modes

Call Tree provide two viewing Modes:

* **Top-down Mode** - the tree is similar to the methods calling order:<br>
   children are methods called by the parent method
* **Bottom-up mode** - the tree is displayed from the branches to the top of the tree <br>
   It means that for each method **"+"** hides not the children, but the parent methods.<br>
   So expanding some node, you can find **where it has been called from**

The top-down mode is easier to understand, but sometimes performance problems are more visible in the bottom-up mode.

In both modes

* Tree nodes are ordered by the work duration:
  * The longer nodes are displayed on the top of the tree
    > Thus, the displayed order of the nodes does not match the actual order of the code execution.

* Part of the tree can be hidden using `Collapse` button. For example, the part of the tree with java-calls chain
  that calls only each other, and does not use much time will be probably hidden

### Operations

* Click on arrow near line to get popup with an operation list:

  ![Call Tree](../images/user_guide/call-tree/operations.png)

* **Get stacktrace:** \
  This provides a representation of a call stack at a certain point in time, with each element representing a method
  invocation. The stack trace contains all invocations from the start of a thread until the point it is generated.

  ![Get stacktrace](../images/user_guide/call-tree/stack_trace.png)

* **Outgoing calls:** \
  When you click this option, a separate tab opens with the method that is called from the chosen one. It shows the
  number of entries of the selected entity the tree has. The results are similar to the Call Tree results.

  ![Outgoing calls](../images/user_guide/call-tree/outgoing_calls.png)

* **Incoming calls:** \
  Allows finding methods, which have called the chosen one. On the Hotspots tab, Incoming calls for all the possible
  methods are displayed.

  ![Incoming calls](../images/user_guide/call-tree/incoming_calls.png)

* **Local hotspots:** \
  It is similar to the Outgoing calls bottom-up view.

  ![Local hotspots](../images/user_guide/call-tree/local_hotspots.png)

* **Adjust Duration:** \
  This enables to artificially adjust the duration of the given method in the profiling results. It enables to predict
  the overall duration for the specific methods. \
  The syntax is a list of strings `<multiplier> <method name>`.

  ![Adjust Duration](../images/user_guide/call-tree/adjust_duration.png)

* **Add Category:** \
  This configured categories for the "bottom-up" mode view. The syntax
  is `<cagerory>.<subcategory>.<subsubcategory>.... <method name>`.

  ![Add Category](../images/user_guide/call-tree/add_category.png)

* **Mark red:** \
  Highlight suspicious calls

  ![Mark red](../images/user_guide/call-tree/mark_red.png)

* **Find Usages:** \
  Enables finding all the places, where the selected method is used.
  The meanings of the parameters are same as in a call tree, but most of them (all except calls) are relative to the
  method, on which `Find usages` been used.

  ![Find Usages](../images/user_guide/call-tree/find_usage.png)

  Description of parameters about the method, on which `Find usages` was opened:

  <!-- markdownlint-disable line-length -->
  | Name               | Description                                                                                       |
  | ------------------ | ------------------------------------------------------------------------------------------------- |
  | `total_time`       | working time of the method (including the called methods, except suspension)                      |
  | `total_suspension` | the server hang-up time during executing the method                                               |
  | `self_time`        | specifies the working time of method excluding the children methods working time.                 |
  | `self_suspension`  | specifies the server hang-up time during executing the method or the non-profiled children method |
  | `invocations`      | the number of method calls, hidden in the children nodes                                          |
  | `calls`            | the number of node method calls                                                                   |
  <!-- markdownlint-enable line-length -->

  Notes:

  * For example, `5 inv 10 calls` means that the node method was called `10` times, which led to `5` calls of the method
  * If value of `self_time` is too high, the method have been executing for a long time or the non-profiled methods
    have been executing, for example, `java.lang.*`

### Hotspots

The Hotspots tab displays exactly the same tree as on Call Tree, but is turned upside down (**top-down** -> **bottom-up**).

![Hotspots](../images/user_guide/call-tree/hotspots.png)

### Parameters

The Parameters tab provides a short summary of a Call Tree page with listing only the key parameters. This is helpful to
easily identify the call.

![Parameters](../images/user_guide/call-tree/parameters.png)

## Services

### Pods Info

`Pods Info` tab allows browsing active pods for selected services.
It also allows to view and download GC Logs, TOP and thread dumps:

![Services](../images/user_guide/services/services.png)

* **Thread dumps:** \
  The snapshot of java thread states collected with 1 min interval in the selected time range.
* **TOP dumps:** \
  `top` command output collected with 1 min interval in selected time range.
* **GC logs:** \
  Garbage Collection logs starting from java process start time

#### Thread Dumps

Thread Dumps are the snapshot of java thread states.
<br> User can download dumps of the pod or service for a selected time range.

![Thread dumps](../images/user_guide/services/thread_dumps.png)

Downloaded zip file will contain different thread dump files generated with an interval of a minute, and filename is UTC
timestamp with `YYYY-MM-DD-T-HH-MM-SS-UTC` format.

![TD File Structure](../images/user_guide/services/thread_file.png)

Example of contents in the thread dump file

```java
2023-08-03 07:58:04
Full thread dump OpenJDK 64-Bit Server VM (17.0.7+7-alpine-r0 mixed mode, sharing):
Threads class SMR info:
_java_thread_list=0x00007f3d841e0ca0, length=71, elements={
0x00007f3d9b978d30, 0x00007f3d9b979390, 0x00007f3d9b979a40, 0x00007f3d8bc8b0d0
}

"Reference Handler" #2 daemon prio=10 os_prio=0 cpu=7.44ms elapsed=15248.42s tid=0x00007f3d9b978d30 nid=0xd0 waiting on condition  [0x00007f3d8be18000]
  java.lang.Thread.State: RUNNABLE
  at java.lang.ref.Reference.waitForReferencePendingList(java.base@17.0.7/Native Method)
  at java.lang.ref.Reference.processPendingReferences(java.base@17.0.7/Reference.java:253)
  at java.lang.ref.Reference$ReferenceHandler.run(java.base@17.0.7/Reference.java:215)

  Locked ownable synchronizers:
  - None

"Finalizer" #3 daemon prio=8 os_prio=0 cpu=0.34ms elapsed=15248.42s tid=0x00007f3d9b979390 nid=0xd1 in Object.wait()  [0x00007f3d8bd97000]
  java.lang.Thread.State: WAITING (on object monitor)
  at java.lang.Object.wait(java.base@17.0.7/Native Method)
  - waiting on <0x00000000b06174b0> (a java.lang.ref.ReferenceQueue$Lock)
  at java.lang.ref.ReferenceQueue.remove(java.base@17.0.7/ReferenceQueue.java:155)
  - locked <0x00000000b06174b0> (a java.lang.ref.ReferenceQueue$Lock)
  at java.lang.ref.ReferenceQueue.remove(java.base@17.0.7/ReferenceQueue.java:176)
  at java.lang.ref.Finalizer$FinalizerThread.run(java.base@17.0.7/Unknown Source)

  Locked ownable synchronizers:
  - None
```

#### TOP dumps

TOP dumps contain information about currently running processes and their parameters like resource consumption,
no of threads in different states, process ID, etc.
<br> User can download dumps of the pod or service for a selected time range.

![TOP dumps](../images/user_guide/services/top_dumps.png)

Downloaded zip file will contain different TOP dumps generated with an interval of a minute, and filename is UTC
timestamp with `YYYY-MM-DD-T-HH-MM-SS-UTC` format.

![TOP File Structure](../images/user_guide/services/top_file.png)

Example of contents in the TOP log file:

```bash
Start collecting CPU usage for PID 200
top - 07:58:04 up 289 days, 15:46,  0 users,  load average: 1.85, 1.95, 2.09
Threads:  74 total,   1 running,  73 sleeping,   0 stopped,   0 zombie
%Cpu(s): 17.0 us,  5.3 sy,  0.0 ni, 76.6 id,  0.0 wa,  0.0 hi,  1.1 si,  0.0 st
KiB Mem : 65.2/12268760 [||||||||||||||||||||||||||||||||||                   ]
KiB Swap:  0.0/0        [                                                     ]

  PID USER      PR  NI    VIRT    RES    SHR S  %CPU  %MEM     TIME+ COMMAND
  220 root      20   0 2652556 700140  26556 S   0.0   5.7   5:22.01 Timer ca+
  264 root      20   0 2652556 700140  26556 S   0.0   5.7   1:21.82 Thread-12
  213 root      20   0 2652556 700140  26556 S   0.0   5.7   1:06.54 C2 Compi+
  207 root      20   0 2652556 700140  26556 S   0.0   5.7   0:22.65 VM Thread
  262 root      20   0 2652556 700140  26556 S   0.0   5.7   0:15.44 s1-io-1
  261 root      20   0 2652556 700140  26556 S   0.0   5.7   0:12.92 s1-io-0
  205 root      20   0 2652556 700140  26556 S   0.0   5.7   0:12.87 java
  222 root      20   0 2652556 700140  26556 S   0.0   5.7   0:10.46 VM Perio+
  214 root      20   0 2652556 700140  26556 S   0.0   5.7   0:09.66 C1 Compi+
  224 root      20   0 2652556 700140  26556 S   0.0   5.7   0:07.03 Profiler+
  258 root      20   0 2652556 700140  26556 S   0.0   5.7   0:05.44 s1-timer+
  265 root      20   0 2652556 700140  26556 S   0.0   5.7   0:05.03 Thread-14
  241 root      20   0 2652556 700140  26556 R   0.0   5.7   0:04.63 s0-timer+
  546 root      20   0 2652556 700140  26556 S   0.0   5.7   0:02.08 pool-3-t+
  548 root      20   0 2652556 700140  26556 S   0.0   5.7   0:01.88 pool-3-t+
  286 root      20   0 2652556 700140  26556 S   0.0   5.7   0:01.80 http-nio+
  556 root      20   0 2652556 700140  26556 S   0.0   5.7   0:01.74 pool-3-t+
  544 root      20   0 2652556 700140  26556 S   0.0   5.7   0:01.73 pool-3-t+
  542 root      20   0 2652556 700140  26556 S   0.0   5.7   0:01.71 pool-3-t+
  547 root      20   0 2652556 700140  26556 S   0.0   5.7   0:01.66 pool-3-t+
  560 root      20   0 2652556 700140  26556 S   0.0   5.7   0:01.64 pool-3-t+
```

#### Garbage Collection logs

Garbage Collection logs contain records for the memory availability and memory deallocation.
It can help find out the possible performance issues due to memory leaks.
<br> User can download Garbage Collection logs of the pod or service for a selected time range.

![Garbage Collection](../images/user_guide/services/gc_log.png)

Downloaded zip file will contain different garbage collection log files, filename is a UTC timestamp with
`YYYY-MM-DD-T-HH-MM-SS-UTC` format.

![GC File Structure](../images/user_guide/services/gc_file.png)

Example of contents in the garbage collection log file:

```java
[2023-08-03T07:57:08.772+0000][15192.847s][trace][gc] GC(26) Young invoke=27 size=168
[2023-08-03T07:57:08.772+0000][15192.847s][trace][gc] GC(26) Tenured: promo attempt is safe: available(1286427576) >= av_promo(8091563), max_promo(373051440)
[2023-08-03T07:57:08.788+0000][15192.863s][info ][gc] GC(26) Pause Young (Allocation Failure) 403M->49M(1268M) 16.168ms
[2023-08-03T08:17:31.746+0000][16415.820s][trace][gc] GC(27) Young invoke=28 size=16
[2023-08-03T08:17:31.746+0000][16415.820s][trace][gc] GC(27) Tenured: promo attempt is safe: available(1286331360) >= av_promo(7714596), max_promo(368146368)
[2023-08-03T08:17:31.752+0000][16415.827s][info ][gc] GC(27) Pause Young (Allocation Failure) 399M->48M(1268M) 6.175ms
[2023-08-03T08:38:01.748+0000][17645.822s][trace][gc] GC(28) Young invoke=29 size=32
[2023-08-03T08:38:01.748+0000][17645.822s][trace][gc] GC(28) Tenured: promo attempt is safe: available(1286204784) >= av_promo(7327908), max_promo(367724224)
[2023-08-03T08:38:01.755+0000][17645.829s][info ][gc] GC(28) Pause Young (Allocation Failure) 398M->48M(1268M) 7.151ms
[2023-08-03T08:44:40.845+0000][18044.920s][trace][gc] GC(29) Young invoke=30 size=56
[2023-08-03T08:44:40.845+0000][18044.920s][trace][gc] GC(29) Tenured: promo attempt is safe: available(1286176736) >= av_promo(6959426), max_promo(367705536)
[2023-08-03T08:44:40.861+0000][18044.935s][info ][gc] GC(29) Pause Young (Allocation Failure) 398M->51M(1268M) 15.752ms
```

### Heap dumps

`Heap dumps` tab allows browsing found heap dumps in time range.
**Heap dumps** usually collect java process heap dumps during OOM failure of a microservice.

> **Note:** Gathering a heap dump is a heavy operation that will impact service performance.
>
> Heap dump size can be huge (>>`1 GB`). Make sure that a volume has enough free space

#### Heap dumps

A heap dump (`.hprof` file) is a snapshot of all the objects in the JVM heap at a certain point in time.
It contains detailed information for each object instance, such as the address, type, class name, or size,
and whether the instance has references to other objects. This dump could be extremely useful
during investigation of memory leaks.

Base docker image for java microservices contains settings to automatically create `.hprof` file,
when application fails with an `OutOfMemoryError` exception. It would be uploaded to a CDT storage as soon as possible.

Available heap dumps will be listened in a separate column on the `Heap dumps` tab:

![Heap Dumps](../images/user_guide/heap/heap_dumps.png)

Due to big size of heap dump in most cases, it is persisted and downloaded as an archive.
Downloaded zip file will contain a heap dump itself - a filename with `java_pid<number>.hprof` format.
Before further processing it should be unzipped.

##### Analysis

There are two types of object sizes to consider during the investigation:

* `Shallow heap size`: actual size of an object itself in the memory
* `Retained heap size`: the amount of memory that will be freed when an object is garbage collected.

There are at least two free open-source tools useful during memory allocation investigation:

##### VisualVM

* VisualVM: [visualvm.github.io](https://visualvm.github.io/)

VisualVM displays the Summary view by default, it displays the running environment where the heap dump
was taken and other system properties:

![VisualVM Summary View](../images/user_guide/heap/heap_vvm_summary.png)

You can use `Compute Retained Sizes` button to figure out dominators (instances which have the biggest cumulative size),
but it takes long time for heavy heap dumps.

![VisualVM Summary Dominators](../images/user_guide/heap/heap_vvm_dominators.png)

By clicking on a row it possible to get info about usage and actual values for selected object:

![VisualVM Summary Details View](../images/user_guide/heap/heap_vvm_details.png)

There also `Classes View` (displays list of classes and the number and percentage of instances referenced by that class)
and `Instances View` (displays object instances for a selected class) for additional statistics.
And advanced
[OQL](https://htmlpreview.github.io/?https://raw.githubusercontent.com/visualvm/visualvm.java.net.backup/master/www/oqlhelp.html)
(`Object Query Language`, a SQL-like query language) to query opened Java heap with complex requests.

See documentation for more information:

* [https://visualvm.github.io/documentation.html](https://visualvm.github.io/documentation.html)

##### Eclipse Memory Analyzer

* Eclipse Memory Analyzer (MAT): [eclipse.dev/mat](https://eclipse.dev/mat/)

![MAT Open File](../images/user_guide/heap/heap_mat_open.png)

It contains not only an overview of the heap dump and leak suspect info:

![MAT Open Leak suspects](../images/user_guide/heap/heap_mat_leak.png)

Several things to investigate:

* Should analyze the biggest items at the top level of the dominator tree because if an item were no longer
  referenced then all that memory could be freed.
* It could be that single objects do not retain a significant amount of memory but many objects all of one type do.
  This is a second class of leak suspect. This type is found using the dominator tree, grouped by class.

If the leak suspect is a group of objects then the biggest few objects are shown by Biggest Instances:

![MAT Top Consumers](../images/user_guide/heap/heap_mat_top_consumers.png)

The dominator tree is used to identify the retained heap. It is produced by the complex object graph
generated at runtime and helps to identify the largest memory graphs, so we can see which objects
are retained in the memory:

![MAT Big Objects](../images/user_guide/heap/heap_mat_big_objects.png)

And also `Histogram view` can be used to get a better insight into which objects exist:

![MAT Histogram](../images/user_guide/heap/heap_mat_histogram.png)

It is also possible to use `OQL` here too.

See documentation for more information:

* [Memory Analyzer Help](https://help.eclipse.org/latest/index.jsp?topic=/org.eclipse.mat.ui.help/welcome.html)
* [Learning Material](https://wiki.eclipse.org/MemoryAnalyzer/Learning_Material)
* [Memory Analyzer FAQ](https://wiki.eclipse.org/MemoryAnalyzer/FAQ)
