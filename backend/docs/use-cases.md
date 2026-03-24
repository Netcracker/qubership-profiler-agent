# Cloud Profiler. Use cases

## Table of Content

* [Table of Content](#table-of-content)
* [Use Cases](#use-cases)
  * [Find calls by external IDs](#find-calls-by-external-ids)
    * [Case: some task takes too much time](#case-some-task-takes-too-much-time)
  * [Investigate performance particular method](#investigate-performance-particular-method)
    * [Case: perform quick trace analysis](#case-perform-quick-trace-analysis)
  * [Find anomalies](#find-anomalies)

## Use Cases

This section describes different use cases that can occur during use of CDT and provides a description of how you can
solve them using CDT.

### Find calls by external IDs

#### Case: some task takes too much time

Need to find the slowest operation

**Solution**

1. Reproduce the case and get `request-id`
2. Find in Graylog all `trace-id`s  by `request-id`
3. Analyze traces in Jaeger
    * find the longest service operation
4. In CDT filter calls by service and `request-id`
5. Check the call and find the longest method
6. Export CDT data

**By steps**

1. Reproduce the case and get `request-id`

    ![img.png](/docs/images/user_guide/use-case/step1_request_id.png)

2. Find in Graylog all `trace-id`s  by `request-id`

    ![img_1.png](/docs/images/user_guide/use-case/step2_trace_ids.png)

3. Analyze traces in Jaeger
   * find the longest service operation

    ![img_2.png](/docs/images/user_guide/use-case/step3_trace_longest.png)

4. In CDT filter calls by service and `request-id`

    ![img_3.png](/docs/images/user_guide/use-case/step4_cdt_request_id.png)

5. Check the call and find the longest method

    ![img_4.png](/docs/images/user_guide/use-case/step_cdt_investigation.png)

6. Export CDT data

### Investigate performance particular method

#### Case: perform quick trace analysis

**By steps**

1. In CDT filter calls by service and additional information (method name)

    ![img.png](/docs/images/user_guide/use-case/perf_step1_calls_list.png)

2. Click on `Duration` link of found process and go to tree view of the call:

    ![img.png](/docs/images/user_guide/use-case/perf_step2_call_tree.png)

3. Perform analyses investigating method duration, number of method invocations, number of calls and etc

    ![img_1.png](/docs/images/user_guide/use-case/perf_step3_methods.png)

4. Perform analyses investigating SQL queries duration, ran query

    ![img_2.png](/docs/images/user_guide/use-case/perf_step4_sql.png)

### Find anomalies

TODO
