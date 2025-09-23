package com.netcracker.profiler.agent;

import java.util.Map;

/* Example:
1. AND (root):
   - children: ->
   1.1 AND:
       - children:
         1.1.1 web.method EXACT f1
         1.1.2 web.url ENDSWITH s2
         1.1.3 OR:
               - children:
               1.1.3.1 NOT
                       - children:
                       1.1.3.1.1 web.method STARTSWITH s3
               1.1.3.2 web.method EXACT r4
               1.1.3.3 web.url CONTAINS t5

  <and>
    <web.method EXACT f1>
    <web.url ENDSWITH s2>
    <or>
        <not>
            <web.method STARTSWITH s3>
        </not>
        <web.method EXACT r4>
        <web.url CONTAINS t5>
    </or>
</and>
 */
public interface FilterOperator {
    String CALL_INFO_PARAM = "callInfo";
    String THREAD_STATE_PARAM = "threadState";
    String DURATION_PARAM = "duration";
    String THREAD_NAME_PARAM = "java.thread";
    String NODE_NAME_PARAM = "node.name";
    String ADDITIONAL_INPUT_PARAMS = "additionalInputParams";

    boolean evaluate(Map<String, Object> params);
}
