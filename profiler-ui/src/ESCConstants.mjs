const ESCConstants = {
    //mapping of fields in each call record in the table
    C_TIME: 0,
    C_DURATION: 1,
    C_NON_BLOCKING: 2,
    C_CPU_TIME: 3,
    C_QUEUE_WAIT_TIME: 4,
    C_SUSPENSION: 5,
    C_CALLS: 6,
    C_FOLDER_ID: 7,
    C_ROWID: 8,
    C_METHOD: 9,
    C_TRANSACTIONS: 10,
    C_MEMORY_ALLOCATED: 11,
    C_LOG_GENERATED: 12,
    C_LOG_WRITTEN: 13,
    C_FILE_TOTAL: 14,
    C_FILE_WRITTEN: 15,
    C_NET_TOTAL: 16,
    C_NET_WRITTEN: 17,
    C_PARAMS: 18,
    C_TITLE_HTML: 19,
    C_TITLE_HTML_NOLINKS: 20,

    C_NAMESPACE: 21,
    C_SERVICE_NAME: 22,
    C_TRACE_ID: 23,
    C_SPAN_ID: 24,

//tags.t[%index%] contains array with the following indexes
    T_FULL_NAME: 0,
    T_RETURN_TYPE: 1,
    T_PACKAGE: 2,
    T_CLASS: 3,
    T_METHOD: 4,
    T_ARGUMENTS: 5,
    T_SOURCE: 6,
    T_JAR: 7,
    T_HTML: 11,
    T_CATEGORY: 12,
    T_CATEGORY_ACTIVE: 13,

//statemeta params is a mapping of param_name -> array[] with the following indexes
    T_TYPE_LIST: 0,
    T_TYPE_ORDER: 1,
    T_TYPE_INDEX: 2,
    T_TYPE_SIGNATURE: 3,
    T_TYPE_REACTOR: 4,

//default tags
    TAGS_ROOT: -1,
    TAGS_HOTSPOTS: -2,
    TAGS_PARAMETERS: -3,
    TAGS_CALL_ACTIVE: -4,
    TAGS_JAVA_THREAD: -5,
};

ESCConstants.TAGS_CALL_ACTIVE_STR = ESCConstants.TAGS_CALL_ACTIVE.toString();

export { ESCConstants };
