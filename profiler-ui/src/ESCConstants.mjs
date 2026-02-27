const ESCConstants = {
    //mapping of fields in each call record in the table
    C_TIME: 0,
    C_DURATION: 1,
    C_CPU_TIME: 2,
    C_QUEUE_WAIT_TIME: 3,
    C_SUSPENSION: 4,
    C_CALLS: 5,
    C_FOLDER_ID: 6,
    C_ROWID: 7,
    C_METHOD: 8,
    C_TRANSACTIONS: 9,
    C_MEMORY_ALLOCATED: 10,
    C_LOG_GENERATED: 11,
    C_LOG_WRITTEN: 12,
    C_FILE_TOTAL: 13,
    C_FILE_WRITTEN: 14,
    C_NET_TOTAL: 15,
    C_NET_WRITTEN: 16,
    C_PARAMS: 17,
    C_TITLE_HTML: 18,
    C_TITLE_HTML_NOLINKS: 19,

    C_NAMESPACE: 20,
    C_SERVICE_NAME: 21,
    C_TRACE_ID: 22,
    C_SPAN_ID: 23,

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
