var ESCConstants = window.ESCConstants || new function() {
    //mapping of fields in each call record in the table
    this.C_TIME = 0;
    this.C_DURATION = 1;
    this.C_NON_BLOCKING = 2;
    this.C_CPU_TIME = 3;
    this.C_QUEUE_WAIT_TIME = 4;
    this.C_SUSPENSION = 5;
    this.C_CALLS = 6;
    this.C_FOLDER_ID = 7;
    this.C_ROWID = 8;
    this.C_METHOD = 9;
    this.C_TRANSACTIONS = 10;
    this.C_MEMORY_ALLOCATED = 11;
    this.C_LOG_GENERATED = 12;
    this.C_LOG_WRITTEN = 13;
    this.C_FILE_TOTAL = 14;
    this.C_FILE_WRITTEN = 15;
    this.C_NET_TOTAL = 16;
    this.C_NET_WRITTEN = 17;
    this.C_PARAMS = 18;
    this.C_TITLE_HTML = 19;
    this.C_TITLE_HTML_NOLINKS = 20;

    this.C_NAMESPACE = 21;
    this.C_SERVICE_NAME = 22;
    this.C_TRACE_ID = 23;
    this.C_SPAN_ID = 24;

//tags.t[%index%] contains array with the following indexes
    this.T_FULL_NAME = 0;
    this.T_RETURN_TYPE = 1;
    this.T_PACKAGE = 2;
    this.T_CLASS = 3;
    this.T_METHOD = 4;
    this.T_ARGUMENTS = 5;
    this.T_SOURCE = 6;
    this.T_JAR = 7;
    this.T_HTML = 11;
    this.T_CATEGORY = 12;
    this.T_CATEGORY_ACTIVE = 13;

//statemeta params is a mapping of param_name -> array[] with the following indexes
    this.T_TYPE_LIST = 0;
    this.T_TYPE_ORDER = 1;
    this.T_TYPE_INDEX = 2;
    this.T_TYPE_SIGNATURE = 3;
    this.T_TYPE_REACTOR = 4;

//default tags
    this.TAGS_ROOT = -1;
    this.TAGS_HOTSPOTS = -2;
    this.TAGS_PARAMETERS = -3;
    this.TAGS_CALL_ACTIVE = -4;
    this.TAGS_CALL_ACTIVE_STR = this.TAGS_CALL_ACTIVE.toString();
    this.TAGS_JAVA_THREAD = -5;
}

if(typeof module === 'object' && typeof module.exports === 'object') {
    module.exports['ESCConstants'] = ESCConstants;
}
window.ESCConstants = ESCConstants;
