package com.netcracker.profiler.sax.readers;

import com.netcracker.profiler.sax.MethodIdBulder;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class HPROFLine {
    /*
      Sample patterns:
      P#C PLSQL."".""."__plsql_vm"
      P#C PLSQL."SYS"."DBMS_ASSERT"::11."SIMPLE_SQL_NAME"#28dc3402baeb2b0d #153
      P#C PLSQL."NC82"."ISNUMBER"::8."ISNUMBER"#8440831613f0f5d3 #1
      P#C SQL."NC82"."RTE_INSTALLER"::11."__static_sql_exec_line510" #510
      P#C SQL."NC82"."RTE_VALIDATION"::11."__dyn_sql_exec_line22" #22
      P#C SQL."NC82"."RTE_VALIDATION"::11."__sql_fetch_line395" #395
     */
    protected final static Pattern CALL_PATTERN = Pattern.compile(
            "^P#C" // Call line
                    + " (PLSQL|SQL)"// Namespace
                    + "\\.\"([^\"]*)\"" // Schema name
                    + "\\.\"([^\"]*)\"" // Object name
                    + "(?:::([0-9]+))?" // Object type: 11 package procedure, 8 function, 12 trigger
                    + "\\.\"([^\"]*)\"" // Procedure name
                    + "(?:#([^ ]+))?" // overload hash id
                    + "(?: #(\\d+))?" // line number
                    + "(?:\\.\"([^\\s]+)\")?" // sqlId
    );

    protected final static Map<String, String> OBJECT_TYPES = new HashMap<String, String>();

    static {
        OBJECT_TYPES.put("8", "FUNCTION");
        OBJECT_TYPES.put("11", "PACKAGE");
        OBJECT_TYPES.put("12", "TRIGGER");
    }

    public String namespace;
    public String schemaName;
    public String objectName;
    public String objectType;
    public String procedureName;
    public String overloadHashId;
    public int lineNumber;
    public String lineNumberString;
    public String sqlId;

    public boolean init(CharSequence cs) {
        Matcher m = CALL_PATTERN.matcher(cs);
        if (!m.matches())
            return false;
        namespace = m.group(1);
        schemaName = m.group(2);
        objectName = m.group(3);
        objectType = m.group(4);
        String type = OBJECT_TYPES.get(objectType);
        if (type != null)
            objectType = type;
        procedureName = m.group(5);
        overloadHashId = m.group(6);
        lineNumberString = m.group(7);
        if (lineNumberString != null)
            lineNumber = Integer.parseInt(lineNumberString);
        else
            lineNumber = -1;
        sqlId = m.group(8);
        return true;
    }

    public String buildId(MethodIdBulder b) {
        if (b == null)
            b = new MethodIdBulder();

        String objectName = defaultIfEmpty(this.objectName, "no_object");
        String schemaName = defaultIfEmpty(this.schemaName, "no_schema");
        String objectType = defaultIfEmpty(this.objectType, "no_type");
        StringBuilder arguments = new StringBuilder();
        if(overloadHashId != null) {
            arguments.append("hash=").append(overloadHashId);
        }
        if(sqlId != null) {
            if(arguments.length() > 0) {
                arguments.append(", ");
            }
            arguments.append("sql_id=").append(sqlId);
        }

        return b.build(
                namespace + '.' + schemaName + '.' + objectType
                , objectName
                , procedureName
                , arguments.toString()
                , "void"
                , objectName
                , lineNumberString
                , objectName
        );
    }

    private String defaultIfEmpty(String value, String defaultValue) {
        String objectName = value;
        if (objectName == null || objectName.length() == 0) {
            objectName = defaultValue;
        }
        return objectName;
    }
}
