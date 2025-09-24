# Diagtools

The `diagtools` is a CLI tool to work with java heap and thread dumps, CPU usage, schedule some tasks or
export ZooKeeper config.

Supported features:

- `diagtools heap zip upload`
  - heap subcommand. Runs `jmap -dump:format=b,file={{.DumpPath}} {{.Pid}}` to collect heap dump for java pid and
    put it in the file specified.
  - zip parameter is optional. Zips heap file with default compression level. Compression level can be configured with
    NC_HEAP_DUMP_COMPRESSION_LEVEL environment variable:
    - NoCompression      = 0
    - BestSpeed          = 1
    - BestCompression    = 9
    - DefaultCompression = -1
      see [golang:deflate.go](https://github.com/golang/go/blob/master/src/compress/flate/deflate.go#L15-L18)
  - upload parameter is optional. Uploads zipped heap dump to collector service diagnostic controller
    via REST `http://localhost:8081/diagnostic/{namespace}/*/*/*/*/*/*/{podName}/{dumpName}`.
    Works together with zip flag only.

- `diagtools dump`
  - "dump" subcommand is responsible for collecting java thread dump (`jstack -l "{{.Pid}}"`) and
    CPU usage for java pid (`top -Hb -p{{.Pid}} -oTIME+ -d60 -n1`)
    NC_DIAGNOSTIC_THREADDUMP_ENABLED and NC_DIAGNOSTIC_TOP_ENABLED is responsible for turning on/off collecting
    data. If environment variable is absent it is treated as on.

- `diagtools scan /tmp/diagnostic/*.hprof* ./core* ./hs_err*`
  - "scan" subcommand is responsible for finding files matching patterns, zipping (if necessary) ".hprof" and
    uploading ".hprof.zip" and other found files to collector service diagnostic controller via
    REST `http://localhost:8081/diagnostic/{namespace}/*/*/*/*/*/*/{podName}/{dumpName}`.

- `diagtools schedule`
  - "schedule" subcommand is responsible for collecting dumps(like dump subcommand),
    scanning(like scan subcommand) and cleaning logs located in NC_DIAGNOSTIC_LOGS_FOLDER by schedule.
    Interval can be changed via DIAGNOSTIC_DUMP_INTERVAL(default 1m), DIAGNOSTIC_SCAN_INTERVAL(default 3m) and
    KEEP_LOGS_INTERVAL(default 2 days) environment variables.

- `diagtools zkConfig "${NC_DIAGNOSTIC_FOLDER}/zkproperties" esc.config NC_DIAGNOSTIC_ESC_ENABLED ...`
  - "zkConfig" subcommand is responsible for changing nc-diagnostic-agent settings in case when ZOOKEEPER_ENABLED=true
    The first parameter is a path to zookeeper property file.
    The second and further ones are the zookeeper properties which are to be changed.

Environment variables used by tool:

- NC_DIAGNOSTIC_FOLDER path - to diagnostic folder. Default is `/tmp/diagnostic`.
- NC_DIAGNOSTIC_LOGS_FOLDER - path to logs. Default is `/tmp/diagnostic/log`.
- LOG_FILE_SIZE - size of log file in MB. Default is 1.
- LOG_FILE_BACKUPS - number of log file backups. Default is 5.
- KEEP_LOGS_INTERVAL - logs located in NC_DIAGNOSTIC_LOGS_FOLDER rotation interval in days. Default is 2.
- LOG_TO_CONSOLE - indicates if send logs to the console. Default is false.
- DIAGNOSTIC_CENTER_DUMPS_ENABLED - used to check if upload dumps to diagnostic center.
- NC_DIAGNOSTIC_THREADDUMP_ENABLED - used to check if thead dumps enabled.
- NC_DIAGNOSTIC_TOP_ENABLED - used to check if top dumps enabled.
- DIAGNOSTIC_DUMP_INTERVAL - dump interval used in case of schedule. Default is 1 minute. Support go Duration format.
- DIAGNOSTIC_SCAN_INTERVAL - scan interval used in case of schedule. Default is 1 minutes. Support go Duration format.
- NC_DIAGNOSTIC_AGENT_SERVICE - diagnostic agent service name. Default is `nc-diagnostic-agent`.
- PROFILER_FOLDER -path to profiler folder. Default is `/app/diag`
- ZOOKEEPER_ENABLED - used to check if zookeeper enabled. Default is false.
- CLOUD_NAMESPACE - contains actual microservice namespace. Can't be empty.
- MICROSERVICE_NAME - contains actual microservice name. Can't be empty.
- ZOOKEEPER_ADDRESS - zookeeper address for fetch settings from ZooKeeper.
- NC_HEAP_DUMP_COMPRESSION_LEVEL - defines heap dump compression level. Default is `-1`.
- ESC_LOG_FORMAT - used to set custom log format for agent loggers using java logging service
- LOGBACK_CLOUD_AGENT_LOG_FORMAT - Used to set custom log format for agent loggers using logback service.
