# Cloud maintenance job configuration

cloud-maintenance-job runs several jobs, that executes operations on tables/s3 files/pods with specific time ranges:

- `TempTableCreation` job creates temp tables from current time for next time hours specified;
- `TempTableRemoval` job removes old temp tables with `persisted` status that are older than time hours specified;
- `S3FileRemoval` job removes old s3 files with `completed` status that are older than time hours specified.
  It's possible to specify different time ranges for different types of s3 files;
- `MetadataRemoval` job removes old pod information that are older than time hours specified;

User can specify time ranges for different jobs and different types of files using `--run.config` cli argument.
If config location is not specified, the default configuration will be used:

- 2 hours for `TempTableCreation` job;
- 2 hours for `TempTableRemoval` job;
- 2 weeks for every type and every file for `S3FileRemoval` job;
- 2 weeks for `MetadataRemoval` job;

User can overrides some of time ranges in YAML configuration. In that case, specified values will be merged
with default one. E.g. if user specified such configuration:

```yaml
tempTableRemoval: 1 # 1 hour
s3FileRemoval:
  calls:
    0ms: 168 # 14 * 24 hours = 2 weeks
dumps:
    heap: 240 # 10 * 24 hours = 10 days
  heaps: 288  # 12 * 24 hours = 12 days
```

The following configuration will be used:

- Default 2 hours for `TempTableCreation` job;
- 1 hour for `TempTableRemoval` job;
- 2 weeks for every type and every file for `S3FileRemoval` job except:
  - 1 week for calls s3 file with `0ms` duration range;
  - 10 days for dumps s3 file with `heap` type;
  - 12 days for heaps s3 file;

## Configuration format

| Field             | Description                                                   | Scheme                                                 |
| ----------------- | ------------------------------------------------------------- | ------------------------------------------------------ |
| tempTableCreation | `TempTableCreation` time range in hours. Default value is `2` | unsigned integer                                       |
| tempTableRemoval  | `TempTableRemoval` time range in hours. Default value is `2`  | unsigned integer                                       |
| s3FileRemoval     | `S3FileRemoval` time range.                                   | \*[S3FileRemoval](#s3fileremoval-configuration-format) |
| metadataRemoval   | `MetadataRemoval` time range in hours. Default value is `336` | unsigned integer                                       |

### S3FileRemoval configuration format

| Field | Description                                                | Scheme                                                           |
| ----- | ---------------------------------------------------------- | ---------------------------------------------------------------- |
| calls | Calls s3 files time range in hours per duration range      | \*[S3FileRemovalCalls](#s3fileremovalcalls-configuration-format) |
| dumps | Dumps s3 files time range in hours per duration range      | \*[S3FileRemovalDumps](#s3fileremovaldumps-configuration-format) |
| heaps | Heaps s3 files time range in hoursю Default value is `336` | unsigned integer                                                 |

### S3FileRemovalCalls configuration format

| Field | Description                                                                           | Scheme           |
| ----- | ------------------------------------------------------------------------------------- | ---------------- |
| 0ms   | Calls s3 files time range in hours for `0ms` duration range. Default value is `336`   | unsigned integer |
| 1ms   | Calls s3 files time range in hours for `1ms` duration range. Default value is `336`   | unsigned integer |
| 10ms  | Calls s3 files time range in hours for `10ms` duration range. Default value is `336`  | unsigned integer |
| 100ms | Calls s3 files time range in hours for `100ms` duration range. Default value is `336` | unsigned integer |
| 1s    | Calls s3 files time range in hours for `1s` duration range. Default value is `336`    | unsigned integer |
| 5s    | Calls s3 files time range in hours for `5s` duration range. Default value is `336`    | unsigned integer |
| 30s   | Calls s3 files time range in hours for `30s` duration range. Default value is `336`   | unsigned integer |
| 90s   | Calls s3 files time range in hours for `90s` duration range. Default value is `336`   | unsigned integer |

### S3FileRemovalDumps configuration format

| Field       | Description                                                                            | Scheme           |
| ----------- | -------------------------------------------------------------------------------------- | ---------------- |
| td          | Dumps s3 files time range in hours for `td` dump type. Default value is `336`          | unsigned integer |
| top         | Dumps s3 files time range in hours for `top` dump type. Default value is `336`         | unsigned integer |
| gc          | Dumps s3 files time range in hours for `gc` dump type. Default value is `336`          | unsigned integer |
| alloc       | Dumps s3 files time range in hours for `alloc` dump type. Default value is `336`       | unsigned integer |
| goroutine   | Dumps s3 files time range in hours for `goroutine` dump type. Default value is `336`   | unsigned integer |
| heap        | Dumps s3 files time range in hours for `heap` dump type. Default value is `336`        | unsigned integer |
| profile     | Dumps s3 files time range in hours for `profile` dump type. Default value is `336`     | unsigned integer |
| thread_info | Dumps s3 files time range in hours for `thread_info` dump type. Default value is `336` | unsigned integer |
