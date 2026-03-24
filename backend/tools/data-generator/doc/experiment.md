# Experiment to estimate file sizes when storing data in s3 storage (POC)

- [Experiment to estimate file sizes when storing data in s3 storage (POC)](#experiment-to-estimate-file-sizes-when-storing-data-in-s3-storage-poc)
  - [Introduction](#introduction)
  - [Objective and Method](#objective-and-method)
  - [Quick Summary](#quick-summary)
    - [Compression Type (Our Recomendation)](#compression-type-our-recomendation)
    - [Comparison Of The Obtained Data With The Analysis](#comparison-of-the-obtained-data-with-the-analysis)
  - [Postgres experiment](#postgres-experiment)
  - [Data Description](#data-description)
  - [Parquet - Description of Format and Characteristics](#parquet---description-of-format-and-characteristics)
    - [Compression Types](#compression-types)
  - [Results](#results)
    - [Calls](#calls)
    - [Dumps](#dumps)

## Introduction

This document presents the results of an experiment to evaluate the amount of memory used when storing data
on an S3 cloud storage. The experiment was conducted using .parquet files for two types of data: Call and Dump,
and file sizes were analyzed for different storage scenarios.

Separately, an experiment was performed to write an inverted index in Postgres using random data.

## Objective and Method

The objective of the experiment was to evaluate the use of space when storing data that is close to real data,
as well as to confirm previous estimates of file sizes made in the analysis of the S3 storage.

To achieve our goal, we used the **cdt-data-generator** tool, which allows us to obtain a set of real data
that can be expanded to the required amount.

## Quick Summary

Based on the results obtained, it can be concluded that the calculations performed during the analysis were generally
correct for Calls, and it was also confirmed that the .parquet format provides a good degree of compression and helps
save space.
Below is a brief table of results for saving Calls to .parquet files.

- Common Results For Calls:

  | Compression codec | Total compressed | Сompression ratio | File size (in Mb) | Tmerange | Total calls |
  | ----------------- | ---------------- | ----------------- | ----------------- | -------- | ----------- |
  | GZIP, LZ4         | 22.50%           | 1.3               | 930               | 5 min    | 495000      |
  | Brotli, LZO       | 68.25%           | 3.1               | 381               | 5 min    | 495000      |
  | ZSTD              | 22.67%           | 1.3               | 928               | 5 min    | 495000      |
  | Snappy            | 21.50%           | 1.3               | 942               | 5 min    | 495000      |

- Common Results For Java Dumps:

  | Compression Type | Total compressed | Сompression ratio | Size (Mb) |
  | ---------------- | ---------------- | ----------------- | --------- |
  | Brotli           | 17%              | 1.2               | 5095      |
  | Gzip             | 17%              | 1.2               | 5101      |
  | LZ4              | 15%              | 1.2               | 5204      |
  | LZO              | 17%              | 1.2               | 5095      |
  | Snappy           | 15%              | 1.2               | 5204      |
  | ZSTD             | 17%              | 1.2               | 5100      |

- Commo Results For Go Dumps:

  | Compression Type | Total compressed | Сompression ratio | Size (Mb) |
  | ---------------- | ---------------- | ----------------- | --------- |
  | Brotli           | 21%              | 1.3               | 3553      |
  | Gzip             | 0%               | 1.0               | 4489      |
  | LZ4              | 0%               | 1.0               | 4488      |
  | LZO              | 21%              | 1.3               | 3553      |
  | Snappy           | 0%               | 1.0               | 4489      |
  | ZSTD             | 0%               | 1.0               | 4488      |

### Compression Type (Our Recomendation)

- **Brotli** - Provides maximum compression ratio for Calls
- **ZSTD** - It is supported by many analysis tools and provides good compression ratio

### Comparison Of The Obtained Data With The Analysis

- Calls per day

  | Compression type | From experiments | From analysis |
  | ---------------- | ---------------- | ------------- |
  | ZSTD             | 261 Gb           | 691 Gb        |
  | Brotli           | 107 Gb           | 691 Gb        |

- Java Dumps per day

  | Compression type | From experiments | From analysis |
  | ---------------- | ---------------- | ------------- |
  | ZSTD             | 1.43 Tb          | 37 Gb         |
  | Brotli           | 1.43 Tb          | 37 Gb         |

- Go Dumps per day

  | Compression type | From experiments | From analysis |
  | ---------------- | ---------------- | ------------- |
  | ZSTD             | 1.23 Tb          | 35 Gb         |
  | Brotli           | 1 Tb             | 35 Gb         |

## Postgres experiment

Experiments were carried out to write the inverted index in Postgres. The results are listed below:

| Number of rows | Size of one table | Postgres index size | Database size (9 tables) | Insert time | Select time |
| -------------- | ----------------- | ------------------- | ------------------------ | ----------- | ----------- |
| 500,000        | 250 Mb            | 100 Kb              | 2078 Mb                  | < 10 ms     | ~ 70-80 ms  |

Row - unique value for trace_id

## Data Description

According to the analysis, it was necessary to conduct experiments for two types of stored data - Call and Dumps.
You can find the main information about these data types in the S3 storage analysis document - 
In our experiments, we were interested in the following storage scheme:

- Calls

  - Period - 5 minutes
  - Each Pod - 1000 calls / 5 min
  - Dev-shared scheme - 20 namespaces, 50 services, 2 pods = 2000 pods

- Dumps
  - Period - 5 minutes
  - Each Pod - 3 dumps / 1 min

## Parquet - Description of Format and Characteristics

The .parquet format is a columnar data storage format that allows for efficient compression and processing of large
volumes of data. Unlike string formats such as CSV or JSON, where data is stored row by row, in the .parquet format,
data is stored column-wise. This means that all values of a specific data type are stored together in one column.
This approach allows for better data compression and speeds up query processing that works only with specific columns.

The data storage schema in the .parquet format looks like this: data is divided into blocks (chunks), each of which
contains data for only one column. Each block is further divided into pages, which contain fragments of data for that
column. Inside each page, data can be further compressed and encoded according to the chosen compression and encoding
algorithm. This approach allows for efficient use of memory and speeds up the reading and writing of data.

![Alt text](parquet.png)

### Compression Types

The .parquet format uses several types of data compression, such as Snappy, GZip, Brotli, Zstd, LZO, and LZ4, etc.
In experiments, we tested the compression ratio of each of these codecs and selected the four most common ones for
further use: Snappy, GZip, Brotli, and Zstd. However, LZO and LZ4 codecs showed similar results, but they are not
supported by analytical tools that we used to check the results (Clickhouse/DuckDB).

| Compression type | Сompression ratio Min | Compression ratio Max | Supported by Clickhouse/DuckDb |
| ---------------- | --------------------- | --------------------- | ------------------------------ |
| Brotli           | 1.2                   | 3.1                   | No                             |
| Gzip             | 1                     | 1.4                   | Yes                            |
| LZ4              | 1                     | 1.4                   | No                             |
| LZO              | 1.2                   | 3.1                   | No                             |
| Snappy           | 1                     | 1.4                   | Yes                            |
| Zstd             | 1                     | 1.4                   | Yes                            |

When choosing the Brotli codec, we were guided by the fact that it provides a good compression ratio and can be used to
store data that can later be converted to another format. Although ClickHouse and DuckDB do not support Brotli
directly, it can still be used to store data and then re-encoded if a specific file needs to be loaded into one of
these tools.

## Results

### Calls

- **Pod in file**

  The experiment involved generating load for each pod and saving the data to a separate file. Four types of
  compression codecs were used (GZip, Brotli, LZO, LZ4). The results are shown in the table below.

  | Description  | Compression codec | Timerange | Number of pods in file | Total calls | File size (Mb) | One call size (in Kb) | Uncompressed file size (Mb) | One uncompressed call size | Total compressed | Сompression ratio |
  | ------------ | ----------------- | --------- | ---------------------- | ----------- | -------------- | --------------------- | --------------------------- | -------------------------- | ---------------- | ----------------- |
  | Pod per file | GZIP, LZ4         | 5 min     | 1                      | 990         | 1.9            | 1.95                  | 2.4                         | 2.47                       | 20.83%           | 1.3               |
  | Pod per file | Brotli            | 5 min     | 1                      | 990         | 0.771          | 0.789                 | 2.4                         | 2.47                       | 67.88%           | 3.1               |
  | Pod per file | LZO               | 5 min     | 1                      | 990         | 0.77           | 0.789                 | 2.4                         | 2.47                       | 67.92%           | 3.1               |

- **Service in flie**

  In this experiment, data was saved separately for each service. Additionally, 4 compression codecs were used.

  | Description      | Compression codec | Timerange | Number of pods in file | Total calls | File size (Mb) | One call size (in Kb) | Uncompressed file size (Mb) | One uncompressed call size | Totalcompressed | Сompression ratio |
  | ---------------- | ----------------- | --------- | ---------------------- | ----------- | -------------- | --------------------- | --------------------------- | -------------------------- | --------------- | ----------------- |
  | Service per file | GZIP, LZ4         | 5 min     | 2                      | 1980        | 3.8            | 1.95                  | 4.8                         | 2.47                       | 20.83%          | 1.3               |
  | Service per file | Brotli, LZO       | 5 min     | 2                      | 1980        | 1.6            | 0.8                   | 4.8                         | 2.47                       | 66.67%          | 3.0               |

- **Emulation of a collector with pre-sorted records**

  In this experiment, the focus was on partially emulating the profiler's work. In this case, load generation was done
  for 2000 pods, which were divided into 4 collectors (500 pods per collector), but the data in the "collector" was
  received sequentially, in a pre-sorted form. As seen from the results, Brotli and LZO showed the best compression
  ratio, while the results for the others were similar.

  | Description          | Compression codec | Timerange | Number of pods in file | Total calls | File size (Mb) | One call size (in Kb) | Uncompressed file size (Mb) | One uncompressed call size | Total compressed | Сompression ratio |
  | -------------------- | ----------------- | --------- | ---------------------- | ----------- | -------------- | --------------------- | --------------------------- | -------------------------- | ---------------- | ----------------- |
  | Collector simulation | GZIP, LZ4         | 5 min     | 500                    | 495000      | 927            | 1.91                  | 1200                        | 2.47                       | 22.75%           | 1.3               |
  | Collector simulation | Brotli, LZO       | 5 min     | 500                    | 495000      | 381            | 0.788                 | 1200                        | 2.47                       | 68.25%           | 3.1               |
  | Collector simulation | ZSTD              | 5 min     | 500                    | 495000      | 925            | 1.91                  | 1200                        | 2.47                       | 22.92%           | 1.30              |
  | Collector simulation | Snappy            | 5 min     | 500                    | 495000      | 938            | 1.94                  | 1200                        | 2.47                       | 21.83%           | 1.28              |

- **Emulation of a collector's work (data collection from various services at different times)**
  In this experiment, the task was made more complex by emulating the real work of a collector, where data from
  different services comes in randomly with varying time delays. As seen from the results, the sizes of .parquet files
  increased, which is quite expected, since compression is harder to implement in this case.

  | Description                       | Compression codec | Timerange | Number of pods in file | Tota calls | File size (Mb) | One call size (in Kb) | Uncompressed file size (Mb) | One uncompressed call size | Total compressed | Сompression ratio |
  | --------------------------------- | ----------------- | --------- | ---------------------- | ---------- | -------------- | --------------------- | --------------------------- | -------------------------- | ---------------- | ----------------- |
  | Collector simulation with channel | GZIP, LZ4         | 5 min     | 500                    | 495000     | 930            | 1.91                  | 1200                        | 2.47                       | 22.50%           | 1.3               |
  | Collector simulation with channel | Brotli, LZO       | 5 min     | 500                    | 495000     | 381            | 0.788                 | 1200                        | 2.47                       | 68.25%           | 3.1               |
  | Collector simulation with channel | ZSTD              | 5 min     | 500                    | 495000     | 928            | 1.91                  | 1200                        | 2.47                       | 22.67%           | 1.3               |
  | Collector simulation with channel | Snappy            | 5 min     | 500                    | 495000     | 942            | 1.94                  | 1200                        | 2.47                       | 21.50%           | 1.3               |

- **Emulation of a collector's work with the use of different encodings for string fields when saving to .parquet**
  This experiment was based on the previous one, but when saving to .parquet, the PLAIN_DICTIONARY encoding was used
  for frequently occurring string fields. This could help achieve a higher degree of compression, but it did not lead
  to a significant improvement in results.

  | Description                                    | Compression codec | Timerange | Number of pods in file | Total calls | File size (Mb) | One call size (in Kb) | Uncompressed file size (Mb) | One uncompressed call size | Total compressed | Сompression ratio |
  | ---------------------------------------------- | ----------------- | --------- | ---------------------- | ----------- | -------------- | --------------------- | --------------------------- | -------------------------- | ---------------- | ----------------- |
  | Collector simulation with channel and encoding | GZIP              | 5 min     | 500                    | 495000      | 926            | 1.91                  | 1200                        | 2.47                       | 22.83%           | 1.3               |
  | Collector simulation with channel and encoding | Brotli, LZO       | 5 min     | 500                    | 495000      | 381            | 0.788                 | 1200                        | 2.47                       | 68.25%           | 3.1               |

### Dumps

Experiments were conducted for 2 types of dumps in the system:

1. Dumps from java applications

   | Description | Compression codec | Timerange (min) | Number of pods in file | Total dumps | File size (Mb) | One dump size (in Kb) | Uncompressed file size (Mb) | One uncompressed dump size | Total compressed | Сompression ratio |
   | ----------- | ----------------- | --------------- | ---------------------- | ----------- | -------------- | --------------------- | --------------------------- | -------------------------- | ---------------- | ----------------- |
   | td          | gzip              | 5               | 500                    | 7500        | 3,700          | 505.17                | 4300                        | 587.09                     | 13.95%           | 1.2               |
   | top         | gzip              | 5               | 500                    | 7500        | 101            | 13.79                 | 143                         | 19.52                      | 29.37%           | 1.4               |
   | gc          | gzip              | 5               | 500                    | 7500        | 1,300          | 177.49                | 1700                        | 232.11                     | 23.53%           | 1.3               |
   | td          | brotli            | 5               | 500                    | 7500        | 3,700          | 505.17                | 4300                        | 587.09                     | 13.95%           | 1.2               |
   | top         | brotli            | 5               | 500                    | 7500        | 95             | 12.97                 | 143                         | 19.52                      | 33.57%           | 1.5               |
   | gc          | brotli            | 5               | 500                    | 7500        | 1,300          | 177.49                | 1700                        | 232.11                     | 23.53%           | 1.3               |
   | td          | zstd              | 5               | 500                    | 7500        | 3,700          | 505.17                | 4300                        | 587.09                     | 13.95%           | 1.2               |
   | top         | zstd              | 5               | 500                    | 7500        | 100            | 13.65                 | 143                         | 19.52                      | 30.07%           | 1.4               |
   | gc          | zstd              | 5               | 500                    | 7500        | 1,300          | 177.49                | 1700                        | 232.11                     | 23.53%           | 1.3               |
   | td          | snappy            | 5               | 500                    | 7500        | 3,700          | 505.17                | 4300                        | 587.09                     | 13.95%           | 1.2               |
   | top         | snappy            | 5               | 500                    | 7500        | 104            | 14.20                 | 143                         | 19.52                      | 27.27%           | 1.4               |
   | gc          | snappy            | 5               | 500                    | 7500        | 1,400          | 191.15                | 1700                        | 232.11                     | 17.65%           | 1.2               |

2. Dumps from Go applications

   | Description | Compression codec | Timerange (min) | Number of pods in file | Total dumps | File size (Mb) | One dump size (in Kb) | Uncompressed file size (Mb) | One uncompressed dump size | Total compressed | Сompression ratio |
   | ----------- | ----------------- | --------------- | ---------------------- | ----------- | -------------- | --------------------- | --------------------------- | -------------------------- | ---------------- | ----------------- |
   | alloc       | gzip              | 5               | 500                    | 7500        | 2,200          | 300.37                | 2200                        | 300.37                     | 0.00%            | 1.0               |
   | goroutine   | gzip              | 5               | 500                    | 7500        | 76             | 10.38                 | 77                          | 10.51                      | 1.30%            | 1.0               |
   | heap        | gzip              | 5               | 500                    | 7500        | 2,200          | 300.37                | 2,200.00                    | 300.37                     | 0.00%            | 1.0               |
   | profile     | gzip              | 5               | 500                    | 7500        | 13             | 1.77                  | 13                          | 1.77                       | 0.00%            | 1.0               |
   | alloc       | brotli            | 5               | 500                    | 7500        | 1,700          | 232.11                | 2200                        | 300.37                     | 22.73%           | 1.3               |
   | goroutine   | brotli            | 5               | 500                    | 7500        | 48             | 6.55                  | 77                          | 10.51                      | 37.66%           | 1.6               |
   | heap        | brotli            | 5               | 500                    | 7500        | 1,800          | 245.76                | 2,200.00                    | 300.37                     | 18.18%           | 1.2               |
   | profile     | brotli            | 5               | 500                    | 7500        | 5              | 0.63                  | 13                          | 1.77                       | 64.62%           | 2.8               |
   | alloc       | zstd              | 5               | 500                    | 7500        | 2,200          | 300.37                | 2200                        | 300.37                     | 0.00%            | 1.0               |
   | goroutine   | zstd              | 5               | 500                    | 7500        | 76             | 10.38                 | 77                          | 10.51                      | 1.30%            | 1.0               |
   | heap        | zstd              | 5               | 500                    | 7500        | 2,200          | 300.37                | 2,200.00                    | 300.37                     | 0.00%            | 1.0               |
   | profile     | zstd              | 5               | 500                    | 7500        | 12             | 1.64                  | 13                          | 1.77                       | 7.69%            | 1.1               |
   | alloc       | snappy            | 5               | 500                    | 7500        | 2,200          | 300.37                | 2200                        | 300.37                     | 0.00%            | 1.0               |
   | goroutine   | snappy            | 5               | 500                    | 7500        | 76             | 10.38                 | 77                          | 10.51                      | 1.30%            | 1.0               |
   | heap        | snappy            | 5               | 500                    | 7500        | 2,200          | 300.37                | 2,200.00                    | 300.37                     | 0.00%            | 1.0               |
   | profile     | snappy            | 5               | 500                    | 7500        | 13             | 1.77                  | 13                          | 1.77                       | 0.00%            | 1.0               |
