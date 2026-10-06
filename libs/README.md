# profiler-common

* Common golang packages
  * logging (`log`)
  * utilities (`common` & `files`)

## Module

### Folders

* `log`
* `common`
* `files`
* `cron`

## How to

### How to develop it locally

In order to make it quicker to develop, developer can use symlink instead of using original library.

1. Checkout latest version of `profiler-common`
2. Replace full name in all Golang source files and `go.mod`:

   Replace `github.com/Netcracker/qubership-profiler-backend/libs/common` to `profiler-common`

3. Create symlink for the folder in dependant repositories:
   * Linux: `ln -s /mnt/c/workspace/profiler-common  /mnt/c/workspace/project/profiler-common`
   * Windows: `mklink /d C:\workspace\project\profiler-common C:\workspace\profiler-common`

# profiler-protocol

* Common golang packages
  * ESC/CDT binary protocol

## Module

### Folders

* `generator`
* `io`
* `model`
* `parser`
* `server`

## How to

### How to develop it locally

In order to make it quicker to develop, developer can use symlink instead of using original library.

1. Checkout latest version of `profiler-protocol`
2. Replace full name in all Golang source files and `go.mod`:

   Replace `github.com/Netcracker/qubership-profiler-backend/libs/protocol` to `profiler-protocol`

3. Create symlink for the folder in dependant repositories:
   * Linux: `ln -s /mnt/c/workspace/profiler-protocol /mnt/c/workspace/project/profiler-protocol`
   * Windows: `mklink /d C:\workspace\project\profiler-protocol C:\workspace\profiler-protocol`

# Cloud Storage

* Common Golang packages related
  * Packages for Cloud Storage implementation
    * Classes to operate with meta tables in Postgres
    * Classes to read/write Parquet files to S3-compatible storage
* Schema and documentation for meta tables in Postgres

## Module

### Folders

* `metrics`
* `model`
* `parquet`
* `pg`
* `s3`

## How to

### How to develop it locally

In order to make it quicker to develop, developer can use symlink instead of using original library.

1. Checkout latest version of `cloud-storage`
2. Replace full name in all Golang source files and `go.mod`:

   Replace `github.com/Netcracker/qubership-profiler-backend/libs/storage` to `cloud-storage`

3. Create symlink for the folder in dependant repositories:
    * Linux: `ln -s /mnt/c/workspace/cloud-storage /mnt/c/workspace/project/cloud-storage`
    * Windows: `mklink /d C:\workspace\project\cloud-storage C:\workspace\cloud-storage`
