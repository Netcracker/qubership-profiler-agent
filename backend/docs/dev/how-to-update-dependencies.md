# How to Update Dependencies

## Table of Contents

<!-- TOC -->
* [How to Update Dependencies](#how-to-update-dependencies)
  * [Table of Contents](#table-of-contents)
  * [Go Services and Libraries](#go-services-and-libraries)
    * [Update Order](#update-order)
    * [Update Dependencies](#update-dependencies)
    * [Resolve "Not promotable" Status](#resolve-not-promotable-status)
  * [Cloud Profiler and UI](#cloud-profiler-and-ui)
<!-- TOC -->

## Go Services and Libraries

### Update Order

The profiler has 3 services and 3 libraries written in Go.

If you want to update both services and libraries,
you should start with the libraries, because services use these libraries.

Moreover, some libraries use other libraries, so you need to follow the update order:

1. [profiler-common](https://github.com/Netcracker/qubership-profiler-backend/libs/common)
2. [profiler-protocol](https://github.com/Netcracker/qubership-profiler-backend/libs/protocol)
3. [cloud-storage](https://github.com/Netcracker/qubership-profiler-backend/libs/storage)

After that, you can update the services in any order:

* [cloud-profiler-dumps-collector](https://github.com/Netcracker/qubership-profiler-backend/apps/dumps-collector)
* [cloud-maintenance-job](https://github.com/Netcracker/qubership-profiler-backend/apps/maintenance)
* [cloud-profiler-compactor](https://github.com/Netcracker/qubership-profiler-backend/apps/compactor)

### Update Dependencies

Open **go.mod** in the service/library you need.
Open the terminal in the root folder of the project.

1. You can update individual dependencies to the version you need:
   `go get <module-path>@<version>`

2. Or you can update all dependencies to the latest compatible version at once:
   `go get -u ./..`

If you encounter an error stating that our libraries cannot be found (404)
when updating all dependencies, this is normal.
You need to clone these libraries to your computer and use `require` to temporarily replace them in **go.mod**.

For example:

`replace github.com/Netcracker/qubership-profiler-backend/libs/storage => C:\dev\cloud-storage`

After that, you can run `go get -u ./..` again.
But don't forget to remove all `require`s after that.

To update the version of our libraries (profiler-common, profiler-protocol, cloud-storage),
you will need to manually change the version in the **go.mod** file.

Finally, run `go mod tidy` to add missing dependencies, remove unused dependencies, and update **go.sum**.

### Resolve "Not promotable" Status

After such updates, it is essential to resolve the “Not promotable” status.
To do this, please refer to this [page](how-to-work-with-ci.md#resolve-not-promotable-status).

Again, it is best to do this in the correct order:
starting with profiler-common, profiler-protocol, cloud-storage, and moving on to services.

## Cloud Profiler and UI

After updating **cloud-profiler-ui** and merging it into master,
the **profiler-ui-version** file with its version should automatically update in **cloud-profiler**.

However, after promoting **cloud-profiler-ui**, this file must be changed manually.

You can read more about this on this [page](how-to-work-with-ci.md#ui-build).

After such updates, it is essential to resolve the “Not promotable” status.
To do this, please refer to this [page](how-to-work-with-ci.md#resolve-not-promotable-status).
