# Data

## Table of Content

<!-- TOC -->
* [Data](#data)
  * [Table of Content](#table-of-content)
  * [Gather data](#gather-data)
    * [Gather with `ksniff`](#gather-with-ksniff)
      * [Installation](#installation)
      * [Catch data](#catch-data)
    * [Root privileges](#root-privileges)
    * [Next steps](#next-steps)
    * [Additional guides](#additional-guides)
<!-- TOC -->

## Gather data

Easiest way to gather data is to catch TCP communication with `ksniff`.

### Gather with `ksniff`

`ksniff` is a kubectl plugin that utilize tcpdump and Wireshark to start
a remote capture on any pod in your Kubernetes cluster.

#### Installation

1. Install `krew` if not installed yet  ( <https://github.com/kubernetes-sigs/krew> )
   * See docs for more information:
     * <https://krew.sigs.k8s.io/docs/user-guide/setup/install/>
     * <https://krew.sigs.k8s.io/docs/user-guide/quickstart/>

2. Install kubectl krew install sniff ( <https://github.com/eldadru/ksniff> )
   * `kubectl krew install sniff`

3. Install Wireshark ( <https://www.wireshark.org/download.html> )
   * add its folder with binaries `./Wireshark/App/Wireshark` to `PATH`

#### Catch data

1. Use correct context for kubectl, check pod names
   > `kubectl get pods`

2. Catch data:

   a. to file:

      `kubectl sniff <podname> -o svt1715.pcapng --image <ksniff_image> --tcpdump-image <tcpdump_image>`

      Press `Ctrl+C` to stop.

   b. to Wireshark:

      `kubectl sniff <podname> --image <ksniff_image> --tcpdump-image <tcpdump_image>`

      Close Wireshark to stop (don't forget to save captured data)

   > IMPORTANT: The collected data must contain the `COMMAND_GET_PROTOCOL_VERSION_V2` command from
   > the profiled applications, so after starting data collection, restart the profiled application
   > so that it sends this command.

   > NOTE: recommend to catch `15-30`Mb tcpdump file in (`1m-5m-10m` of work, depending on load).
   > Otherwise WireShark starting work really slow.

3. Values:

| Name            | Value                                                         |
|-----------------|---------------------------------------------------------------|
| `ksniff_image`  | hamravesh/ksniff-helper:v3 |
| `tcpdump_image` | maintained/tcpdump:latest  |

MUST use proxy because original Dockerhub is not available from internal cloud environment.

Example:
> `kubectl sniff esc-collector-service-854987ddb8-wxhnm --image hamravesh/ksniff-helper:v3 --tcpdump-image maintained/tcpdump:latest`

### Root privileges

> IMPORTANT! Pods must have the `root` privileges in order to get tcp dumps.

* Kubernetes:

Add this to namespace resource configuration:

```yaml
metadata:
  name: profiler
  labels:
    kubernetes.io/metadata.name: profiler
    pod-security.kubernetes.io/enforce: privileged
```  

* OpenShift:

Add this to load generator deployment:

```yaml
spec:
  template:
    spec:
      serviceAccountName: profiler
      securityContext: {}
      containers:
          name: esc-collector-service
          securityContext:
            privileged: true
      serviceAccount: profiler
```  

Make sure that this service account exists. For example:

```yaml
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: profiler
  labels:
    app: profiler
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: ops-profiler-profiler
rules:
  - apiGroups:
      - security.openshift.io
    resources:
      - securitycontextconstraints
    resourceNames:
      - privileged
    verbs:
      - use
---
kind: ClusterRoleBinding
apiVersion: rbac.authorization.k8s.io/v1
metadata:
  name: ops-profiler-profiler
subjects:
  - kind: ServiceAccount
    name: profiler
    namespace: ops-profiler
roleRef:
  kind: ClusterRole
  name: ops-profiler-profiler
  apiGroup: rbac.authorization.k8s.io
```

> NOTE: If you have updated existing resources, don't forget to restart the deployment
> so that the new pods with the necessary privileges appear.

### Next steps

After collecting data, you can prepare tcp, top, and thread dumps for use in the load generator.

To do this, check this [page](preparing_data.md).

### Additional guides

1. Ksniff:
   * <https://kubesandclouds.com/2021-01-20-ksniff/>
