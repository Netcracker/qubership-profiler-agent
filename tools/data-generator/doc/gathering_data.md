# Data

## Gather data

Easiest way to get data is to catch TCP communication with `tcpdump`.

### Gather with `ksniff`

Useful plugin for kubectl

#### Installation

1. Install `krew` if not installed yet ( <https://github.com/kubernetes-sigs/krew> )

- See docs for more information:
  - <https://krew.sigs.k8s.io/docs/user-guide/setup/install/>
  - <https://krew.sigs.k8s.io/docs/user-guide/quickstart/>

1. Install kubectl krew install sniff ( <https://github.com/eldadru/ksniff> )

- `kubectl krew install sniff`

1. Install Wireshark ( <https://www.wireshark.org/download.html> )

- add its folder with binaries `./Wireshark/App/Wireshark` to `PATH`

#### Catch data

1. Use correct context for kubectl, check pod names
   > `kubectl get pods`

1. Catch data:  
  a. Catch data to file:

    - `kubectl sniff <podname> -o svt1715.pcapng --image <ksniff_image> --tcpdump-image <tcpdump_image>`

    Press `Ctrl+C` to stop.  
  
    b. Catching data and sending it Wireshark:

    - `kubectl sniff <podname> --image <ksniff_image> --tcpdump-image <tcpdump_image>`

      Close Wireshark to stop

      > NOTE: recommend to catch `15-30`Mb tcpdump file in (`1m-5m-10m` of work, depending on load).
      > Otherwise WireShark starting work really slow.

1. Values:

| Name            | Value                                                         |
| --------------- | ------------------------------------------------------------- |
| `ksniff_image`  | hamravesh/ksniff-helper:v3 |
| `tcpdump_image` | maintained/tcpdump:latest  |

MUST use proxy because original Dockerhub is not available from internal cloud environment.

Example:

> `kubectl sniff esc-collector-service-854987ddb8-wxhnm
> --image hamravesh/ksniff-helper:v3
> --tcpdump-image maintained/tcpdump:latest`

### Root privileges

> IMPORTANT! Pods should have ability to use `root` priligies in order to get tcp dumps.

- Kubernetes:

Check namespace resource configuration:

```yaml
metadata:
  name: profiler
  labels:
    kubernetes.io/metadata.name: profiler
    pod-security.kubernetes.io/enforce: privileged
```

- OpenShift:

Check service deployment:

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

#### Additional guides

1. Ksniff:

   - <https://kubesandclouds.com/2021-01-20-ksniff/>
