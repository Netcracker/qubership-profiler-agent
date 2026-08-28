# dumps-collector: Local Deployment Guide

Complete guide for deploying dumps-collector in local Kubernetes (OrbStack) using Helmfile. dumps-collector is
self-contained: it stores dump metadata in an embedded SQLite database on its PersistentVolume, so no external
database is required.

## Table of Contents

- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Architecture](#architecture)
- [Detailed Commands](#detailed-commands)
- [Configuration](#configuration)
- [Accessing Services](#accessing-services)
- [Troubleshooting](#troubleshooting)
- [Advanced Usage](#advanced-usage)

## Prerequisites

### Required Tools

- **OrbStack** - Local Kubernetes cluster (or any local K8s like Minikube, Kind)
- **kubectl** v1.25+ - Kubernetes CLI
- **Helm** v3.x - Package manager for Kubernetes
- **helmfile** v0.140+ - Declarative Helm deployment tool

### Install helmfile

```bash
# macOS (Homebrew)
brew install helmfile

# Or download from GitHub releases
# https://github.com/helmfile/helmfile/releases
```

### Verify Installation

```bash
kubectl version --client
helm version
helmfile --version
docker context show  # Should output: orbstack
```

### OrbStack Configuration

Ensure OrbStack Kubernetes is running:

```bash
kubectl cluster-info
kubectl get nodes
kubectl get storageclass  # Should show 'local-path' (default)
```

## Quick Start

### 1. Deploy dumps-collector

```bash
cd apps/dumps-collector

# Deploy dumps-collector
helmfile sync

# Wait for pods to be ready (usually 1-2 minutes)
watch kubectl get pods -n profiler
```

### 2. Start Port-Forwards

```bash
# Start all port-forwards in background
helmfile -l type=port-forward sync
```

### 3. Verify Access

```bash
# Test dumps-collector HTTP endpoint
curl http://localhost:8080/health
# Expected: {"status":"UP"}

# Test dumps-collector API endpoint
curl http://localhost:8000/esc/health
# Expected: 204 No Content
```

### 4. Cleanup

```bash
# Stop port-forwards
helmfile -l type=port-forward destroy

# Remove the deployment
helmfile destroy

# Optional: Clean up PVCs (will delete all data!)
kubectl delete pvc -n profiler --all
```

## Architecture

### Components

```text
┌─────────────────────────────────────────────────────────────┐
│                    OrbStack Kubernetes                      │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌─────────────────────────────────────────────────────┐  │
│  │  Namespace: profiler                                │  │
│  │  ┌──────────────────────────────────────────────┐  │  │
│  │  │ dumps-collector                              │  │  │
│  │  │  ┌────────────┐  ┌─────────────────────┐   │  │  │
│  │  │  │   Nginx    │  │  prf_dump_writer    │   │  │  │
│  │  │  │  :8080     │  │  (Go app)           │   │  │  │
│  │  │  │  WebDAV    │  │  :8000              │   │  │  │
│  │  │  └────────────┘  └─────────────────────┘   │  │  │
│  │  │                                              │  │  │
│  │  │  PersistentVolume: 5Gi (local-path)         │  │  │
│  │  │    • dumps under /diag/diagnostic           │  │  │
│  │  │    • metadata in /diag/profiler_dumps.db    │  │  │
│  │  └──────────────────────────────────────────────┘  │  │
│  │                                                      │  │
│  │  Service: cloud-profiler-dumps-collector           │  │
│  │  • :8080 (HTTP/WebDAV)                             │  │
│  │  • :8000 (API)                                     │  │
│  └─────────────────────────────────────────────────────┘  │
│                                                             │
└─────────────────────────────────────────────────────────────┘
         │                    │
         │ (port-forward)     │ (port-forward)
         ↓                    ↓
    localhost:8080       localhost:8000
```

### Data Flow

1. **Dump Upload**: Java agents → `PUT http://localhost:8080/diagnostic/{path}` → Nginx WebDAV → PersistentVolume
2. **Indexing**: prf_dump_writer scans the PV every minute → stores metadata in SQLite (`/diag/profiler_dumps.db`)
3. **Download**: User → `GET http://localhost:8000/cdt/v2/download?...` → reads from PV/ZIP archives

## Detailed Commands

### Deployment Management

```bash
# Deploy everything
helmfile sync

# Deploy only the application
helmfile -l component=application sync

# Deploy everything except port-forwards
helmfile -l type!=port-forward sync

# Check what will be deployed (dry-run)
helmfile diff

# Update existing deployment
helmfile apply

# List all releases
helmfile list
```

### Port-Forward Management

```bash
# Start all port-forwards
helmfile -l type=port-forward sync

# Start specific port-forward
helmfile -l name=port-forward-dumps-collector-http sync

# Check if port-forwards are running
ps aux | grep "kubectl port-forward"

# Check port-forward logs
tail -f /tmp/pf-dumps-collector-http.log
tail -f /tmp/pf-dumps-collector-api.log

# Kill specific port-forward manually
kill $(cat /tmp/pf-dumps-collector-http.pid)

# Stop all port-forwards
helmfile -l type=port-forward destroy
```

### Status Checks

```bash
# Check all pods
kubectl get pods -A

# Check dumps-collector pods
kubectl get pods -n profiler -l app.kubernetes.io/name=dumps-collector

# Check services
kubectl get svc -n profiler

# Check PVCs
kubectl get pvc -n profiler

# Check logs
kubectl logs -n profiler -l app.kubernetes.io/name=dumps-collector -f
```

### Metadata Access

The metadata lives in an embedded SQLite database on the PV at `/diag/profiler_dumps.db`.

```bash
# Copy the database out of the pod for inspection
kubectl cp -n profiler \
  "$(kubectl get pod -n profiler -l app.kubernetes.io/name=dumps-collector -o name | head -1 | cut -d/ -f2)":/diag/profiler_dumps.db \
  ./profiler_dumps.db

# Inspect it with a local sqlite3 client
sqlite3 ./profiler_dumps.db ".tables"
sqlite3 ./profiler_dumps.db "SELECT * FROM heap_dumps LIMIT 10;"
```

## Configuration

### Environment Variables

You can override default values using environment variables:

```bash
# Custom namespace
export NAMESPACE=my-profiler

# Custom storage class
export STORAGE_CLASS=nfs-client

# Deploy with custom settings
helmfile sync
```

**Key Variables**:

- `NAMESPACE` (default: `profiler`): Namespace for dumps-collector
- `STORAGE_CLASS` (default: `local-path`): StorageClass for PVCs

### Customizing values-local.yaml

Edit `values-local.yaml` to customize:

- Storage size and class
- Resource limits
- Retention policies
- Log levels

Example:

```yaml
cloud:
  dumpsStorage:
    size: 10Gi  # Increase storage
    daysDeleteAfter: 14  # Keep dumps longer

dumpsCollector:
  replicas: 2  # Scale up
  resources:
    limits:
      cpu: 1000m
      memory: 1Gi
```

Then apply:

```bash
helmfile apply
```

## Accessing Services

### dumps-collector Endpoints

After starting port-forwards:

```bash
# Health checks
curl http://localhost:8080/health
curl http://localhost:8000/esc/health

# Metrics (Prometheus format)
curl http://localhost:8000/esc/metrics

# Upload a dump via WebDAV
curl -X PUT \
  -H "Content-Type: application/octet-stream" \
  --data-binary @/path/to/dump.hprof.zip \
  http://localhost:8080/diagnostic/test-ns/2024/12/17/15/30/00/test-pod-abc123/dump.hprof.zip

# Download thread dumps
curl "http://localhost:8000/cdt/v2/download?dateFrom=1734422400000&dateTo=1734426000000&type=td&namespace=test-ns" \
  -o dumps.zip

# Download heap dump by handle
curl "http://localhost:8000/cdt/v2/heaps/download/test-pod-abc123-heap-1734422400000" \
  -o heap.hprof.zip
```

## Troubleshooting

### dumps-collector Issues

#### Pod crash loop

```bash
# Check logs
kubectl logs -n profiler -l app.kubernetes.io/name=dumps-collector --tail=200

# Common causes:
# 1. PV not mounted or not writable - check the PVC and security context
# 2. Invalid configuration - verify values-local.yaml

# Check events
kubectl get events -n profiler --sort-by='.lastTimestamp'
```

#### Cannot write dumps (403 Forbidden)

```bash
# Check PVC is mounted
kubectl exec -n profiler -it deployment/cloud-profiler-dumps-collector -- ls -la /diag

# Check permissions
kubectl exec -n profiler -it deployment/cloud-profiler-dumps-collector -- \
  sh -c "touch /diag/test && rm /diag/test"

# If permission denied, check security context in chart
```

#### Dumps not indexed

```bash
# Check prf_dump_writer logs
kubectl logs -n profiler -l app.kubernetes.io/name=dumps-collector -f | grep "Insert Task"

# Verify file structure on PV
kubectl exec -n profiler -it deployment/cloud-profiler-dumps-collector -- \
  find /diag/diagnostic -type f | head -20

# Expected structure:
# /diag/diagnostic/{namespace}/{year}/{month}/{day}/{hour}/{minute}/{second}/{pod}/{file}
```

### Port-Forward Issues

#### Port already in use

```bash
# Find process using the port
lsof -i :8080
lsof -i :8000

# Kill the process
kill $(lsof -t -i :8080)

# Restart port-forwards
helmfile -l type=port-forward destroy
helmfile -l type=port-forward sync
```

#### Port-forward dies immediately

```bash
# Check if service exists
kubectl get svc -n profiler cloud-profiler-dumps-collector

# Check if pods are ready
kubectl get pods -n profiler

# Manual port-forward test
kubectl port-forward -n profiler svc/cloud-profiler-dumps-collector 8080:8080
```

### General Debugging

```bash
# Check Helm releases
helmfile list

# Check Kubernetes resources
kubectl get all -n profiler

# Describe problem pod
kubectl describe pod -n profiler <pod-name>

# Get into container for debugging
kubectl exec -it -n profiler deployment/cloud-profiler-dumps-collector -- /bin/sh

# Check resource usage
kubectl top pods -n profiler
```

## Advanced Usage

### Using Different Storage Classes

For NFS or other storage classes:

```bash
# Override storage class
export STORAGE_CLASS=nfs-client
helmfile sync

# Or edit values-local.yaml:
cloud:
  dumpsStorage:
    storageClassName: nfs-client
```

### Scaling dumps-collector

```bash
# Edit values-local.yaml
dumpsCollector:
  replicas: 3

# Apply changes
helmfile apply

# Note: Multiple replicas share the same PV (ReadWriteOnce limitation)
# Consider using ReadWriteMany storage class for multi-replica setup
```

### Enabling Monitoring

If you have Prometheus Operator installed:

```yaml
# In values-local.yaml
dumpsCollector:
  monitoring:
    enabled: true
    port: http
    path: /esc/metrics
```

### Multi-Environment Setup

```bash
# Create environment-specific values
cp values-local.yaml values-staging.yaml

# Deploy to different namespace
export NAMESPACE=profiler-staging
helmfile -f helmfile.yaml -e staging sync
```

## File Structure

```text
apps/dumps-collector/
├── helmfile.yaml.gotmpl         # Main deployment configuration
├── values-local.yaml            # Local development values
├── README-local-deployment.md   # This file
└── charts/
    └── port-forward/            # Port-forward helper chart
        ├── Chart.yaml
        ├── values.yaml
        └── templates/
            └── NOTES.txt
```

## Useful Resources

- **dumps-collector Chart**: `../../charts/dumps-collector/`
- **Helmfile Documentation**: <https://helmfile.readthedocs.io/>
- **Helm Documentation**: <https://helm.sh/docs/>
- **OrbStack Documentation**: <https://orbstack.dev/docs>

## Support

For issues or questions:

1. Check the [Troubleshooting](#troubleshooting) section
2. Review logs: `kubectl logs -n profiler -l app.kubernetes.io/name=dumps-collector -f`
3. Check GitHub issues: <https://github.com/Netcracker/qubership-profiler-backend/issues>

---

**Happy Profiling! 🚀**
