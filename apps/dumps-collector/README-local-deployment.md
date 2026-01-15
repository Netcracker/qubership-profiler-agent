# dumps-collector: Local Deployment Guide

Complete guide for deploying dumps-collector with PostgreSQL in local Kubernetes (OrbStack) using Helmfile.

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

### Scenario 1: Deploy Everything (PostgreSQL + dumps-collector)

```bash
cd apps/dumps-collector

# Deploy PostgreSQL + dumps-collector (default)
helmfile sync

# Wait for all pods to be ready (usually 2-3 minutes)
watch kubectl get pods -n postgres
watch kubectl get pods -n profiler
```

### Scenario 2: Deploy Only dumps-collector (PostgreSQL already exists)

If you've deployed PostgreSQL separately using pgskipper-operator or another method:

```bash
cd apps/dumps-collector

# Deploy only dumps-collector (skips PostgreSQL installation)
INSTALL_POSTGRES=false helmfile sync

# Wait for pods to be ready
watch kubectl get pods -n profiler
```

**Note**: When using `INSTALL_POSTGRES=false`, ensure:
- PostgreSQL service `pg-patroni.postgres.svc.cluster.local:5432` is accessible
- Database `postgres` exists with user `profiler` (or adjust `values-local.yaml`)

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

# Test PostgreSQL connection
psql -h localhost -p 5432 -U profiler -d postgres -c "SELECT version();"
# Password: profiler_password
```

### 4. Cleanup

```bash
# Stop port-forwards
helmfile -l type=port-forward destroy
# Or if PostgreSQL was skipped:
INSTALL_POSTGRES=false helmfile -l type=port-forward destroy

# Remove all deployments (including PostgreSQL if installed)
helmfile destroy

# Or remove only dumps-collector (keep PostgreSQL)
INSTALL_POSTGRES=false helmfile destroy

# Optional: Clean up PVCs (will delete all data!)
kubectl delete pvc -n postgres --all
kubectl delete pvc -n profiler --all
```

## Architecture

### Components

```
┌─────────────────────────────────────────────────────────────┐
│                    OrbStack Kubernetes                      │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌─────────────────────────────────────────────────────┐  │
│  │  Namespace: postgres                                │  │
│  │  ┌──────────────────────┐  ┌──────────────────────┐│  │
│  │  │ patroni-core         │  │ patroni-services     ││  │
│  │  │ (Operator)           │  │ (Operator)           ││  │
│  │  └──────────────────────┘  └──────────────────────┘│  │
│  │                                                      │  │
│  │  ┌──────────────────────┐  ┌──────────────────────┐│  │
│  │  │ pg-patroni-node-0    │  │ pg-patroni-node-1    ││  │
│  │  │ (Primary)            │  │ (Replica)            ││  │
│  │  │ PostgreSQL 16        │  │ PostgreSQL 16        ││  │
│  │  └──────────────────────┘  └──────────────────────┘│  │
│  │                                                      │  │
│  │  Services:                                          │  │
│  │  • pg-patroni:5432 (RW)                            │  │
│  │  • pg-patroni-ro:5432 (RO)                         │  │
│  └─────────────────────────────────────────────────────┘  │
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
│  │  └──────────────────────────────────────────────┘  │  │
│  │                                                      │  │
│  │  Service: cloud-profiler-dumps-collector           │  │
│  │  • :8080 (HTTP/WebDAV)                             │  │
│  │  • :8000 (API)                                     │  │
│  └─────────────────────────────────────────────────────┘  │
│                                                             │
└─────────────────────────────────────────────────────────────┘
         │                    │                    │
         │ (port-forward)     │ (port-forward)     │ (port-forward)
         ↓                    ↓                    ↓
    localhost:8080       localhost:8000       localhost:5432
```

### Data Flow

1. **Dump Upload**: Java agents → `PUT http://localhost:8080/diagnostic/{path}` → Nginx WebDAV → PersistentVolume
2. **Indexing**: prf_dump_writer scans PV every minute → stores metadata in PostgreSQL
3. **Download**: User → `GET http://localhost:8000/cdt/v2/download?...` → reads from PV/ZIP archives

## Detailed Commands

### Deployment Management

```bash
# Deploy everything (default: with PostgreSQL)
helmfile sync

# Deploy only dumps-collector (skip PostgreSQL)
INSTALL_POSTGRES=false helmfile sync

# Deploy only PostgreSQL
helmfile -l component=postgres sync

# Deploy only application
helmfile -l component=application sync

# Deploy everything except port-forwards
helmfile -l type!=port-forward sync

# Check what will be deployed (dry-run)
helmfile diff
INSTALL_POSTGRES=false helmfile diff  # Without PostgreSQL

# Update existing deployment
helmfile apply

# List all releases
helmfile list
INSTALL_POSTGRES=false helmfile list  # Show only dumps-collector releases
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
tail -f /tmp/pf-postgres.log

# Kill specific port-forward manually
kill $(cat /tmp/pf-dumps-collector-http.pid)

# Stop all port-forwards
helmfile -l type=port-forward destroy
```

### Status Checks

```bash
# Check all pods
kubectl get pods -A

# Check PostgreSQL pods
kubectl get pods -n postgres -l app=postgres

# Check dumps-collector pods
kubectl get pods -n profiler -l app.kubernetes.io/name=dumps-collector

# Check services
kubectl get svc -n postgres
kubectl get svc -n profiler

# Check PVCs
kubectl get pvc -n postgres
kubectl get pvc -n profiler

# Check logs
kubectl logs -n profiler -l app.kubernetes.io/name=dumps-collector -f
kubectl logs -n postgres -l app=postgres -f
```

### Database Access

```bash
# Via port-forward
psql -h localhost -p 5432 -U profiler -d postgres
# Password: profiler_password

# Direct from cluster
kubectl exec -it -n postgres pg-patroni-node-0 -- \
  psql -U profiler -d postgres

# Check Patroni cluster status
kubectl exec -n postgres pg-patroni-node-0 -- \
  patronictl -c /home/postgres/patroni.yml list
```

## Configuration

### Environment Variables

You can override default values using environment variables:

```bash
# Skip PostgreSQL installation (default: true)
export INSTALL_POSTGRES=false  # Use if PostgreSQL already deployed separately

# Custom namespaces
export NAMESPACE=my-profiler
export PG_NAMESPACE=my-postgres

# Custom storage class
export STORAGE_CLASS=nfs-client

# Custom pgskipper-operator path
export PGSKIPPER_PATH=/path/to/pgskipper-operator

# Deploy with custom settings
helmfile sync
```

**Key Variables**:
- `INSTALL_POSTGRES` (default: `true`): Set to `false` to skip PostgreSQL deployment
- `NAMESPACE` (default: `profiler`): Namespace for dumps-collector
- `PG_NAMESPACE` (default: `postgres`): Namespace for PostgreSQL
- `STORAGE_CLASS` (default: `local-path`): StorageClass for PVCs
- `PGSKIPPER_PATH` (default: `../../../pgskipper-operator`): Path to pgskipper-operator charts

### Customizing values-local.yaml

Edit `values-local.yaml` to customize:

- PostgreSQL credentials
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

### PostgreSQL Access

```bash
# Connect via psql
psql -h localhost -p 5432 -U profiler -d postgres

# View dumps metadata
psql -h localhost -p 5432 -U profiler -d postgres -c "\dt"

# Check timeline records
psql -h localhost -p 5432 -U profiler -d postgres -c "SELECT * FROM timelines LIMIT 10;"

# Check heap dumps
psql -h localhost -p 5432 -U profiler -d postgres -c "SELECT * FROM heap_dumps LIMIT 10;"
```

## Troubleshooting

### PostgreSQL Issues

#### Pods stuck in Pending

```bash
# Check PVC status
kubectl describe pvc -n postgres

# Check storage class
kubectl get storageclass

# Solution: Ensure 'local-path' storage class exists
kubectl get storageclass local-path
```

#### Connection refused

```bash
# Wait for cluster initialization (takes 30-60 seconds)
kubectl wait --for=condition=ready --timeout=300s pods -l app=postgres -n postgres

# Check PostgreSQL logs
kubectl logs -n postgres -l app=postgres --tail=100

# Verify services are created
kubectl get svc -n postgres | grep pg-patroni
```

#### Database authentication failed

```bash
# Verify credentials in values-local.yaml match
grep -A5 "postgres:" values-local.yaml

# Check secret
kubectl get secret -n profiler

# Restart dumps-collector to pick up new credentials
kubectl rollout restart deployment -n profiler
```

### dumps-collector Issues

#### Pod crash loop

```bash
# Check logs
kubectl logs -n profiler -l app.kubernetes.io/name=dumps-collector --tail=200

# Common causes:
# 1. PostgreSQL not ready - wait for PostgreSQL pods
# 2. Database migration failed - check migration logs
# 3. Invalid configuration - verify values-local.yaml

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

#### Dumps not indexed in database

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
lsof -i :5432

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
kubectl get svc -n postgres pg-patroni

# Check if pods are ready
kubectl get pods -n profiler
kubectl get pods -n postgres

# Manual port-forward test
kubectl port-forward -n profiler svc/cloud-profiler-dumps-collector 8080:8080
```

### General Debugging

```bash
# Check Helm releases
helmfile list

# Check Kubernetes resources
kubectl get all -n profiler
kubectl get all -n postgres

# Describe problem pod
kubectl describe pod -n profiler <pod-name>

# Get into container for debugging
kubectl exec -it -n profiler deployment/cloud-profiler-dumps-collector -- /bin/sh

# Check resource usage
kubectl top pods -n profiler
kubectl top pods -n postgres
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

### Custom PostgreSQL Configuration

Edit `helmfile.yaml` to modify PostgreSQL settings:

```yaml
patroni:
  replicas: 3  # More replicas
  storage:
    size: 20Gi  # More storage
  postgreSQLParams:
    - "max_connections: 500"
    - "shared_buffers: 256MB"
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

```
apps/dumps-collector/
├── helmfile.yaml                 # Main deployment configuration
├── values-local.yaml             # Local development values
├── README-local-deployment.md    # This file
└── charts/
    └── port-forward/             # Port-forward helper chart
        ├── Chart.yaml
        ├── values.yaml
        └── templates/
            └── NOTES.txt
```

## Useful Resources

- **dumps-collector Chart**: `../../charts/dumps-collector/`
- **pgskipper-operator**: `/Users/vlsi/Documents/code/qubership/pgskipper-operator`
- **Helmfile Documentation**: https://helmfile.readthedocs.io/
- **Helm Documentation**: https://helm.sh/docs/
- **OrbStack Documentation**: https://orbstack.dev/docs

## Support

For issues or questions:

1. Check the [Troubleshooting](#troubleshooting) section
2. Review logs: `kubectl logs -n profiler -l app.kubernetes.io/name=dumps-collector -f`
3. Check GitHub issues: https://github.com/Netcracker/qubership-profiler-backend/issues

---

**Happy Profiling! 🚀**
