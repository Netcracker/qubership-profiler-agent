# dumps-collector Quick Start

## 🚀 One-Command Deploy

```bash
cd apps/dumps-collector

# 1. Deploy dumps-collector
helmfile sync

# 2. Start port-forwards
helmfile -l type=port-forward sync
```

dumps-collector is self-contained: it stores dump metadata in an embedded SQLite database on its PersistentVolume
(`/diag/profiler_dumps.db`), so no external database is needed.

## ✅ Verify

```bash
# Check dumps-collector
curl http://localhost:8080/health
curl http://localhost:8000/esc/health
```

## 🧹 Cleanup

```bash
# Stop port-forwards
helmfile -l type=port-forward destroy

# Remove the deployment
helmfile destroy
```

## 📋 Useful Commands

```bash
# List all releases
helmfile list

# Deploy only the application (skip port-forwards)
helmfile -l component=application sync

# Check status
kubectl get pods -n profiler

# View logs
kubectl logs -n profiler -l app.kubernetes.io/name=dumps-collector -f

# Check port-forwards are running
ps aux | grep "kubectl port-forward"

# Port-forward logs
tail -f /tmp/pf-*.log
```

## 📚 Full Documentation

See [README-local-deployment.md](./README-local-deployment.md) for complete documentation.

## 🔗 Endpoints (after port-forward)

- **dumps-collector HTTP/WebDAV**: <http://localhost:8080>
- **dumps-collector API**: <http://localhost:8000>

## 📦 What Gets Deployed

| Component | Namespace | Replicas | Storage |
|-----------|-----------|----------|---------|
| dumps-collector | profiler | 1 | 5Gi |

## 🎯 Architecture

```text
[Java Agents] → PUT /diagnostic → [Nginx:8080] → [PV Storage]
                                         ↓
                                   [prf_dump_writer:8000]
                                         ↓
                                   [SQLite] (metadata, on the PV)
                                         ↓
[Users] ← GET /cdt/v2/download ← [API:8000] ← [PV/ZIP]
```

## 🔧 Configuration

All configuration in `values-local.yaml`:

- Metadata store: SQLite at `/diag/profiler_dumps.db` (on the PV)
- Storage: `local-path` StorageClass (OrbStack), 5Gi mounted at `/diag`
- Retention: archive after 2h, delete after 7 days, at most 5 heap dumps per pod

## ⚠️ Prerequisites

- OrbStack with Kubernetes running
- kubectl, helm, helmfile installed

## 🐛 Troubleshooting

**Helmfile errors?**

```bash
helmfile list  # Check all releases
helmfile diff  # See what would change
```

**Pods not starting?**

```bash
kubectl get pods -A
kubectl describe pod -n profiler <pod-name>
kubectl logs -n profiler <pod-name>
```

**Port-forward not working?**

```bash
lsof -i :8080  # Check if port is in use
helmfile -l type=port-forward destroy  # Stop all
helmfile -l type=port-forward sync     # Restart
```

---

**Need help?** Check the full documentation in [README-local-deployment.md](./README-local-deployment.md)
