# dumps-collector Quick Start

## 🚀 One-Command Deploy

### Option A: Deploy with PostgreSQL (Default)

```bash
cd apps/dumps-collector

# 1. Deploy everything (PostgreSQL + dumps-collector)
helmfile sync

# 2. Start port-forwards
helmfile -l type=port-forward sync
```

### Option B: Deploy without PostgreSQL (if already deployed separately)

```bash
cd apps/dumps-collector

# 1. Deploy only dumps-collector (assumes PostgreSQL already exists)
INSTALL_POSTGRES=false helmfile sync

# 2. Start port-forwards (only for dumps-collector)
INSTALL_POSTGRES=false helmfile -l type=port-forward sync
```

## ✅ Verify

```bash
# Check dumps-collector
curl http://localhost:8080/health
curl http://localhost:8000/esc/health

# Check PostgreSQL
psql -h localhost -p 5432 -U profiler -d postgres
# Password: profiler_password
```

## 🧹 Cleanup

```bash
# Stop port-forwards
helmfile -l type=port-forward destroy

# Remove everything (including PostgreSQL if installed)
helmfile destroy

# Or remove only dumps-collector (keep PostgreSQL)
INSTALL_POSTGRES=false helmfile destroy
```

## 📋 Useful Commands

```bash
# List all releases
helmfile list

# Deploy only PostgreSQL
helmfile -l component=postgres sync

# Deploy only application
helmfile -l component=application sync

# Check status
kubectl get pods -n postgres
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

- **dumps-collector HTTP**: http://localhost:8080
- **dumps-collector API**: http://localhost:8000
- **PostgreSQL**: localhost:5432

## 📦 What Gets Deployed

| Component | Namespace | Replicas | Storage |
|-----------|-----------|----------|---------|
| PostgreSQL (Patroni) | postgres | 2 | 10Gi × 2 |
| dumps-collector | profiler | 1 | 5Gi |

## 🎯 Architecture

```
[Java Agents] → PUT /diagnostic → [Nginx:8080] → [PV Storage]
                                         ↓
                                   [prf_dump_writer]
                                         ↓
                                   [PostgreSQL] (metadata)
                                         ↓
[Users] ← GET /cdt/v2/download ← [API:8000] ← [PV/ZIP]
```

## 🔧 Configuration

All configuration in `values-local.yaml`:
- PostgreSQL: `pg-patroni.postgres.svc.cluster.local:5432`
- Credentials: `profiler` / `profiler_password`
- Storage: `local-path` StorageClass (OrbStack)
- Retention: Archive after 2h, Delete after 7 days

## ⚠️ Prerequisites

- OrbStack with Kubernetes running
- kubectl, helm, helmfile installed
- pgskipper-operator at `../../../pgskipper-operator`

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

**PostgreSQL connection issues?**
```bash
kubectl get svc -n postgres  # Verify service exists
kubectl logs -n postgres -l app=postgres  # Check logs
```

---

**Need help?** Check the full documentation in [README-local-deployment.md](./README-local-deployment.md)
