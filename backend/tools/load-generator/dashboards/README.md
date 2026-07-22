# Stand dashboards

Grafana dashboards as code for the load-testing stand (load-testing-plan.md §6.2). Every `*.json` file here is wrapped
into a `GrafanaDashboard` CR by the `monitoring-crs` release of `../deploy/helmfile.yaml` — drop a dashboard JSON in,
re-apply, done. File name (without `.json`) becomes the CR name, so keep names kebab-case.
