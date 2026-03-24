# Usage

1. Build Load Generator and publish
   * see `doc/build.md` for more information

2. Create your values file for HELM (e.g., `values-sample.yaml`)
   * see `charts/cdt-load-generator/values.yaml` for reference
   * see `doc/settings.md` for supported environment variables

3. Deploy Load Generator on environment using HELM
   * `helm install cdt-load-generator -n profiler -f values-sample.yaml .`

4. Check performance:
   * In Grafana dashboards: `CDT Load Generator`, `Cassandra`, `Execution Statistics Collector`, etc.
   * In the logs of the load generator pod

> Don't forget to scale the deployment down to 0 after the test is complete,
> otherwise it will restart and run indefinitely.
