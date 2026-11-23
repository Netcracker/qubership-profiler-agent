# cloud-profiler-dump-collector

To deploy it to a local Kubernetes cluster, see the
[local deployment quick start](../../deploy/dumps-collector/QUICKSTART.md).

## Build the Docker image

Run from the repository root with JDK 21 and Docker installed:

```bash
make -C apps/dumps-collector docker-build
```

The image includes the Java agent installer built from the current checkout and
`diag-bootstrap.sh` from the sources. The default image tag is
`cloud-profiler-dumps-collector`. Set `DOCKER_IMAGE` to use another tag.

To prepare the installer and run Docker separately:

```bash
make -C apps/dumps-collector docker-prepare
docker build -f apps/dumps-collector/Dockerfile -t dumps-collector:local .
```

Run `docker-prepare` again after changing the agent sources. CI prepares the
installer before building the image; release builds use the release tag for both
the installer and the Docker context.
