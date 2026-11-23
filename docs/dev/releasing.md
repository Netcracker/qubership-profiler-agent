# Releasing Qubership Profiler

A release publishes the Java agent and its plugins to Maven Central (group `org.qubership.profiler`), creates the
`v<version>` tag and the GitHub release, and publishes the `qubership-profiler-dumps-collector`,
`qubership-profiler-backend`, and `qubership-profiler-transfer` images for `linux/amd64` and `linux/arm64`.

## Start a release manually

1. Open the [release workflow](https://github.com/Netcracker/qubership-profiler-agent/actions/workflows/release.yaml).
1. Click `Run workflow`.
1. Select the branch to release: `main` or a `release/**` branch.
1. Leave `version` empty to release the version in `gradle.properties` (`profiler.version`), or enter another
   version in the `x.y.z` form.
1. Click `Run workflow`.

The release workflow performs the following steps:

1. Fails unless it runs on `main` or a `release/**` branch, the version is empty or has the `x.y.z` form, and at least
   one of the `PUBLISH_TO_CENTRAL` and `PUBLISH_TO_GITHUB_PACKAGES` repository variables enables publishing.
1. Fails if the `v<version>` tag already exists.
1. Commits `profiler.version=<version>` to `gradle.properties` and pushes it to the selected branch.
1. Builds the artifacts and publishes them to GitHub Packages when `PUBLISH_TO_GITHUB_PACKAGES` is `true`, then to the
   Central Portal unless `PUBLISH_TO_CENTRAL` is `false`.
1. Creates the `v<version>` tag and publishes the GitHub release.
1. Commits the next patch version to `gradle.properties` and pushes it to the selected branch.
1. Builds and publishes the Docker images from the release tag.

The workflow uses [Release Drafter](https://github.com/release-drafter/release-drafter) to prepare the release notes,
and groups the changes by pull request labels. To adjust the notes, change the labels on the pull requests.

## Automatic releases

The [Auto Release workflow](../../.github/workflows/auto-release.yaml) runs when the weekly scheduled security scan
completes and on Mondays, and starts the release workflow on `main` when a Dependabot or source code scanning alert has
been fixed since the latest release, or Grype or Trivy finds a HIGH or CRITICAL vulnerability in `latest` that is absent
from `main`. Image comparisons use categories of the form `<image>:<tag>@<platform>` and match the advisory and package
across all locations in each image. Scorecard alerts and code scanning rules without a security severity do not count
as vulnerabilities. Otherwise the workflow releases when about a month has passed since the latest release. It does
nothing when `main` has no commits since the latest release, or while a release workflow run is in progress. The latest
release is the creation time of the newest successful release workflow run on `main`, kept across re-runs, so a fix
merged while a release runs still counts. Without such a run, the latest GitHub release is used. Run it manually with
`dry-run` to see the decision, the alerts it counted, and any skipped image comparisons without releasing.

A Dependabot alert counts only for a runtime dependency outside `apps/examples/` and the `it-*` integration test
directories, and only when the same advisory is not still open for the same package in such a dependency. Alerts on
`settings.gradle.kts` do not count: the Gradle dependency submission sets no scope, and that manifest also covers
build tools such as Checkstyle and Error Prone. Fixes in the Java dependencies therefore ship with the monthly release
unless an image scan finds them in a published image.

When the newest completed release workflow run on `main` did not succeed, for example failed, was cancelled, or timed
out, the Auto Release workflow fails and starts nothing, because that run may have stopped after publishing to Maven
Central, tagging the release, or pushing the version bump. Re-run its failed jobs, or start the release workflow once
the cause is fixed; automatic releases resume after a release workflow run on `main` succeeds. The workflow also skips
the release when a release workflow run starts or finishes while it reads the alerts and images.

Each image pair requires error-free scans of the same image and platform with the same scanner version and
configuration. Both analyses must be for the current `main` commit, at most 48 hours old, and at most 24 hours apart.
Their scan workflows must have completed, and a run whose other jobs failed still counts. They must have started after
the latest release and all runs of the release and backend image build workflows on `main`, including failed runs that
may have published some images. A running publisher, missing or incomplete report, or a branch, image publication, or
release change during comparison invalidates the image comparison. Skipped image comparisons leave Dependabot, source
code scanning, and monthly release decisions available.
