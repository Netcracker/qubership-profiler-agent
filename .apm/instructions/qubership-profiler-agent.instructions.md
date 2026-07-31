---
description: Repository-wide guidance for contributing to Qubership Profiler Agent.
applyTo: "**/*"
---

## Scope

- This repository builds the Qubership JVM profiler agent, instrumentation plugins, diagnostic tools, and the
  cloud-profiler backend.
- Keep repository-wide guidance here and subsystem-specific guidance next to the subsystem it governs.

## Repository map

- `agent/`, `runtime/`, `instrumenter/`, `dumper/`, and `profiler/` contain the core Gradle modules for the Java agent,
  runtime instrumentation, data capture, and profiler application.
- `plugins/` contains instrumentation modules; `proto-definition/` contains agent and collector transport types.
- `profiler-ui/` is the profiler web application built through Gradle and Yarn.
- `diagtools/` is a Go diagnostic CLI packaged by the Gradle build.
- `backend/` contains the cloud-profiler services, libraries, tools, deployment assets, and a separate React UI.
- `it-test/`, `it-e2e/`, `testkit/`, `test-app/`, and `sample-apps/` support integration and end-to-end verification.

## Commands

- Launch Gradle with Java 17 or 21 and use the checked-in wrapper.
- Run the full Gradle gate from the root with `./gradlew --no-parallel build`.
- Run focused Gradle tests with `./gradlew :<module>:test`, for example `./gradlew :dumper:test`.
- Check JVM source style with `./gradlew styleCheck`; use `./gradlew style` only when you intend to rewrite formatting.
- From `diagtools/`, run `make fmt vet test` for Go changes; cross-platform builds also require Zig as documented there.
- From the root, build and test backend applications with `make -C backend apps-build apps-test`; from `backend/`, run
  `go test ./...` for changes to Go libraries.
- From `backend/apps/ui/`, run `npm run typecheck`, `npm run test`, and `npm run build` for React UI changes.

## Non-obvious invariants

- Treat `AGENTS.md` and deployed agent assets as APM outputs. Edit `apm.yml` or a local primitive under `.apm/`, then
  run `apm compile`; do not hand-edit generated outputs.
- Keep root `CLAUDE.md` as the canonical one-line `@AGENTS.md` wrapper; it is not an independent authoring surface.
- `settings.gradle.kts` controls build inclusion, while `nmcpAggregation` in `build.gradle.kts` separately selects
  Central Portal artifacts. Update the applicable list or lists for the intended change, then run the full Gradle gate.

## Done when

- Focused tests for every changed subsystem pass, and cross-Gradle-module changes pass `./gradlew --no-parallel build`.
- JVM changes pass `./gradlew styleCheck`; backend and UI changes pass the applicable commands listed above.
- When APM inputs change, re-running `apm compile` leaves the tracked generated outputs unchanged.
- The final report lists checks that ran and any checks that could not run.

## Context routing

- Before changing `backend/`, read `backend/CLAUDE.md` for mandatory design documents, workflow rules, and protocol
  terminology that are not repeated here.
- Before changing `diagtools/` packaging or cross-compilation, read `diagtools/README.md` and `diagtools/Makefile` for
  platform and Zig requirements.
- For workflows that consume Qubership actions or Netcracker templates, use the installed Qubership workflow skills.
