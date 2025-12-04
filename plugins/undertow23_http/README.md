# Undertow23 Plugin Configuration

This plugin provides deep insights into the inner workings of Undertow web application. It acts as a monitoring tool,
providing detailed information about various aspects of servlets:

- Lifecycle Events
- Session Management
- Network Connections
- Filter Orchestration

> **Note:**
> WildFly(JBoss) >= 27 use Undertow >= 2.3.x with Jakarta EE 10 (jakarta.servlet.*)
>
> Plugin to instrument WildFly(JBoss) <= 26 you can find in module [undertow_http](../undertow_http).

## Table of Contents

- [Introduction](#introduction)
- [Configuration](#configuration)
  - [Ruleset for Undertow23 plugin](#ruleset-for-undertow23-plugin)

## Introduction

The purpose of this plugin is to instrument all incoming requests for  Undertow >= 2.3.x servlets events in profiler.Below
are the different Undertow entities which this plugin can instrument:

**Lifecycle Events:** Instrument the initialization and destruction of both the overall application context and individual
servlets,allowing to identify potential startup or shutdown issues.

**Session Management:** Instrument session creation and invalidation, providing valuable data for
understanding user activity and session lifespans within application.

**Network Connections:** Instrument network connections, including the ports used and the IP addresses involved. This can
be crucial for debugging network-related problems.

**Filter Orchestration:** Observe the order in which filters are executed within the Undertow framework. This helps in
understanding how filters interact and potentially troubleshooting any issues related to filter behavior.

In essence, this plugin acts as comprehensive monitoring suite for Undertow servlets. It provides deeper understanding of
undertow servlets lifecycle, session management, network interactions, and filter execution, identify bottlenecks, and
effectively debug web application.

## Configuration

Config path: [config/minimal/50main/undertow23_http.xml](./src/main/resources/config/minimal/50main/undertow23_http.xml)

### Ruleset for Undertow23 plugin

In order to instrument every interaction for all incoming requests in profiler Undertow23 plugin is executed based on
below rules.

All incoming requests are handled by **Undertow23RequestHandler** which will start instrumentation on method entry and
exit in profiler.It instruments tasks like initializing database connections, loading configuration data, or cleaning up
resources during application startup and shutdown.Instruments servlet lifecycle events ,session creation and invalidation.

| Class                                          | Method              | Method-Editor             |
|------------------------------------------------|---------------------|---------------------------|
| io.undertow.servlet.core.ApplicationListeners  | requestInitialized  | Undertow23RequestHandle r |

Undertow23 plugin provides capabilities to instrument endpoints (IP addresses and ports) on which the Undertow server
listens for incoming requests.

| Class                           | Method   | Execute-After Exception ( if "true") |
|---------------------------------|----------|--------------------------------------|
| io.undertow.server.Connectors   | run      | `dumpQueueWaitTime$profiler(p2)`     |

Undertow23 plugin instruments filters which are Similar to standard Java Servlet API.It orchestrates requests intercepted
by multiple filters in chain of execution within the Undertow web framework.It can instrument request data (e.g., headers
, parameters) and request and response details for auditing purposes.

| Class                                                       | Method    | Execute-Before            |
|-------------------------------------------------------------|-----------|---------------------------|
| io.undertow.servlet.handlers.FilterHandler$FilterChainImpl  | doFilter  | `fillNcUser$profiler(p1)` |
