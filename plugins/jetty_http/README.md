# Jetty Plugin Configuration

This plugin allows instrumenting lightweight HTTP context for `Jetty <= 9` that use `jakarta.servlet.*`.It is capable
to instrument http servlets lifecycle methods including initialization and destruction of servlets , static
content, security checks and errors which are created by using `org.eclipse.jetty.servlet.ServletHandler`.

> **Note:**
> Jetty <= 9 with Java EE (`javax.servlet.*`)
>
> Plugin to instrument Jetty >= 10.x you can find in module [jetty10_http](../jetty10_http).

## Table of Contents

- [Introduction](#introduction)
- [Configuration](#configuration)
  - [Ruleset for Jetty plugin](#ruleset-for-jetty-plugin)

## Introduction

The purpose of this plugin is to instrument events in profiler for all http requests coming from `Jetty <= 9`.

It provides capabilities to instrument below HTTP context details:

- All http requests and responses are created using `org.eclipse.jetty.servlet.ServletHandler`.
- Able to instrument API calls for lightweight microservices created by using embedded jetty.
- All lifecycle methods of servlets which are designed by using `Jetty <= 9`.

## Configuration

Config path: [config/minimal/50main/jetty_http.xml](./src/main/resources/config/minimal/50main/jetty_http.xml)

### Ruleset for Jetty plugin

In order to register http requests in profiler Jetty plugin is executed based on below rules.

All incoming requests are handled by **Jetty10RequestHandler** which will start instrumentation on request entry and
exit in profiler.

| Class                                      | Method         | Method-Editor       |
|--------------------------------------------|----------------|---------------------|
| org.eclipse.jetty.servlet.ServletHandler   | doHandle       | JettyRequestHandler |
