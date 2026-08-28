# Jetty10 Plugin Configuration

This plugin allows instrumenting lightweight HTTP context for `Jetty >= 10.x` that use `jakarta.servlet.*`.

It is capable to instrument http servlets lifecycle methods including initialization and destruction of servlets, static
content, security checks and errors which are created by using `org.eclipse.jetty.servlet.ServletHandler`.

> **Note:**
> Tomcat >= 9 with Jakarta EE 10 (`jakarta.servlet.*`)
>
> Plugin to instrument Jetty <= 9 you can find in module [jetty_http](../jetty_http).

## Table of Contents

- [Introduction](#introduction)
- [Configuration](#configuration)
  - [Ruleset for Jetty10 plugin](#ruleset-for-jetty10-plugin)

## Introduction

The purpose of this plugin is to instrument events in profiler for all http requests coming from `Jetty >= 10.x`.

It provides capabilities to instrument below HTTP context details:

- All http requests and responses are created using `org.eclipse.jetty.servlet.ServletHandler`.
- Able to instrument API calls for lightweight microservices created by using embedded jetty.
- All lifecycle methods of servlets which are designed by using `Jetty >= 10.x`.

## Configuration

Config path: [config/minimal/50main/jetty10_http.xml](./src/main/resources/config/minimal/50main/jetty10_http.xml)

### Ruleset for Jetty10 plugin

In order to register http requests in profiler Jetty10 plugin is executed based on below rules.

All incoming requests are handled by **Jetty10RequestHandler** which will start instrumentation on request entry and
exit in profiler.

| Class                                      | Method         | Method-Editor         |
|--------------------------------------------|----------------|-----------------------|
| org.eclipse.jetty.servlet.ServletHandler   | doHandle       | Jetty10RequestHandler |
