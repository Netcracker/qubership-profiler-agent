# Tomcat Plugin Configuration

This plugin allows instrumenting HTTP context for `Tomcat <= 9.x` that use `javax.servlet.*` packages.

It provided capabilities to instrument http request, asynchronous and synchronous requests, event details
and TCP/IP network connections and I/O.

Applicable for:

- Tomcat <= 9.x
- WildFly <= 26.x
- Spring Boot <= 2.x (in case if Spring Boot will use a Tomcat)

> **Note:**
> Plugin to instrument Tomcat >= 10.x you can find in module [tomcat10_http](../tomcat10_http).

## Table of Contents

- [Introduction](#introduction)
- [Configuration](#configuration)
  - [Ruleset for Tomcat plugin](#ruleset-for-tomcat-plugin)

## Introduction

The purpose of this plugin is to log events in profiler for all http requests coming Tomcat >=10.x.

It provides capabilities to log incoming request details from different sources of http-requests in profiler.

Below are the different sources of request which this plugin can instrument:

- All requests and responses of servlets.
- All http requests, asynchronous calls from Spring MVC.
- All events and synchronous calls for reactive requests.
- All details related to TCP/IP network connections and I/O operations.

## Configuration

Config path: [config/minimal/50main/tomcat_http.xml](./src/main/resources/config/minimal/50main/tomcat_http.xml)

### Ruleset for Tomcat plugin

In order to register http requests in profiler Tomcat10 plugin is executed based on below rules.

All incoming requests are handled by **TomcatRequestHandler** which will start instrumentation on method entry and
exit in profiler.

| Class                                        | Method     | Method-Editor        |
|----------------------------------------------|------------|----------------------|
| org.apache.catalina.core.StandardEngineValve | invoke     | TomcatRequestHandler |

Provides capabilities to instrument Spring MVC **reactive web requests** which are **asynchronous**.It can register the
instrumentation information even if there is any exception during request processing.

| Class                                                                                                   | Method   | Execute-After Exception ( if "true") |
|---------------------------------------------------------------------------------------------------------|----------|--------------------------------------|
| org.springframework.web.servlet.mvc.method.annotation.<br>ReactiveTypeHandler$AbstractEmitterSubscriber | run      | `run$profiler(throwable)`            |

Provides capabilities to instrument http traffic attributes like number of **concurrent connections**,**request**
and **response size** etc.

| Class                                        | Method            | Execute-After Exception ( if "true")     |
|----------------------------------------------|-------------------|------------------------------------------|
| reactor.netty.http.server.HttpTrafficHandler | channelRead       | `channelRead$profiler(p1, p2,throwable)` |

Provides capabilities to instrument **event loop** , **asynchronous processing** and **task execution**.

| Class                              | Method            | Minimum-Method-Size  |
|------------------------------------|-------------------|----------------------|
| io.netty.channel.nio.NioEventLoop  | runAllTasks       | 0                    |

Tomcat10 plugin can instrument network connection over TCP/IP and I/O operation performed during socket connection.

| Class                                             | Method      | Minimum-Method-Size |
|---------------------------------------------------|-------------|---------------------|
| org.apache.tomcat.util.net.SocketProcessorBase    | run         | 0                   |
