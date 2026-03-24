# How to set up and launch Cloud Profiler UI on Local system

## Table of Content

- [Introduction](#introduction)
- [Prerequisites](#prerequisites)
- [Environment setup](#environment-setup)
  - [Cloud Profiler UI Repository](#cloud-profiler-ui-repository)
  - [Project Setup](#project-setup)

## Introduction

This guide will help user to set up the environment on their machine.
It will provide step-by-step instructions to run the Cloud Profiler UI locally.

## Prerequisites

- Node.js 20.16.0 or higher
- npm 10.8.1 or higher
- Wsl 2

>Note: Preferred to run both UI and Profiler on Windows or Wsl 2.But Don't deploy it separately.
      Ex. If deployed Profiler on Windows and UI on wsl then, you will get `504` error for API calls from UI.

## Environment setup

### Cloud Profiler UI Repository

Clone `cloud-profiler-ui` repository with this git link to any location under WSL:
[PROD.Platform.CLoud.Infra.profiler.cloud-profiler-ui](https://github.com/Netcracker/qubership-profiler-backend-ui.git)

### Project setup

- Open `.env` file in root directory and change its content to this:

```shell
API_URL=http://localhost:8080
OPEN_BROWSER=false
PORT=3030
```

- Open terminal in project's root directory and execute `npm install` command
- Execute `npm start`command
- After this open `http://localhost:3030/` in browser, it should show profiler ui.

>Note: To view data on UI, need to deploy Profiler with `service_type=ui`
