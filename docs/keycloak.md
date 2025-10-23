This document provides information about the keycloak.

# Table of Content

<!-- TOC -->
* [Table of Content](#table-of-content)
* [Introduction](#introduction)
* [Prerequisite](#prerequisite)
* [Client Creation in keycloak](#client-creation-in-keycloak)
* [Get The Client Secret](#get-the-client-secret)
* [User Creation in Keycloak](#user-creation-in-keycloak)
<!-- TOC -->

# Introduction

This Document will provide the information about Keycloak Client and User management process.

# Prerequisite

* Realm should be created for the environment

# Client Creation in keycloak

We need to create a client in keycloak for the environment.

* Go to `Keycloak` environment realm
* Click on `Clients`
* Click on `Create client`
* Fill the `Client ID` and click `Next'.
  > Note: Recommended `Client ID` is environment `namespace` where profiler in installed.

  ![client_id.png](../images/keycloak/client_id.png)

* Enable `Client authentication` and click `Next`
  ![client_auth.png](../images/keycloak/client_auth.png)

* Fill `Valid redirect URIs` and `Web origins` with `UI service` ingress value
  > Note:  
  > For `Valid redirect URIs` please postfix uri with `\*`  
  > For `Web origins` please mention uri without `\ \`

    ![uri.png](../images/keycloak/uri.png)

# Get The Client Secret

* Go to `Keycloak` environment realm
* Click on `Clients`
* Click on the created Client
* Go to `Credentials` tab
  ![cred.png](../images/keycloak/cred.png)

# User Creation in Keycloak

We need to create a user in keycloak to login to `ui-service`.

* Go to `Keycloak` environment realm
* Click on `Users`
* Click on `Add user`
* Fill the details and click `Create`  
  ![add_user.png](../images/keycloak/add_user.png)
* Go to `Credentials` tab and click on `Set Password`
* Fill the details and disable `Temporary` and click `Save`  
  ![password.png](../images/keycloak/password.png)
