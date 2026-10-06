This guide describes how to change passwords for CDT and all components in it during
installation and during the work.

# Table of Content

<!-- TOC -->
* [Table of Content](#table-of-content)
* [Set passwords during deployment](#set-passwords-during-deployment)
* [Change Passwords](#change-passwords)
  * [Change Basic Auth passwords](#change-basic-auth-passwords)
  * [Change Client passwords/secret](#change-client-passwordssecret)
<!-- TOC -->

# Set passwords during deployment

# Change Passwords

Profiler allows specifying the following credentials during deploy:

## Change Basic Auth passwords

To update Basic Auth credentials using deployment parameters please read the section `HTTP Basic`
in [installation doc](installation.md). Need to update `ui.security.basic.password` parameter of `ui-service` and
redeploy it.

## Change Client passwords/secret

To Update Client password/secret, browse to `Kelcloak`and change the password of the user under environment realm.
After that update the encoded password in deployment parameter `ui.security.oidc.idp_client_secret`.
Follow these steps:

* Go to `Clients` in Keycloak
* Change client secret for the particular client and copy it.
* update `ui-service` deployment parameter `ui.security.oidc.idp_client_secret`
* redeploy `ui-service`
