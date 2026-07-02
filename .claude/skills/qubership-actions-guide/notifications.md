# Email Notifications

Guide for sending email notifications from GitHub Actions workflows using `email-action`.

## When to use

Use `email-action` when a workflow needs to send an email — on failure, on release, on
approval request, or any event-driven notification. The action wraps SMTP delivery and
resolves connection settings from org/repo variables so individual callers only need to
supply secrets and message content.

## Fallback chain

Each connection setting is resolved in order — the first non-empty value wins:

| Setting | Input | Env var | Default |
| --- | --- | --- | --- |
| SMTP host | `server-address` | `EMAIL_SMTP_HOST` | `smtp.gmail.com` |
| SMTP port | `server-port` | `EMAIL_SMTP_PORT` | `587` |
| TLS mode | `secure` | `EMAIL_SECURE` | `false` (STARTTLS) |
| Username | `username` | `EMAIL_USERNAME` | - |
| Password | `password` | `EMAIL_PASSWORD` | - (required) |
| Sender | `from` | `EMAIL_FROM` | - (required) |
| Recipients | `to` | `EMAIL_TO` | - (required) |

## Key inputs

| Input | Notes |
| --- | --- |
| `subject` | Required. Plain string. |
| `to` | Comma-separated. Required if `EMAIL_TO` env is not set. |
| `from` | `Display Name <addr>` or plain address. |
| `body` | Plain text. Accepts `file://` prefix to read from file. |
| `html-body` | HTML. Takes precedence over `body` when both are set. |
| `convert-markdown` | Set `true` to render `body` as HTML. |
| `priority` | `high`, `normal`, or `low`. |

## Permissions

```yaml
permissions:
  contents: read
```

No elevated permissions required.

## Recommended pattern — connection settings from org variables

Composite actions cannot read `secrets.*` or `vars.*` directly. Pass them via the `env:`
block on the step; the fallback chain picks them up automatically.

```yaml
- uses: netcracker/qubership-workflow-hub/actions/email-action@cabbb90e9471163cfac84bd50ff0296b2803b44c  # v2.3.0
  env:
    EMAIL_SMTP_HOST: ${{ vars.EMAIL_SMTP_HOST }}
    EMAIL_SMTP_PORT: ${{ vars.EMAIL_SMTP_PORT }}
    EMAIL_FROM:      ${{ vars.EMAIL_FROM }}
    EMAIL_PASSWORD:  ${{ secrets.EMAIL_PASSWORD }}
  with:
    to: team@example.com
    subject: "CI failed on ${{ github.ref }}"
    body: |
      Workflow ${{ github.workflow }} failed.
      See ${{ github.server_url }}/${{ github.repository }}/actions/runs/${{ github.run_id }}
```

## HTML body pattern

```yaml
- uses: netcracker/qubership-workflow-hub/actions/email-action@cabbb90e9471163cfac84bd50ff0296b2803b44c  # v2.3.0
  env:
    EMAIL_SMTP_HOST: ${{ vars.EMAIL_SMTP_HOST }}
    EMAIL_FROM:      ${{ vars.EMAIL_FROM }}
    EMAIL_PASSWORD:  ${{ secrets.EMAIL_PASSWORD }}
  with:
    to: stakeholders@example.com
    subject: "Release ${{ github.ref_name }} published"
    html-body: |
      <h2>Release ${{ github.ref_name }}</h2>
      <p>Published by <strong>${{ github.actor }}</strong>.</p>
      <p><a href="${{ github.server_url }}/${{ github.repository }}/releases/tag/${{ github.ref_name }}">View release</a></p>
```

## Gmail setup

1. Enable 2-Step Verification on the Gmail account.
2. Generate an **App Password**: Google Account → Security → App Passwords.
3. Store the App Password as `secrets.EMAIL_PASSWORD`.
4. Set `vars.EMAIL_FROM` to the Gmail address (e.g. `ci.noreply.qubership@gmail.com`).
5. The defaults (`smtp.gmail.com:587`, STARTTLS) are pre-configured — no further setup needed.

## Org-level variables to configure

| Variable | Type | Example value |
| --- | --- | --- |
| `EMAIL_SMTP_HOST` | var | `smtp.gmail.com` |
| `EMAIL_SMTP_PORT` | var | `587` |
| `EMAIL_FROM` | var | `ci.noreply.qubership@gmail.com` |
| `EMAIL_PASSWORD` | secret | Gmail App Password |
