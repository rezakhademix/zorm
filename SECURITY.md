# Security Policy

## Supported versions

Security fixes target the latest published ZORM release. Older releases do not
have a separate security maintenance commitment; upgrade to the latest release
before checking whether an issue still exists. Development on `main` is not a
supported release.

## Reporting a vulnerability

Please report suspected vulnerabilities privately through
[GitHub private vulnerability reporting](https://github.com/rezakhademix/zorm/security/advisories/new)
when the **Report a vulnerability** button is available.

If private reporting is unavailable, open an issue asking the maintainer to
enable private vulnerability reporting or provide a private contact channel.
Do not include vulnerability details, exploit code, credentials, or sensitive
data in that public issue. Wait for a private channel before sharing details.

Include the following in your private report:

- Affected ZORM version or commit, Go version, database, and driver versions.
- A description of the issue, security impact, and required conditions.
- Minimal reproduction steps or a proof of concept using synthetic data.
- Any suggested mitigation or fix, if available.

Please allow time for investigation and a fix before public disclosure. Response
and remediation times depend on maintainer availability and issue severity; no
fixed response deadline is promised. Disclosure timing and attribution can be
agreed in the private report.

Ordinary bugs and feature requests belong in
[GitHub issues](https://github.com/rezakhademix/zorm/issues).

## Safe use

- Bind untrusted values as query arguments. Never concatenate user input into
  SQL text, including `Raw` queries or raw `Where` / `OrWhere` fragments. Raw
  fragment checks do not prevent all forms of SQL injection.
- Allowlist user-selectable tables, columns, and sort options. Identifier
  validation does not enforce application authorization.
- Enforce access control and tenant isolation in the application. ZORM does
  not add authorization or tenant filters automatically.
- Use database accounts with only the permissions your application needs, and
  configure TLS with certificate and hostname verification for remote databases.
- Keep database credentials out of source control. Treat query arguments and
  debug output as potentially sensitive when logging or reporting errors.
- Keep Go, ZORM, and database drivers updated. Review dependency alerts and
  vulnerability scan results.
