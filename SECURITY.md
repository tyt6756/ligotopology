# Security Policy

LigoTopology takes the security of our codebase, databases, and multi-channel synchronization pipelines very seriously. This document outlines our policy for reporting security vulnerabilities.

## Supported Versions

Only the latest release (including stable beta versions) is supported for security updates:

| Version | Supported |
| :--- | :--- |
| **v0.1.0** |  Yes |
| < v0.1.0 |  No |

## Reporting a Vulnerability

If you discover a security vulnerability (e.g., sensitive database credential leakage, denial-of-service in async queue locks, or remote execution bugs), **please do not open a public GitHub issue**.

Instead, report the vulnerability privately by sending an email to the lead maintainer at `tyt6756@example.com` (or contact via verified GitHub profile security channel).

Please include the following details in your report:
- A description of the vulnerability and its potential impact.
- Steps to reproduce the issue (proof-of-concept scripts or commands).
- Any suggested remediations if available.

We will acknowledge receipt of your report within 48 hours and work with you to release a security patch in a timely manner.
