# Security Policy

## Reporting a vulnerability

If you find a security vulnerability in `skilj`, please report it
privately by emailing **g.klijs@gmail.com** rather than opening a
public issue - this gives time to prepare a fix before the details are
public.

Please include:

- A description of the vulnerability and its potential impact.
- Steps to reproduce, or a proof of concept if you have one.
- The affected crate(s) and version(s), if known.

## What to expect

This is a solo-maintainer project, so there's no fixed SLA, but
security reports are prioritised over other work. You'll get an
acknowledgement, and a fix or mitigation plan once the issue is
understood.

## Supported versions

`skilj` is pre-1.0. Only the latest published release is supported:
a fix ships as a new patch release of the current minor series (for
example `0.1.x`), which is compatible by the project's semver promise
(README.md, "Status and stability"). There is no long-term-support
branch, and older minor series get no backports.
