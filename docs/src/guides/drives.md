---
myst:
  html_meta:
    description: "Create drives, and put, list and download their files, with nomctl."
---

# Drives

{.lead}
A drive is a named container of files in a workspace, like a bucket or a shared folder.
Create drives and manage their files with [nomctl](https://dev.nominal.io/nomctl/), Nominal's command line interface.

```shell
nomctl fs drive create test-reports
nomctl fs put run-12-report.pdf test-reports:/reports/run-12.pdf
nomctl fs ls test-reports:/reports
nomctl fs download test-reports:/reports/run-12.pdf run-12.pdf
```

Paths are written as `DRIVE:/PATH`, where `DRIVE` is the ID the drive was created with.
[nomctl fs](https://dev.nominal.io/nomctl/commands/fs/) lists every command, including moving files and restoring past revisions.
