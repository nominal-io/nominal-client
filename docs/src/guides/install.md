---
myst:
  html_meta:
    description: "Install the Nominal Python Client and its optional extras."
---

# Install

The Nominal Python Client is published on [PyPI](https://pypi.org/project/nominal/) as `nominal`
and supports Python 3.11 and later.

::::{tab-set}
:sync-group: installer

:::{tab-item} uv
:sync: uv

```bash
uv add nominal
```
:::

:::{tab-item} pip
:sync: pip

```bash
pip install nominal
```
:::
::::

This includes the {py:class}`~nominal.core.NominalClient` and the [`nom` CLI](/sdk/cli.md).

## Extras

Extras add the dependencies for one feature. Name them in brackets, separated by commas.

::::{tab-set}
:sync-group: installer

:::{tab-item} uv
:sync: uv

```bash
uv add "nominal[tdms,video]"
```
:::

:::{tab-item} pip
:sync: pip

```bash
pip install "nominal[tdms,video]"
```
:::
::::

| Extra | For |
| --- | --- |
| `tdms` | [Uploading TDMS files](/sdk/thirdparty/tdms.md) |
| `ibis` | [Querying data with Ibis](/sdk/ibis/index.md) |
| `mis` | [Excel instrumentation sheets](/guides/cli/instrumentation-sheets.md) |
| `compute` | [Compute as code](/sdk/experimental/compute-as-code.md) and [authoring checklists](/guides/checklists/authoring.md) |
| `video` | [Live video streaming](/guides/video/live-video-streaming.md). Also needs GStreamer 1.20+ installed on your system. |

`compute` and `video` install only on CPython for Windows (x86-64), macOS (Apple silicon) and
Linux (x86-64 and ARM64).

Next, [set up your API keys](/guides/api-keys-setup.md).
