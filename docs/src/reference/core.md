---
tocdepth: 2
---

# Core SDK

Use {py:class}`~nominal.core.NominalClient` to create and find resources.
The main data-model relationships are:

| Concept | Role |
| --- | --- |
| {py:class}`~nominal.core.Asset` | A physical thing, such as a vehicle or test stand, with an identity that persists across tests. |
| {py:class}`~nominal.core.Dataset` | Time-series channels populated by file uploads or streaming; a dataset can accumulate data across tests. |
| {py:class}`~nominal.core.Run` | A test interval with a start, optional end, and associated assets or data sources. |
| Data scope | A named data source on an asset or run, optionally filtered by series tags. Dataset-backed scopes select data from a dataset. |

Consistent data scope names on assets and reference names on runs let workbook templates
and checklists identify corresponding data sources across tests.

```{eval-rst}
.. automodule:: nominal.core

.. autodata:: nominal.core.DataStream

.. autodata:: nominal.core.LogStream

.. autodata:: nominal.core.NominalProperties

.. autodata:: nominal.core.UnitLike
```
