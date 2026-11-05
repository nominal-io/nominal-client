# Core

`nominal.core` is the object-oriented client for the Nominal core platform.
Start with {py:class}`~nominal.core.NominalClient`, then work with the resources it returns.
For setup and an end-to-end example, see the [Python quickstart](/guides/quickstart.md).

```{eval-rst}
.. include:: ../_summaries/nominal.core.rst
   :start-after: .. core-navigation-start
   :end-before: .. core-navigation-end

.. module:: nominal.core
```

## Client and resources

```{list-table}
:class: autosummary
:widths: 30 70

* - {doc}`client`
  - Create a client, select a workspace, and configure requests.
* - {doc}`assets-and-runs`
  - Organize datasets and runs, attach files, and add discussion comments.
* - {doc}`workbooks`
  - Manage workbooks and instantiate them from templates.
* - {doc}`markings`
  - Apply markings and configure their display symbols.
* - {doc}`shared-types`
  - Look up time bounds, resource links, archive filters, and property types.
```

## Data

```{list-table}
:class: autosummary
:widths: 30 70

* - {doc}`datasets-and-files`
  - Upload files, monitor ingestion, and work with dataset channels and exports.
* - {doc}`channels-and-units`
  - Inspect channel metadata, search tagged logs, and manage measurement units.
* - {doc}`streaming`
  - Write timeseries data and log messages through asynchronous streams.
* - {doc}`containerized-extractors`
  - Register container images, configure extractors, and manage their secrets.
* - {doc}`drives`
  - Store files in drives, add versions, and restore or download past revisions.
```

## Events and validation

```{list-table}
:class: autosummary
:widths: 30 70

* - {doc}`events`
  - Create and classify events associated with Nominal resources.
* - {doc}`automatic-validation`
  - Execute checklists, review violations, and configure notification integrations.
```

```{toctree}
:hidden:

client
assets-and-runs
datasets-and-files
channels-and-units
streaming
containerized-extractors
drives
events
automatic-validation
workbooks
markings
shared-types
```
