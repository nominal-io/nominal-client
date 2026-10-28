---
myst:
  html_meta:
    description: "Definitions of the Nominal concepts that the client documentation uses."
---

# Glossary

{.lead}
Short definitions of the concepts in this documentation.
Each entry links to the page that explains the concept in detail.

```{glossary}
:sorted:

API key
  A secret token that authorizes requests to Nominal.
  A {term}`profile` stores it.
  See [API keys setup](/guides/api-keys-setup.md).

asset
  A system that you test, such as a vehicle, satellite or test stand.
  It collects the {term}`datasets <dataset>`, {term}`runs <run>`, {term}`events <event>`
  and {term}`attachments <attachment>` of that system across every test.
  See [Assets](/guides/assets.md).

attachment
  A file about an {term}`asset` or a {term}`run`, such as a test plan or a photo.
  Nominal stores it as is and does not read data from it.
  See [Update metadata and attachments](/guides/assets.md#update-metadata-and-attachments).

channel
  One named signal in a {term}`dataset`, such as `altitude` or `fault_code`.
  Nominal sets its data type from the first values it receives,
  unless you register the channel first.
  See [Channels and data types](/guides/datasets/channels.md).

check
  One condition on {term}`channel` data that must never be true,
  such as a temperature over its limit.
  A {term}`checklist` holds checks.
  See [Authoring checklists as code](/guides/checklists/authoring.md).

checklist
  A set of {term}`checks <check>`.
  A {term}`data review` runs it against a {term}`run`,
  and a {term}`streaming checklist` runs it against live data.
  See [Authoring checklists as code](/guides/checklists/authoring.md).

data review
  One execution of a published {term}`checklist` against a {term}`run`.
  Each place where a {term}`check` fails becomes an {term}`event`.
  See [Data review](/guides/checklists/data-review.md).

data scope
  The name under which an {term}`asset` holds a {term}`dataset`, such as `telemetry`.
  Use the same name for the same kind of data on every asset,
  so that {term}`workbook templates <workbook template>` and
  {term}`checklists <checklist>` work across assets.
  See [Add data with data scopes](/guides/assets.md#add-data-with-data-scopes).

dataset
  Timestamped data, held as named {term}`channels <channel>`.
  Files that you upload and points that you stream add to the same dataset over time.
  See [Datasets](/guides/datasets/overview.md).

dataset file
  One file uploaded to a {term}`dataset`, with its ingestion status and time bounds.
  See [Inspect channels and files](/guides/datasets/overview.md#inspect-channels-and-files).

decimation
  Reading a {term}`channel` as a fixed number of time buckets,
  each with the minimum, maximum, mean, count and variance of its points.
  See [Decimate data](/guides/datasets/decimating-data.md).

event
  A moment or interval in test data that someone should see,
  such as an engine start or an out-of-limit reading.
  People, {term}`checklists <checklist>` and scripts create events.
  See [Events](/guides/events.md).

file extractor
  A Docker image that Nominal runs during ingest to convert files that it cannot read,
  such as a bespoke binary log.
  See [File extractors](/guides/extractors/overview.md).

ingest job
  A batch of uploaded files that Nominal tracks as one unit, with one status.
  See [Ingest jobs](/guides/ingest/ingestion-jobs.md).

label
  A short text value on an {term}`asset`, {term}`run`, {term}`dataset` or {term}`event`,
  such as `airborne`. Use labels to group and search resources.
  See [Find assets](/guides/assets.md#find-assets).

log stream
  A {term}`write stream` that sends timestamped messages to a log {term}`channel`.
  See [Stream logs](/guides/streaming/logs.md).

marking
  A restriction on who can access the data sources that it is applied to.
  See [Markings](/sdk/core/markings.md).

profile
  A named set of connection settings for the client and the `nom` CLI:
  the Nominal URL, an {term}`API key` and, optionally, a {term}`workspace`.
  See [API keys setup](/guides/api-keys-setup.md).

property
  A key-value pair on a resource, such as `serial: SN42`.
  Properties identify the physical system that an {term}`asset` represents.
  See [Create or find an asset](/guides/assets.md#create-or-find-an-asset).

ref name
  The name under which a {term}`run` holds a {term}`dataset`.
  It has the same function in a run as a {term}`data scope` has in an {term}`asset`.
  See [Create a run from datasets](/guides/runs.md#create-a-run-from-datasets).

RID
  A resource identifier, the unique ID of a resource such as an {term}`asset` or a {term}`run`.
  The page of each resource in Nominal shows its RID.
  See [Find assets](/guides/assets.md#find-assets).

run
  A span of time for one test or analysis, such as a flight or a test-stand firing.
  A run refers to data and does not copy it.
  See [Runs](/guides/runs.md).

streaming checklist
  A published {term}`checklist` that evaluates the live data of an {term}`asset`.
  When a {term}`check` fails, it can create an {term}`event` and send a notification.
  See [Trigger alerts on live data](/guides/streaming/streaming-checklists.md).

tag
  A key-value pair on streamed or uploaded points that tells apart points in the same
  {term}`channel`, such as `motor_id: left`.
  See [Add tags](/guides/streaming/overview.md#add-tags-to-filter-data-in-a-run-or-asset).

timestamp type
  The format of the timestamps in uploaded data,
  such as ISO 8601 strings or epoch seconds.
  See [Timestamps](/sdk/ts/index.md).

unit
  The unit of measurement of a {term}`channel`, such as `m/s`.
  See [Set units](/guides/datasets/channels.md#set-units).

video channel
  A {term}`channel` in a {term}`dataset` that holds video.
  With the correct start time, it plays in sync with the telemetry in the same {term}`run`.
  See [Video ingest](/guides/video/video-ingest.md).

workbook
  A set of charts and panels that shows the data of a {term}`run` or an {term}`asset`
  in Nominal.
  See [Workbooks](/guides/workbooks.md).

workbook template
  A reusable {term}`workbook` layout.
  Its inputs match {term}`ref names <ref name>` or {term}`data scope` names,
  so that you can create the same workbook for every test.
  See [Create a workbook from a template](/guides/workbooks.md#create-a-workbook-from-a-template).

workspace
  A group of resources in Nominal, with its own access rules.
  The client creates and searches resources in one workspace.
  See [Client](/sdk/core/client.md).

write stream
  An object that sends values to a {term}`dataset` while your program makes them,
  in batches in the background.
  See [Stream data](/guides/streaming/overview.md).
```
