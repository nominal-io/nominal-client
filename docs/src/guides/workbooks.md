---
myst:
  html_meta:
    description: "Create workbooks for runs or assets, blank or from templates, and find existing workbooks."
---

# Workbooks

{.lead}
A workbook is a set of charts and panels for viewing a run's or an asset's data in Nominal.
A workbook template defines the panels once, so you can create the same workbook for every test.

```{include} /guides/_snippets/connect-the-client.md
```

## Create a workbook from a template

{py:meth}`~nominal.core.WorkbookTemplate.create_workbook` creates a workbook for a [run](/guides/runs.md) or an [asset](/guides/assets.md).
The template's inputs must match the run's ref names or the asset's data scope names.

```python
template = client.get_workbook_template("<TEMPLATE_RID>")
workbook = template.create_workbook(run=client.get_run("<RUN_RID>"), title="Flight review")
print(workbook.nominal_url)
```

To copy a template's RID, open it from **Workbooks** → **Templates**, click the arrow next to its name and select **Copy RID**.
To look templates up by name or label instead, use {py:meth}`~nominal.core.NominalClient.search_workbook_templates`.

## Create a blank workbook

{py:meth}`Run.create_workbook <nominal.core.Run.create_workbook>` and {py:meth}`Asset.create_workbook <nominal.core.Asset.create_workbook>` create a workbook with no panels, scoped to that run or asset.
Open its link to add panels in Nominal:

```python
workbook = run.create_workbook()
print(workbook.nominal_url)
```

The title defaults to "Workbook for" and the run's or asset's name; pass `title=` to choose another.
To view several runs or several assets in one workbook, use {py:meth}`NominalClient.create_workbook <nominal.core.NominalClient.create_workbook>`.
It takes objects or RIDs:

```python
workbook = client.create_workbook(runs=[run_a, "<RUN_RID>"], title="Spin-up review")
```

## Find workbooks

{py:meth}`~nominal.core.NominalClient.search_workbooks` filters by run, asset, labels and author:

```python
for workbook in client.search_workbooks(run=run):
    print(workbook.title, workbook.nominal_url)
```

{py:meth}`~nominal.core.Run.search_workbooks` and {py:meth}`~nominal.core.Asset.search_workbooks` search the workbooks of one run or asset.
