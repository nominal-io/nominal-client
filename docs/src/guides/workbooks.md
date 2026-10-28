---
myst:
  html_meta:
    description: "Create workbooks from templates for a run or an asset, and find existing workbooks."
---

# Workbooks

{.lead}
A workbook is a set of charts and panels for viewing a {term}`run`'s or an {term}`asset`'s data in Nominal.
A workbook template defines the panels once, so you can create the same workbook for every test.

```{include} /guides/_snippets/connect-the-client.md
```

## Create a workbook from a template

{py:meth}`~nominal.core.WorkbookTemplate.create_workbook` creates a workbook for a [run](/guides/runs.md) or an [asset](/guides/assets.md).
The template's inputs must match the run's {term}`ref names <ref name>` or the asset's {term}`data scope` names.

```python
template = client.get_workbook_template("<TEMPLATE_RID>")
workbook = template.create_workbook(run=client.get_run("<RUN_RID>"), title="Flight review")
print(workbook.nominal_url)
```

To copy a template's {term}`RID`, open it from **Workbooks** → **Templates**, click the arrow next to its name and select **Copy RID**.
To look templates up by name or {term}`label` instead, use {py:meth}`~nominal.core.NominalClient.search_workbook_templates`.

## Find workbooks

{py:meth}`~nominal.core.NominalClient.search_workbooks` filters by run, asset, labels and author:

```python
for workbook in client.search_workbooks(run=run):
    print(workbook.title, workbook.nominal_url)
```

{py:meth}`~nominal.core.Run.search_workbooks` and {py:meth}`~nominal.core.Asset.search_workbooks` search the workbooks of one run or asset.
