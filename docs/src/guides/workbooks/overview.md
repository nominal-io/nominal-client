---
myst:
  html_meta:
    description: "Overview and recipes for working with Nominal's Workbooks in Python"
---

# Workbooks in Nominal with Python

{.lead}
Overview and recipes for working with Nominal's Workbooks in Python

Workbooks are Nominal's tool for creating and sharing interactive data visualizations and analyses. They can be created
directly on the Nominal platform or programmatically using the Nominal Python SDK.

## Creating a workbook from a template

###  Prerequisites

```{include} /guides/_snippets/python/installing-python-client.md
```

#### Connect to Nominal

```{include} /guides/_snippets/python/auth.md
```

### Steps to Create a workbook from a Template
1. Obtain the Template RID:
   - Navigate to the Nominal platform.
   - Go to Workbooks ➔ Templates.
   - Click on the desired template.
   - In the top-left corner next to the template name, click the dropdown arrow ⌄.
   - Select Copy RID to copy the template RID to your clipboard.
2. Obtain the Run RID:
   - Navigate to Runs.
   - Click on the run you want to associate with the workbook.
   - On the right side of the screen, locate the RID under "Metadata".
3. Create the Workbook Using the SDK:

```{literalinclude} /guides/_snippets/code/sdk/python/workbooks/create_workbook_from_template.py
:language: python
```

### Accessing Your New workbook

After creating the workbook programmatically:
- Navigate back to Workbooks on the Nominal platform.
- Locate your new workbook titled "My New Workbook".
- Open it to view and interact with your data visualizations.
