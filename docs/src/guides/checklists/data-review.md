---
myst:
  html_meta:
    description: "How to apply checklists to runs in Nominal using Python, as part of your"
---

# Initiate Data Review in Nominal with Python

{.lead}
How to apply checklists to runs in Nominal using Python, as part of your

```{include} /guides/_snippets/install-warning.md
```

Data review is the process of:
1. Applying a checklist to a run
2. Reviewing time ranges where checks were violated
3. Determining if violations should be ignored or require further action

This guide shows you how to apply a checklist to a run using the Nominal Python client. You can read more on data review in the [Data Review guide](https://docs.nominal.io/core/documentation/platform/flag-events/data-review) in the Core Platform docs.

## Connect to Nominal

```{include} /guides/_snippets/python/auth.md
```

## Applying Checklists to Runs

Nominal allows you to apply multiple checklists to multiple runs simultaneously. You can also optionally specify
integrations to send notifications if violations are found.

1. Locate or Create Run RIDs

   Find the run RID(s) in the [platform](https://app.gov.nominal.io/runs) or [create new runs with the Nominal client](/guides/runs/overview.md).
   Then paste the run RID(s) below.

   ```{literalinclude} /guides/_snippets/code/sdk/python/checklists/data_review_1.py
   :language: python
   ```

2. Locate Checklist RIDs and Commits

   Find the checklist RID(s) in the [platform](https://app.gov.nominal.io/checklists).
   You will also need the commit SHA for the version you want to apply (listed under "Latest version").

   ```{literalinclude} /guides/_snippets/code/sdk/python/checklists/data_review_2.py
   :language: python
   ```

3. Optionally, Specify an Integration

   If you would like to send notifications when violations occur, you can specify an integration RID. You can find the integration RID in the [platform](https://app.gov.nominal.io/settings/integrations).

   ```{literalinclude} /guides/_snippets/code/sdk/python/checklists/data_review_3.py
   :language: python
   ```

4. Apply Checklists

   Use the Nominal client to create a data review builder, add your integration (optional), and add each request (which associates a run with a checklist).
   Then initiate the reviews.

   ```{literalinclude} /guides/_snippets/code/sdk/python/checklists/data_review_4.py
   :language: python
   ```

You have now applied one or more checklists to your runs, and you'll be able to see any violations that arose.

## Retrieving Data Review Results Later

You can retrieve the results of a data review at any time using the `client.get_data_review()` function.

```{literalinclude} /guides/_snippets/code/sdk/python/checklists/data_review_5.py
:language: python
```
