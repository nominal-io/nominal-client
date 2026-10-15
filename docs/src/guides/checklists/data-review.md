---
myst:
  html_meta:
    description: "How to apply checklists to runs in Nominal using Python, as part of your data review process"
---

# Initiate Data Review in Nominal with Python

{.lead}
How to apply checklists to runs in Nominal using Python, as part of your data review process

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

   ```python
   run1_rid = "paste_rid_here"
   run2_rid = "paste_rid_here"
   ```

2. Locate Checklist RIDs and Commits

   Find the checklist RID(s) in the [platform](https://app.gov.nominal.io/checklists).
   To apply a specific version, copy its commit SHA (listed under "Latest version").
   Omit `commit` from `execute_checklist()` to use the latest version instead.

   ```python
   checklist1_rid = "paste_rid_here"
   checklist1_commit = "paste_commit_sha_here"

   checklist2_rid = "paste_rid_here"
   checklist2_commit = "paste_commit_sha_here"
   ```

3. Optionally, Specify an Integration

   If you would like to send notifications when violations occur, you can specify an integration RID. You can find the integration RID in the [platform](https://app.gov.nominal.io/settings/integrations).

   ```python
   integration_rid = "paste_rid_here"
   ```

4. Apply Checklists

   Create a data review builder and call `execute_checklist()` for each run/checklist pair.
   For a run with multiple assets, also supply `asset=` to select the asset to review.
   Add an integration if notifications are needed, then initiate the reviews.
   `initiate()` waits for review completion by default; it does not confirm notification delivery.

   ```python
   review_builder = client.data_review_builder()
   # Optional: uncomment after setting integration_rid above.
   # review_builder.add_integration(integration_rid)

   # Add requests for run1
   review_builder.execute_checklist(run1_rid, checklist1_rid, commit=checklist1_commit)
   review_builder.execute_checklist(run1_rid, checklist2_rid, commit=checklist2_commit)  # Optional

   # Add requests for run2
   review_builder.execute_checklist(run2_rid, checklist1_rid, commit=checklist1_commit)  # Optional

   # Initiate reviews
   reviews = review_builder.initiate()

   # Print out results
   for review in reviews:
       print(f"review: {review.rid}, URL: {review.nominal_url}")
       print(f"  nr. of generated events: {len(review.get_events())}")
   ```

Each returned review has completed. Use `get_events()` to retrieve its generated events.

## Retrieving Data Review Results Later

Retrieve a review with `client.get_data_review()`. Replace `<DATA_REVIEW_RID>` with
one of the review RIDs printed above.

```python
review = client.get_data_review("<DATA_REVIEW_RID>")
```
