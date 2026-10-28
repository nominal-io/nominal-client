---
myst:
  html_meta:
    description: "Run published checklists against runs as data reviews, and read the events they create."
---

# Data review

{.lead}
A data review runs a published {term}`checklist` against a {term}`run`.
Each place a {term}`check` fails becomes an {term}`event` for an engineer to review.

For checking live data as it arrives, see [Trigger alerts](/guides/streaming/streaming-checklists.md).

```{include} /guides/_snippets/connect-the-client.md
```

## Run checklists against runs

{py:meth}`~nominal.core.NominalClient.data_review_builder` returns a {py:class}`~nominal.core.DataReviewBuilder`.
Add one {py:meth}`~nominal.core.DataReviewBuilder.execute_checklist` per run and checklist pair, then start them all with {py:meth}`~nominal.core.DataReviewBuilder.initiate`:

```python
builder = client.data_review_builder()
builder.execute_checklist("<RUN_RID>", "<CHECKLIST_RID>", commit="<COMMIT_SHA>")  # pin a version
builder.execute_checklist("<OTHER_RUN_RID>", "<CHECKLIST_RID>")  # latest version
builder.add_integration("<INTEGRATION_RID>")  # optional: notify when checks fail

for review in builder.initiate():
    print(review.nominal_url, len(review.get_events()))
```

Copy checklist {term}`RIDs <RID>` and commit SHAs from the [Checklists page](https://app.gov.nominal.io/checklists).
For a run with several {term}`assets <asset>`, pass `asset=` to choose the one to review.

`initiate` waits for the reviews to finish by default, but doesn't wait for notifications.
Pass `wait_for_completion=False` to return immediately, then call {py:meth}`~nominal.core.DataReview.poll_for_completion` on each review.

Notifications support Slack, Opsgenie and webhook integrations.

## Read results later

```python
review = client.get_data_review("<DATA_REVIEW_RID>")
events = review.get_events()
```

{py:meth}`~nominal.core.NominalClient.search_data_reviews` and {py:meth}`~nominal.core.Asset.search_data_reviews` find reviews by run or asset.
To review the results in the UI, see [Data review](https://docs.nominal.io/core/documentation/platform/flag-events/data-review).
