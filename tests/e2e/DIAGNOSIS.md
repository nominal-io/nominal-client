# E2E export failure investigation

Diagnostic snapshot: 2026-09-10, main `9e8503f259a91141d54c8ff87067c0076bd78464`.
This records existing CI evidence and source inspection. It does not establish a
backend root cause or change test/SDK behavior.

## What is failing

The [latest main run](https://github.com/nominal-io/nominal-client/actions/runs/34391070399)
has **2 failed, 84 passed** in its general E2E job (172.26 seconds). Migration,
AWS filename uploads, and Azure filename uploads each passed in separate jobs.
The general job runs Python 3.13 against staging, explicitly excluding migration.
Ordinary pytest excludes `tests/e2e`, so green unit tests do not cover these reads.

Both failures use the session-scoped `ingested_dataset` fixture in `conftest.py`:
create a fresh dataset, upload ten CSV rows, then call the returned
`DatasetFile.poll_until_ingestion_completed()`. The data spans
2024-09-05 18:00–18:09 UTC, with `relative_minutes`, `temperature`, and `humidity`.
The preceding `test_get_channel` passes, checking channel metadata, not samples.

| Test | Intended contract | Latest observed failure |
| --- | --- | --- |
| `test_get_channel_pandas` | `temperature` exports to a Series with ten exact values, UTC timestamp index named `timestamp`, name `temperature`, and float64 dtype | The export request raises HTTP 400, `Default:InvalidArgument`, before pandas reads the response |
| `test_get_dataset_pandas` | Full export equals the ten-row, three-column reference DataFrame; a second export with `channel_exact_match=["relative", "minutes"]` equals just `relative_minutes` | Full export returns shape `(0, 3)` versus expected `(10, 3)`; the filtered export assertion is never reached |

Both paths call `POST /export/v1/export`. The single-channel path goes through
`Channel._get_series_values_csv`; the dataset path uses worker threads in
`datasource_to_dataframe`. Both default to the SDK's minimum/maximum timestamp
bounds and enable gzip.

**The empty DataFrame is not evidence of a successful empty server response in
the latest run.** The captured dataset-export log contains another HTTP 400.
`nominal/thirdparty/pandas/_pandas.py` logs worker exceptions and continues; if all
batches fail, it synthesizes an empty DataFrame with the requested columns.
Consequently, a shape mismatch obscures the underlying request failure. With
multiple batches, the same code can return partial data after a batch fails.

Fixture polling is also weaker evidence than an explicit success assertion:
`DatasetFile.poll_until_ingestion_completed` returns on SUCCESS, deletion states,
and its unknown-status fallback. The fixture does not retain/assert the final
status. CI proves that this waiter returned, not which terminal status it saw.

## History and existing attempts

| Evidence | Result and interpretation |
| --- | --- |
| [August 25 main](https://github.com/nominal-io/nominal-client/actions/runs/32884216907) | General E2E: 83 passed, including both pandas reads. The workflow failed in migration. Workflow color alone is insufficient for dating this regression. |
| [September 2 main](https://github.com/nominal-io/nominal-client/actions/runs/33689500904) | 2 failed, 81 passed. Channel export returned HTTP 500; dataset comparison failed. |
| [September 9 earlier main](https://github.com/nominal-io/nominal-client/actions/runs/34362445170) | 2 failed, 81 passed. Channel export returned HTTP 400; dataset comparison failed. |
| [September 9 latest main](https://github.com/nominal-io/nominal-client/actions/runs/34391070399) | Same two failures; 84 passed after markings coverage was added. |
| [PR #953](https://github.com/nominal-io/nominal-client/pull/953) | Adds polling for ten rows and expected columns, with a shared 600-second budget; retains value/index/dtype assertions. Its description reports a local staging success. |
| [PR #953 CI](https://github.com/nominal-io/nominal-client/actions/runs/34166678867/job/101878977567) | Still 2 failed, 81 passed: channel polling exhausts the budget with zero rows, then dataset export fails with zero rows and the budget already spent. Total 779.07 seconds. This does not demonstrate that another timeout increase would fix CI. |
| [PR #899, merged](https://github.com/nominal-io/nominal-client/pull/899) | Isolates archive-search fixtures from videos surfacing as datasets. Latest main's archive-search test passes. |
| [PR #635, merged](https://github.com/nominal-io/nominal-client/pull/635) | Earlier suite repair migrated deprecated top-level calls and introduced shared ingest fixtures. It addresses a different historical failure. |
| [PR #960](https://github.com/nominal-io/nominal-client/pull/960) | Fixes job-level waiting for dynamically registered extractor outputs. These pandas tests upload CSV directly and wait on a DatasetFile, so that job-level waiter is not in their call path. |

`git diff 6b73f89..9e8503f -- nominal/thirdparty/pandas/_pandas.py
nominal/core/channel.py tests/e2e/conftest.py tests/e2e/test_core.py` is empty.
Thus those files did not change between the sampled August 25 pass and latest
main. This prioritizes backend/runtime investigation, but does not exclude
changes elsewhere in the SDK, dependencies, identity, or environment. The first
bad run has not been located by a complete history scan or bisect.

## Next discriminating experiment

Run a focused fresh-dataset reproduction in the **same workspace and runtime as
CI**, preserving the existing equality assertions. Compare its configuration
with the environment where #953 reportedly passed. A named profile alone does
not demonstrate identical workspace, credentials, routing, or backend settings.

1. Record dataset/file identifiers, elapsed times, and the exact final ingest
   status; require SUCCESS before attributing failure to post-ingest export.
   Register cleanup immediately after dataset creation so a setup failure does
   not bypass teardown.
2. Capture channel metadata and refreshed dataset/file bounds. Observe both
   export paths independently, distinguishing an HTTP exception from a valid
   response with zero rows. Record status, error name, trace/error-instance IDs,
   and successful response shape, without tokens or presigned URLs. Avoid
   per-request noise by logging state transitions and a final summary.
3. Correlate failed requests with backend traces. Latest CI logs contain trace
   IDs for both 400s; the public error has empty parameters and does **not** itself
   identify a missing Iceberg table. Establish whether the failure is table
   creation, data publication, request validation, or another backend condition.
4. On the same dataset, compare default unbounded export with explicit bounds
   around the fixture timestamps. Then, in a separate controlled ingest, compare
   fixed 2024 timestamps with recent timestamps. These isolate range handling
   and possible time-based policies; neither is currently a proven cause.
5. Only after identifying the condition, choose the repair: bounded readiness
   handling if delayed visibility is expected and demonstrably recovers; a
   backend fix if successful ingestion leaves export broken; or a client/request
   correction if the request is invalid. Consider propagating dataframe worker
   errors separately, with compatibility review and regression coverage, so
   failed reads cannot silently look like empty/partial success.

PR #953's HTTP predicate matches status 400 plus `Default:InvalidArgument`.
That is broader than a proven missing-table condition, and dataframe worker
errors are swallowed before the helper sees them. Its unit tests of the retry
helper therefore do not establish error propagation through the full dataframe
path. Diagnose those boundaries before treating polling as a complete repair.

No new live-platform reproduction or backend trace lookup was performed for
this snapshot; live outcomes above come from the linked existing CI jobs.
