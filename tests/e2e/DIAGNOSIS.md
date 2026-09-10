# E2E export failure investigation

Diagnostic snapshot: 2026-09-10, main `9e8503f259a91141d54c8ff87067c0076bd78464`.
The initial investigation recorded existing CI evidence and source inspection.
The local reproduction below narrows the backend failure to the Iceberg/default
read path. This branch also fixes the client error handling that hid failed
dataframe exports. The underlying backend cause remains unresolved.

## Local reproduction and client fix

Running the unchanged pair with the local `staging` profile reproduced both
failures in 8.19 seconds:

```sh
uv run pytest tests/e2e/test_core.py \
  -k 'test_get_channel_pandas or test_get_dataset_pandas' \
  --profile staging --no-cov -v
```

Two additional controlled CSV ingests compared the fixture's original 2024
timestamps with recent timestamps. Both files explicitly reached SUCCESS and
their datasets had the expected bounds. Both datasets were DUAL-backed with
KEEP_FOREVER retention. Both bounded and unbounded exports initially returned
HTTP 400 and then successful zero-row responses through approximately 130 seconds.
The original-timestamp probe first observed zero rows around 88 seconds; the
recent-timestamp probe around 46 seconds. These are sampled observations, not
precise backend transition times.

A subsequent read-only comparison on the same two datasets changed only the
export compute node's optional `SeriesStorage` selection:

| Storage selection | Original timestamps | Recent timestamps |
| --- | --- | --- |
| Default | 0 rows | 0 rows |
| CLICKHOUSE | 10 rows, temperature values 20–29 | 10 rows, temperature values 20–29 |
| ICEBERG | 0 rows | 0 rows |

This establishes that the samples are available through ClickHouse while the
Iceberg/default path cannot return them at the observed times. Range bounds and
old fixture dates do not explain that difference. It does not yet distinguish
Iceberg write/publication failure from read/query failure. A successful local
profile run also cannot prove equivalence to CI's workspace or identity.

Finally, the original two pandas tests passed in 50.55 seconds with a
process-local CLICKHOUSE selection on a fresh dataset. All original value,
dtype, index, full-dataframe, and filtered-dataframe assertions were retained.
Each temporary dataset created by these runs was archived afterward.

The client fix makes `datasource_to_dataframe` propagate a worker's original
exception instead of silently returning empty or partial data. Successful empty
exports retain their columns and timestamp index. This is a deliberate behavior
change for callers that previously received partial results after a failed
batch; they now receive the underlying exception. Four regression tests cover
all-failed and partially failed batches, valid empty responses, and successful
multi-batch joining. Both error tests failed before the fix and passed afterward.
After the fix, another default-route staging run still failed both tests in
8.41 seconds, now with the underlying HTTP 400 visible through both entry points.
Local validation: 984 unit tests passed, one skipped; full SDK and regression
test type checking, Ruff lint/format checks, and whitespace checks passed.

The backend storage comparison is diagnostic only: the branch does not force
ClickHouse, retry generic 400s, or weaken the E2E equality assertions. Staging
backend log access was blocked by an expired AWS SSO session. Correlating the
probe trace IDs with backend logs is the remaining step toward repairing the
Iceberg/default path.

## Streaming comparison

A subsequent local probe streamed the same 30 floating-point values (ten
timestamps across all three fixture channels) into a fresh staging dataset
through the default JSON write stream. The diagnostic explicitly awaited the
single batch future's `result()` to confirm successful request completion:
`flush(wait=True)` itself only waits and does not propagate worker errors.

The write was acknowledged at approximately 0.53 seconds. Channels were not
immediately visible. ClickHouse first returned the exact expected DataFrame at
34 seconds; Iceberg initially returned HTTP 400, then returned the exact frame
at 84 seconds. The default route returned zero rows at 56 seconds and the exact
frame at 105 seconds. Subsequent successful reads retained exact value, dtype,
column, and timestamp-index equality. Times are sampled, with requests issued
sequentially, not precise readiness measurements.
The probe finished with exact matches on all three routes at approximately
230 seconds, confirmed DUAL backing and the expected bounds, then archived its
temporary dataset.

This demonstrates that streaming can make the same fixture data readable
through Iceberg/default export, while request acknowledgement alone does not
guarantee immediate visibility. The earlier file probes remained unreadable at
130 seconds. A later recheck still found ten samples in ClickHouse and none in
Iceberg for both file-probe datasets, but those datasets had been archived at the
end of their original probes; this is not an unconfounded measurement of how
long an active file ingest might take to become readable.

The evidence motivates a simultaneous file-versus-streaming comparison, keeping
both datasets active until a shared deadline, and backend trace inspection of
the file-to-Iceberg path. Streaming is useful additional coverage and could
isolate pandas export tests from file ingestion, but replacing the sole file
round-trip assertions would hide the original failure. Retain file-to-export
coverage and give any streaming test an explicit export-readiness check.

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

The historical sections above describe the initial CI investigation. The local
reproduction section records subsequent live staging probes. No backend trace
lookup has succeeded yet.
