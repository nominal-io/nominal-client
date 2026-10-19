# End-to-end test scope

Run the general suite with shared fixtures kept on one worker:

```sh
uv run pytest tests/e2e --ignore=tests/e2e/migration --profile PROFILE --no-cov -n 4 --dist=loadgroup
```

`just test-e2e PROFILE` uses the same scheduling. CI excludes `test_file_upload.py`
from the general job because the dedicated AWS/Azure jobs own that coverage.
Migration keeps its separate source/destination profiles and job.

## Scheduling and resource ownership

| Group | Shared resources | Why it stays together |
| --- | --- | --- |
| `search` | Search resources and archive pairs | Create once; reuse across backend search predicates |
| `derived` | Tagged source datasets and mixed-series input | Reuse ingestion across definition and query contracts |
| `readonly_dataset` | The `ingested_dataset` fixture | Channel, pandas, download, and markings tests share one upload |

Session fixtures are per worker, not per pytest invocation. Mark every new consumer
of `ingested_dataset` with `pytest.mark.xdist_group("readonly_dataset")`.
Search and derived modules carry their group marks at module scope. Independent
resource mutations, timestamp cases, and spatial cases remain ungrouped and can
run concurrently. Resources are unique to a test or group; no mutable resource is
shared between workers. Ingest the two files in the mixed dataset sequentially;
separate source datasets can ingest concurrently.

Search fixtures register cleanup immediately after creation, so partial setup
failures still archive resources and one cleanup failure does not skip the rest.
The read-only dataset and derived inputs retain their fixture-owned cleanup.

## What belongs here

| Behavior | Coverage location and reason |
| --- | --- |
| Timestamp formats and precision | Live ingestion: local conversion tests cannot establish backend parsing |
| Derived filters, offsets, tags, and union results | Live queries: definition serialization alone cannot establish resulting data |
| Search predicates and archive/draft visibility | Live search: these are server-side semantics; cheap assertions reuse setup |
| Spatial indexing and archived-resource ingestion | Live completion: acceptance alone does not prove successful indexing |
| Storage filename encoding | Three uploads per backend: original parentheses regression, combined ASCII characters, Unicode |
| Individual filename validation/forwarding | `tests/core/test_filenames.py` and `test_multipart.py`: exhaustive local cases without ingestion |
| Workspace selector conversion | `tests/core/test_workspace_resolution.py`: object/ALL conversion is local; live default/override resolution remains e2e |
| Tag mismatch and invalid derived-input rejection | `tests/core/test_asset.py` and `tests/experimental/test_derived_dataset.py`: local guards with service-boundary fixtures |
| Migration retry orchestration | Local retry/skip tests cover mechanics; the existing live migration smoke still covers real download/upload failure composition |

Combine assertions that belong to one resource lifecycle: run dataset linkage now
checks listing and lookup; derived creation checks lookup too; empty derived
creation is checked before adding inputs; unit ingestion checks initial units and
updates in one test. Keep different backend semantics separate even when setup
is shared. A low test count is not itself a goal.

The filename e2e matrix consolidates the allowed ASCII characters into one case;
it does not establish every character in isolation on each backend. Local tests
cover each character separately. Keep the original Azure regression isolated to
make that failure recognizable.
