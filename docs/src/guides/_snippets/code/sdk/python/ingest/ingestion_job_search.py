from datetime import datetime, timedelta, timezone

from nominal.core import NominalClient
from nominal.core.ingestion_job import IngestionJobStatus

client = NominalClient.from_profile("default")

# Find what failed in the last day, across every dataset in the workspace
failures = client.search_ingestion_jobs(
    statuses=[IngestionJobStatus.FAILED],
    start_time_after=datetime.now(timezone.utc) - timedelta(days=1),
)

for job in failures:
    print(f"{job.rid}  {job.ingest_type}  {job.origin_files}")
    print(f"  {job.nominal_url}")

# `status` is a snapshot taken when the job was fetched. Call refresh() to
# advance it rather than re-reading the same value in a loop.
job = client.get_ingestion_job("<JOB_RID>")
print(job.refresh().status)
