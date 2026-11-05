from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Collection, Iterable, Iterator

from nominal.core import NominalClient
from nominal.core.dataset import Dataset
from nominal.core.retention import KeepUntilExpiry
from nominal.core.run import Run
from nominal.exceptions import NominalError, NominalFailedPreconditionError, NominalPermissionDeniedError
from nominal.experimental.migration.dry_run import DRY_RUN_PREFIX
from nominal.experimental.migration.migrator.context import MigrationContext
from nominal.experimental.migration.resource_type import ResourceType

logger = logging.getLogger(__name__)

# The server treats an unset bound as the Unix epoch, so listing "every exemption" needs explicit extremes. It
# converts bounds to int64 nanoseconds with an exact multiply of the whole seconds, so the start sits one second
# above the int64 minimum: the minimum itself splits into seconds that overflow that multiply.
_ALL_TIME_START = -(2**63) + 10**9
_ALL_TIME_END = 2**63 - 1


class RetentionMigrator:
    """Copies dataset retention policies, retention exemptions, and run locks to the destination tenant.

    Unlike the resource migrators, this creates nothing of its own. Every write is an idempotent update to a
    destination dataset or run that earlier steps already created, so a rerun repeats it and no migration state
    is recorded. Run it after every asset has migrated: locking a run exempts the datasets of all of the run's
    assets, which only exist on the destination once each of those assets has been copied.

    Writes use the context's base destination client rather than a per-resource impersonated one, because
    retention changes need organization admin access.
    """

    def __init__(self, ctx: MigrationContext) -> None:
        """Constructs a RetentionMigrator with the given MigrationContext.

        Args:
            ctx: The MigrationContext to use for this migrator.
        """
        self.ctx = ctx

    def copy_retention(self, source_datasets: Iterable[Dataset], source_runs: Iterable[Run]) -> None:
        """Copy retention for already-migrated datasets and runs.

        The order matters. Exemptions and run locks are applied before any expiry policy, so data the
        source protected is never exposed to deletion on the destination.

        Raises:
            NominalError: If the destination rejects retention changes outright, because the migration user is
                not an organization admin or dataset retention is not enabled. Other failures are recorded
                as skips and the copy continues.
        """
        datasets = {dataset.rid: dataset for dataset in source_datasets if not dataset.is_archived}
        locked_runs = {run.rid: run for run in source_runs if run.is_locked and not run.is_archived}

        for dataset in datasets.values():
            self._copy_exemptions(dataset, locked_runs.keys())
        for run in locked_runs.values():
            self._lock_run(run)
        for dataset in datasets.values():
            self._copy_policy(dataset)

    def _copy_exemptions(self, source: Dataset, locked_run_rids: Collection[str]) -> None:
        try:
            source_exemptions = source.list_retention_exemptions(_ALL_TIME_START, _ALL_TIME_END)
        except NominalError as error:
            # A failure reading the source says nothing about the destination, so it is only a skip.
            self._record_skip(
                ResourceType.DATASET, source.rid, f"retention: failed to read exemptions from the source: {error}"
            )
            return
        # A locked run's exemptions are named after the source run, so re-locking the run on the destination
        # recreates them under the destination run's RID. Copying them as well would leave a duplicate that
        # unlocking the run could never remove.
        exemptions = [exemption for exemption in source_exemptions if exemption.source_id not in locked_run_rids]
        if not exemptions:
            return
        if self.ctx.dry_run:
            logger.info(
                "%s Would copy %d retention exemption(s) of dataset %s", DRY_RUN_PREFIX, len(exemptions), source.rid
            )
            return
        destination = self._destination_dataset(source)
        if destination is None:
            return
        with self._attempting(ResourceType.DATASET, source.rid, f"copy retention exemptions of dataset {source.rid}"):
            for exemption in exemptions:
                destination.upsert_retention_exemption(
                    exemption.source_id, exemption.start, exemption.end, description=exemption.description
                )
            logger.info("Copied %d retention exemption(s) of dataset %s", len(exemptions), source.rid)

    def _lock_run(self, source: Run) -> None:
        if source.end is None:
            self._record_skip(
                ResourceType.RUN, source.rid, "retention: locked run has no end time, so it can't be locked"
            )
            return
        if self.ctx.dry_run:
            logger.info("%s Would lock run %s", DRY_RUN_PREFIX, source.rid)
            return
        destination_rid = self.ctx.migration_state.get_mapped_rid(ResourceType.RUN, source.rid)
        if destination_rid is None:
            logger.debug("Not locking run %s: it was not migrated", source.rid)
            return
        with self._attempting(ResourceType.RUN, source.rid, f"lock run {source.rid}"):
            self._destination_client().get_run(destination_rid).lock()
            logger.info("Locked run %s on the destination", source.rid)

    def _copy_policy(self, source: Dataset) -> None:
        policy = source.retention_policy
        if policy is None:
            self._record_skip(ResourceType.DATASET, source.rid, "retention: source policy type is not recognized")
            return
        # A dataset with no policy already keeps its data forever, so only an expiry needs copying.
        if not isinstance(policy, KeepUntilExpiry):
            return
        if self.ctx.dry_run:
            logger.info(
                "%s Would set retention of dataset %s to %d day(s)", DRY_RUN_PREFIX, source.rid, policy.expiry_days
            )
            return
        destination = self._destination_dataset(source)
        if destination is None:
            return
        with self._attempting(ResourceType.DATASET, source.rid, f"set retention policy of dataset {source.rid}"):
            destination.set_retention_policy(policy)
            logger.info("Set retention of dataset %s to %d day(s)", source.rid, policy.expiry_days)

    def _destination_dataset(self, source: Dataset) -> Dataset | None:
        destination_rid = self.ctx.migration_state.get_mapped_rid(ResourceType.DATASET, source.rid)
        if destination_rid is None:
            logger.debug("Not copying retention for dataset %s: it was not migrated", source.rid)
            return None
        return self._destination_client().get_dataset(destination_rid)

    def _destination_client(self) -> NominalClient:
        if self.ctx.destination_client is None:
            raise RuntimeError("Retention can only be written with a destination client, which a dry run omits.")
        return self.ctx.destination_client

    @contextmanager
    def _attempting(self, resource_type: ResourceType, source_rid: str, action: str) -> Iterator[None]:
        """Stop the whole copy when the destination refuses retention changes; skip the resource otherwise.

        A permission or precondition failure applies to every remaining call, so continuing would only
        repeat it. Any other failure is specific to this resource and is retried by a rerun.
        """
        try:
            yield
        except NominalPermissionDeniedError as error:
            raise NominalError(
                f"Permission denied while trying to {action}. Copying retention requires the migration user to be an "
                "organization admin on the destination tenant."
            ) from error
        except NominalFailedPreconditionError as error:
            raise NominalError(
                f"Could not {action}: {error}. Dataset retention may not be enabled for the destination organization."
            ) from error
        except NominalError as error:
            self._record_skip(resource_type, source_rid, f"retention: failed to {action}: {error}")

    def _record_skip(self, resource_type: ResourceType, source_rid: str, reason: str) -> None:
        """Record a skip once, since the state file persists across reruns that would repeat it."""
        state = self.ctx.migration_state
        if not any(
            skipped.resource_type == resource_type.value
            and skipped.source_rid == source_rid
            and skipped.reason == reason
            for skipped in state.skipped_resources
        ):
            state.record_skip(resource_type, source_rid, reason)
        logger.warning("Skipping retention for %s %s: %s", resource_type.value.lower(), source_rid, reason)
