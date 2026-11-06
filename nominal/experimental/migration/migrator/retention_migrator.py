from __future__ import annotations

import logging
from typing import Callable, Collection, Iterable

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

_SKIP_PREFIX = "retention:"


class RetentionMigrator:
    """Copies dataset retention policies, retention exemptions, and run locks to the destination tenant.

    Unlike the resource migrators, this creates nothing of its own. Every write is an idempotent update to a
    destination dataset or run that earlier steps already created, and every run attempts all of them again, so
    a rerun repeats the whole pass and only its own skips are kept in the migration state. Run it after every
    asset has migrated: locking a run exempts the datasets of all of the run's assets, which only exist on the
    destination once each of those assets has been copied.

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

        The order matters. Exemptions and run locks are applied before any expiry policy, and a dataset whose
        exemptions or run locks were not all applied gets no expiry, so data the source protected is never
        exposed to deletion on the destination. A rerun applies the withheld expiry once its protection is in
        place.

        Raises:
            NominalError: If the destination rejects retention changes outright, because the migration user is
                not an organization admin or dataset retention is not enabled. Other failures are recorded
                as skips and the copy continues.
        """
        # Every run attempts everything again, so skips from earlier runs are stale.
        self.ctx.migration_state.clear_skips_with_prefix(_SKIP_PREFIX)
        datasets = {dataset.rid: dataset for dataset in source_datasets if not dataset.is_archived}
        locked_runs = {run.rid: run for run in source_runs if run.is_locked and not run.is_archived}

        # Source dataset RIDs whose protection was not fully applied, or every dataset when that is unknowable.
        unprotected: set[str] = set()
        all_unprotected = False
        for dataset in datasets.values():
            if not self._copy_exemptions(dataset, locked_runs.keys()):
                unprotected.add(dataset.rid)
        for run in locked_runs.values():
            if not self._lock_run(run):
                affected = self._datasets_of(run)
                if affected is None:
                    all_unprotected = True
                else:
                    unprotected |= affected
        for dataset in datasets.values():
            self._copy_policy(dataset, withhold_expiry=all_unprotected or dataset.rid in unprotected)

    def _copy_exemptions(self, source: Dataset, locked_run_rids: Collection[str]) -> bool:
        """Copy a dataset's exemptions, returning whether every one that needed copying is in place."""
        try:
            source_exemptions = source.list_retention_exemptions(_ALL_TIME_START, _ALL_TIME_END)
        except NominalError as error:
            # A failure reading the source says nothing about the destination, so it is only a skip.
            self._record_skip(
                ResourceType.DATASET, source.rid, f"{_SKIP_PREFIX} failed to read exemptions from the source: {error}"
            )
            return False
        # A locked run's exemptions are named after the source run, so re-locking the run on the destination
        # recreates them under the destination run's RID. Copying them as well would leave a duplicate that
        # unlocking the run could never remove.
        exemptions = [exemption for exemption in source_exemptions if exemption.source_id not in locked_run_rids]
        if not exemptions:
            return True
        if self.ctx.dry_run:
            logger.info(
                "%s Would copy %d retention exemption(s) of dataset %s", DRY_RUN_PREFIX, len(exemptions), source.rid
            )
            return True
        destination = self._destination_dataset(source)
        if destination is None:
            return True

        def upsert_all() -> None:
            for exemption in exemptions:
                destination.upsert_retention_exemption(
                    exemption.source_id, exemption.start, exemption.end, description=exemption.description
                )
            logger.info("Copied %d retention exemption(s) of dataset %s", len(exemptions), source.rid)

        return self._attempt(
            ResourceType.DATASET, source.rid, f"copy retention exemptions of dataset {source.rid}", upsert_all
        )

    def _lock_run(self, source: Run) -> bool:
        """Lock the destination copy of a source run, returning whether the lock is in place."""
        if source.end is None:
            self._record_skip(
                ResourceType.RUN, source.rid, f"{_SKIP_PREFIX} locked run has no end time, so it can't be locked"
            )
            return False
        if self.ctx.dry_run:
            logger.info("%s Would lock run %s", DRY_RUN_PREFIX, source.rid)
            return True
        destination_rid = self.ctx.migration_state.get_mapped_rid(ResourceType.RUN, source.rid)
        if destination_rid is None:
            # Its exemptions were left out of the dataset copy because the lock was to recreate them.
            self._record_skip(ResourceType.RUN, source.rid, f"{_SKIP_PREFIX} locked run was not migrated")
            return False

        def lock() -> None:
            self._destination_client().get_run(destination_rid).lock()
            logger.info("Locked run %s on the destination", source.rid)

        return self._attempt(ResourceType.RUN, source.rid, f"lock run {source.rid}", lock)

    def _datasets_of(self, run: Run) -> set[str] | None:
        """Source dataset RIDs a lock on this run would have exempted, or None if they can't be listed."""
        try:
            return {dataset.rid for asset in run.list_assets() for _, dataset in asset.list_datasets()}
        except NominalError as error:
            logger.warning("Could not list the datasets of run %s, so no expiry will be set: %s", run.rid, error)
            return None

    def _copy_policy(self, source: Dataset, *, withhold_expiry: bool) -> None:
        policy = source.retention_policy
        if policy is None:
            self._record_skip(ResourceType.DATASET, source.rid, f"{_SKIP_PREFIX} source policy type is not recognized")
        elif isinstance(policy, KeepUntilExpiry):
            self._copy_expiry(source, policy, withhold_expiry)
        else:
            self._clear_stale_expiry(source)

    def _copy_expiry(self, source: Dataset, policy: KeepUntilExpiry, withhold: bool) -> None:
        if withhold:
            self._record_skip(
                ResourceType.DATASET,
                source.rid,
                f"{_SKIP_PREFIX} expiry not set, because its exemptions or run locks were not all copied",
            )
            return
        if self.ctx.dry_run:
            logger.info(
                "%s Would set retention of dataset %s to %d day(s)", DRY_RUN_PREFIX, source.rid, policy.expiry_days
            )
            return
        destination = self._destination_dataset(source)
        if destination is None or destination.retention_policy == policy:
            return

        def set_expiry() -> None:
            destination.set_retention_policy(policy)
            logger.info("Set retention of dataset %s to %d day(s)", source.rid, policy.expiry_days)

        self._attempt(ResourceType.DATASET, source.rid, f"set retention policy of dataset {source.rid}", set_expiry)

    def _clear_stale_expiry(self, source: Dataset) -> None:
        """A new dataset already keeps its data forever, but a rerun can find an expiry copied earlier."""
        if self.ctx.dry_run:
            return
        destination = self._destination_dataset(source)
        if destination is None or not isinstance(destination.retention_policy, KeepUntilExpiry):
            return
        self._attempt(
            ResourceType.DATASET,
            source.rid,
            f"clear the retention policy of dataset {source.rid}",
            lambda: destination.set_retention_policy(None),
        )

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

    def _attempt(self, resource_type: ResourceType, source_rid: str, action: str, fn: Callable[[], None]) -> bool:
        """Run `fn`, returning whether it succeeded.

        Stops the whole copy when the destination refuses retention changes, since that applies to every
        remaining call and continuing would only repeat it. Any other failure is specific to this resource, so
        it is recorded as a skip and a rerun retries it.
        """
        try:
            fn()
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
            self._record_skip(resource_type, source_rid, f"{_SKIP_PREFIX} failed to {action}: {error}")
            return False
        return True

    def _record_skip(self, resource_type: ResourceType, source_rid: str, reason: str) -> None:
        self.ctx.migration_state.record_skip(resource_type, source_rid, reason)
        logger.warning("Skipping retention for %s %s: %s", resource_type.value.lower(), source_rid, reason)
