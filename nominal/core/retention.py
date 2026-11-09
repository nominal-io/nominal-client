from __future__ import annotations

from dataclasses import dataclass
from typing import Self, TypeAlias, assert_never

from nominal_api import scout_catalog

from nominal.protos.storage.v1 import dataset_retention_pb2
from nominal.ts import IntegralNanosecondsUTC, _SecondsNanos


@dataclass(frozen=True)
class KeepForever:
    """Retain a dataset's data indefinitely. This is the behavior of a dataset with no policy set."""


@dataclass(frozen=True)
class KeepUntilExpiry:
    """Delete a dataset's data once it is older than `expiry_days`, unless covered by an exemption."""

    expiry_days: int
    """Age in days after which data becomes eligible for deletion."""


@dataclass(frozen=True)
class UnknownRetentionPolicy:
    """A policy of a type this client version does not recognize, such as one added to the server after this release.

    It can be read but not applied: `Dataset.set_retention_policy` rejects it.
    """

    type: str
    """The server's name for the policy type."""


RetentionPolicy: TypeAlias = KeepForever | KeepUntilExpiry | UnknownRetentionPolicy
"""How long a dataset's data is retained."""


@dataclass(frozen=True)
class RetentionExemption:
    """A time range of a dataset's data that retention must not delete."""

    dataset_rid: str
    """RID of the dataset the exemption applies to."""
    source_id: str
    """Identifies what owns the exemption. Locking a run creates exemptions whose source ID is the run's RID."""
    start: IntegralNanosecondsUTC
    """Start of the exempt range, in UTC nanoseconds."""
    end: IntegralNanosecondsUTC
    """End of the exempt range, in UTC nanoseconds."""
    description: str | None
    """Why the range is exempt, if one was given."""

    @classmethod
    def _from_proto(cls, exemption: dataset_retention_pb2.DatasetRetentionExemption) -> Self:
        return cls(
            dataset_rid=exemption.dataset_rid,
            source_id=exemption.source_id,
            start=_SecondsNanos.from_proto(exemption.bounds.start).to_nanoseconds(),
            end=_SecondsNanos.from_proto(exemption.bounds.end).to_nanoseconds(),
            description=exemption.description if exemption.HasField("description") else None,
        )


def _policy_to_proto(policy: RetentionPolicy | None) -> dataset_retention_pb2.RetentionPolicy | None:
    match policy:
        case None:
            return None
        case KeepForever():
            return dataset_retention_pb2.RetentionPolicy(keep_forever=dataset_retention_pb2.KeepForever())
        case KeepUntilExpiry(expiry_days=expiry_days):
            return dataset_retention_pb2.RetentionPolicy(
                keep_until_expiry=dataset_retention_pb2.KeepUntilExpiry(expiry_days=expiry_days)
            )
        case UnknownRetentionPolicy(type=policy_type):
            raise ValueError(f"Cannot set a retention policy of unrecognized type {policy_type!r}.")
        case _:
            assert_never(policy)


def _policy_from_conjure(policy: scout_catalog.RetentionPolicy) -> RetentionPolicy:
    """Convert a catalog policy, keeping any type this client version does not recognize as `UnknownRetentionPolicy`."""
    match policy.type:
        case scout_catalog.RetentionPolicyType.KEEP_FOREVER:
            return KeepForever()
        case scout_catalog.RetentionPolicyType.KEEP_UNTIL_EXPIRY if policy.expiry_days is not None:
            return KeepUntilExpiry(expiry_days=policy.expiry_days)
        case _:
            return UnknownRetentionPolicy(type=policy.type.name)
