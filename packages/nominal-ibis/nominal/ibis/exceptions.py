from __future__ import annotations

from ibis.common.exceptions import IbisError


class NominalSqlError(IbisError):
    """A catalog type or query result that cannot be represented by this backend."""
