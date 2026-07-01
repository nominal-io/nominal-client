from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from nominal.experimental.impersonation import as_user as as_user


# Lazy so importing a sibling package (e.g. nominal.experimental.extractor, which must stay
# stdlib-only for use inside minimal extractor container images) doesn't eagerly pull in
# nominal.core.client and its nominal_api/conjure_python_client dependency graph.
def __getattr__(name: str) -> Any:
    if name == "as_user":
        from nominal.experimental.impersonation import as_user

        return as_user
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
