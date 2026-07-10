"""Filename safety rules for object-store uploads.

Single source of truth for which characters are unsafe in an upload filename. Two policies
share the same rule: the upload path *validates* (raises) so callers fix bad names up front,
while migration *sanitizes* (replaces) so a bulk migration is never blocked by one bad file.

Background: the client used to ``quote_plus``-encode every upload filename, which corrupted names
on S3 (double-encoding, e.g. ``%20`` -> ``%2520``) and broke Azure Blob uploads outright
(percent-encoded blob names fail SAS signing with a 403). We now send filenames literally and
only reject/replace the characters that are genuinely unsafe.

The set below was determined empirically by uploading probe files with each character to S3
(gov staging) and Azure Blob (azure staging). GCS was not verified (its staging tier is behind
Google IAP, pending an auth integration); treat this set as a floor and extend it if GCS or a
new backend rejects more.
"""

from __future__ import annotations

# Characters that break at least one verified object-store backend when used literally:
#   /  \      path separators — truncate or nest the object key
#   %         literal percent breaks Azure Blob SAS signing (403 AuthenticationFailed)
#   ? { } '   rejected by the ingest service (S3: InvalidS3Path) or fail server-side ingestion (Azure)
# NOTE: verified on S3 + Azure Blob; GCS unverified (see module docstring). Extend as needed.
UNSAFE_UPLOAD_CHARS = frozenset("/\\?%{}'")


def _is_unsafe(char: str) -> bool:
    return char in UNSAFE_UPLOAD_CHARS or ord(char) < 0x20  # also reject control characters


def find_unsafe_chars(name: str) -> set[str]:
    """Return the set of unsafe characters present in ``name`` (includes control characters)."""
    return {char for char in name if _is_unsafe(char)}


def validate_upload_filename(name: str) -> None:
    """Raise ``ValueError`` if ``name`` contains characters unsafe for object-store upload."""
    unsafe = find_unsafe_chars(name)
    if unsafe:
        rendered = ", ".join(repr(char) for char in sorted(unsafe))
        raise ValueError(
            f"Upload filename {name!r} contains characters that are unsafe for storage: {rendered}. "
            "Remove or replace them before uploading."
        )


def sanitize_upload_filename(name: str, replacement: str = "_") -> str:
    """Replace unsafe characters in ``name`` with ``replacement``.

    Used by migration so a single unsafe source filename never blocks a bulk migration. Note this
    can map two distinct names to the same result; callers needing uniqueness must handle that.
    """
    return "".join(replacement if _is_unsafe(char) else char for char in name)
