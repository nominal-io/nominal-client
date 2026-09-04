"""Tests for nominal.core._utils.filenames — the upload-filename safety rules."""

from __future__ import annotations

import pytest

from nominal.core._utils.filenames import (
    UNSAFE_UPLOAD_CHARS,
    find_unsafe_chars,
    sanitize_upload_filename,
    validate_upload_filename,
)

# Characters the empirical probe showed must round-trip cleanly on S3 + Azure Blob.
SAFE_NAMES = [
    "plain_baseline",
    "with space",
    "paren(reduced)",
    "amp&ersand",
    "plus+plus",
    "hash#tag",
    "bracket[1]",
    "at@sign",
    "equals=sign",
    "comma,comma",
    "semi;colon",
    "tilde~caret^",
    "dollar$sign",
    "unicode_résumé_日本",
]


@pytest.mark.parametrize("name", SAFE_NAMES)
def test_safe_names_pass_validation(name: str) -> None:
    validate_upload_filename(name)  # must not raise
    assert find_unsafe_chars(name) == set()
    assert sanitize_upload_filename(name) == name  # safe names are unchanged


@pytest.mark.parametrize("char", sorted(UNSAFE_UPLOAD_CHARS))
def test_each_unsafe_char_is_rejected(char: str) -> None:
    name = f"file{char}name"
    with pytest.raises(ValueError, match="unsafe for storage"):
        validate_upload_filename(name)
    assert char in find_unsafe_chars(name)


@pytest.mark.parametrize("char", ["\x00", "\t", "\x1f", "\x7f", "\x85"])  # C0, tab, C0, DEL, C1
def test_control_characters_are_unsafe(char: str) -> None:
    name = f"file{char}name"
    assert char in find_unsafe_chars(name)
    with pytest.raises(ValueError, match="unsafe for storage"):
        validate_upload_filename(name)


def test_error_lists_all_offending_characters() -> None:
    with pytest.raises(ValueError) as exc:
        validate_upload_filename("a?b%c{")
    message = str(exc.value)
    for char in ("?", "%", "{"):
        assert repr(char) in message


def test_sanitize_replaces_unsafe_chars() -> None:
    assert sanitize_upload_filename("paren%28reduced%29") == "paren_28reduced_29"
    assert sanitize_upload_filename("a/b\\c?d") == "a_b_c_d"
    # a sanitized name is always valid to upload
    validate_upload_filename(sanitize_upload_filename("bad/{name}?"))


def test_sanitize_custom_replacement() -> None:
    assert sanitize_upload_filename("a/b", replacement="-") == "a-b"
