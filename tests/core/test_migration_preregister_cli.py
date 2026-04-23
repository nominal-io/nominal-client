from __future__ import annotations

from pathlib import Path

import pytest
from click.testing import CliRunner

from nominal.experimental.migration.preregister_cli import MAX_EMAILS_PER_REQUEST, _collect_emails, main


def test_collect_emails_combines_file_args_comments_and_duplicates(tmp_path: Path) -> None:
    emails_file = tmp_path / "emails.txt"
    emails_file.write_text("beta@example.com\n# comment\nalpha@example.com\nbeta@example.com\n", encoding="utf-8")

    result = _collect_emails((" alpha@example.com ", "", "gamma@example.com"), str(emails_file))

    assert result == ["alpha@example.com", "gamma@example.com", "beta@example.com"]


def test_collect_emails_raises_when_no_emails_provided() -> None:
    with pytest.raises(Exception, match="Provide at least one email"):
        _collect_emails((), None)


def test_main_requires_profile() -> None:
    runner = CliRunner()
    result = runner.invoke(main, ["user@example.com"])

    assert result.exit_code != 0
    assert "Missing option '--profile'." in result.output


def test_main_uses_explicit_profile_and_batches_requests(monkeypatch: pytest.MonkeyPatch) -> None:
    captured_profiles: list[str] = []
    preregister_batches: list[list[str]] = []

    class FakeUser:
        def __init__(self, rid: str) -> None:
            self.rid = rid

    class FakeClient:
        @staticmethod
        def from_profile(profile: str) -> object:
            captured_profiles.append(profile)
            return object()

    def fake_preregister_users(_client: object, emails: list[str]) -> dict[str, FakeUser]:
        preregister_batches.append(list(emails))
        return {email: FakeUser(f"ri.authn.dev.user.{index}") for index, email in enumerate(emails)}

    monkeypatch.setattr("nominal.experimental.migration.preregister_cli.NominalClient", FakeClient)
    monkeypatch.setattr("nominal.experimental.migration.preregister_cli.preregister_users", fake_preregister_users)

    emails = [f"user{i}@example.com" for i in range(MAX_EMAILS_PER_REQUEST + 3)]
    runner = CliRunner()
    result = runner.invoke(main, ["--profile", "wisk_gcp", *emails])

    assert result.exit_code == 0
    assert captured_profiles == ["wisk_gcp"]
    assert preregister_batches == [
        emails[:MAX_EMAILS_PER_REQUEST],
        emails[MAX_EMAILS_PER_REQUEST:],
    ]
    assert "Processed 1003 email(s) with profile 'wisk_gcp'." in result.output
    assert "Preregistered 1003 new user(s)." in result.output
    assert "Skipped 0 existing user(s)." in result.output
