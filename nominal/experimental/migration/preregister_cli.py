from __future__ import annotations

from collections.abc import Iterable, Sequence

import click

from nominal.core.client import NominalClient
from nominal.experimental.migration.preregister import preregister_users

MAX_EMAILS_PER_REQUEST = 1000


def _chunked(values: Sequence[str], size: int) -> Iterable[Sequence[str]]:
    for index in range(0, len(values), size):
        yield values[index : index + size]


def _collect_emails(emails: Sequence[str], emails_file: str | None) -> list[str]:
    combined_emails = [email.strip() for email in emails]

    if emails_file is not None:
        with open(emails_file, encoding="utf-8") as handle:
            combined_emails.extend(line.strip() for line in handle)

    deduped_emails: list[str] = []
    seen_emails: set[str] = set()
    for email in combined_emails:
        if not email or email.startswith("#"):
            continue
        if email in seen_emails:
            continue
        seen_emails.add(email)
        deduped_emails.append(email)

    if not deduped_emails:
        raise click.UsageError("Provide at least one email via EMAIL arguments or --emails-file.")

    return deduped_emails


@click.command()
@click.argument("emails", nargs=-1)
@click.option(
    "--emails-file",
    type=click.Path(dir_okay=False, exists=True, readable=True),
    help="Optional newline-delimited file of email addresses to preregister.",
)
@click.option(
    "--profile",
    required=True,
    help="Named Nominal profile used to authenticate the destination tenant.",
)
def main(emails: Sequence[str], emails_file: str | None, profile: str) -> None:
    """Preregister users in a destination tenant before first login."""
    normalized_emails = _collect_emails(emails, emails_file)
    client = NominalClient.from_profile(profile)

    preregistered_users: dict[str, str] = {}
    for batch in _chunked(normalized_emails, MAX_EMAILS_PER_REQUEST):
        batch_result = preregister_users(client, batch)
        preregistered_users.update({email: user.rid for email, user in batch_result.items()})

    skipped_count = len(normalized_emails) - len(preregistered_users)
    click.echo(f"Processed {len(normalized_emails)} email(s) with profile '{profile}'.")
    click.echo(f"Preregistered {len(preregistered_users)} new user(s).")
    click.echo(f"Skipped {skipped_count} existing user(s).")

    if preregistered_users:
        click.echo("")
        for email, user_rid in preregistered_users.items():
            click.echo(f"{email}\t{user_rid}")
