import click
from . import attachment, auth, dataset, run


@click.group(context_settings={"show_default": True})
def nom() -> None:
    pass


nom.add_command(attachment.attachment_cmd, "attachment")
nom.add_command(auth.auth_cmd, "auth")
nom.add_command(dataset.dataset_cmd, "dataset")
nom.add_command(run.run_cmd, "run")
