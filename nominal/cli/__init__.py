import click

import nominal
from nominal.cli import attachment, auth, config, dataset, run


@click.group(context_settings={"show_default": True, "help_option_names": ("-h", "--help")})
@click.version_option(nominal.__version__)
def nom() -> None:
    pass


nom.add_command(attachment.attachment_cmd)
nom.add_command(auth.auth_cmd)
nom.add_command(dataset.dataset_cmd)
nom.add_command(run.run_cmd)
nom.add_command(config.config_cmd)
