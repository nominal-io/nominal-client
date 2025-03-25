import click

import nominal.io as io
from nominal.cli import config


@click.group(context_settings={"show_default": True, "help_option_names": ("-h", "--help")})
@click.version_option(io.__version__)
def nom() -> None:
    pass


nom.add_command(config.config_cmd)
