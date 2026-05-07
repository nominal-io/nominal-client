import importlib.metadata

import click

from nominal.cli import attachment, config, dataset, download, mis, run


@click.group(context_settings={"show_default": True, "help_option_names": ("-h", "--help")})
@click.version_option(importlib.metadata.version("nominal"))
def nom() -> None:
    pass


nom.add_command(attachment.attachment_cmd)
nom.add_command(config.config_cmd)
nom.add_command(dataset.dataset_cmd)
nom.add_command(download.download_cmd)
nom.add_command(mis.mis_cmd)
nom.add_command(run.run_cmd)
