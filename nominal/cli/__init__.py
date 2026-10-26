import importlib.metadata
import sys

import click

from nominal.cli import config, container, download, mis


@click.group(context_settings={"show_default": True, "help_option_names": ("-h", "--help")})
@click.version_option(importlib.metadata.version("nominal"))
def nom() -> None:
    pass


nom.add_command(config.config_cmd)
nom.add_command(container.container_cmd)
nom.add_command(download.download_cmd)
nom.add_command(mis.mis_cmd)

# migration_cli imports nominal.cli.util.global_decorators, which requires this package
# to be initialized — guard against the circular import when migration_cli is imported
# directly (e.g. in tests) before nominal.cli has finished loading.
if "nominal.experimental.migration.migration_cli" not in sys.modules:
    from nominal.experimental.migration.migration_cli import migrate_cmd

    nom.add_command(migrate_cmd)
