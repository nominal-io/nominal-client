# SDK reference

{.lead}
The API reference for the `nominal` package. For installation, quickstarts, and how-to guides,
see the [guides](/index.md).

[Authoring checklists as code](../guides/checklists/authoring.md) covers creating and updating
checklists, writing checks, and running them against your data. The
{download}`deploy_checklist.py </examples/deploy_checklist.py>` is a complete test-stand script.

The main components of the `nominal` package are:

- **[`nominal.core`](core/index.md)**: the object-oriented client for the Nominal core platform.
  Start from {py:class}`~nominal.core.NominalClient`.
- **[`nominal.ts`](ts/index.md)**: how timestamps in your data are encoded across the client.
- **[`nominal.exceptions`](exceptions.rst)**: errors raised by the client.
- **[`nominal.experimental`](experimental/index.md)**: cutting-edge experiments. These APIs may change without notice.
- **[Third-Party Integrations](thirdparty/index.md)**: integrations with popular third-party libraries and formats.

For command-line operations, see the [`nom` CLI reference](cli.md).
