# Datasets and files

```python
from nominal.core import Dataset, DatasetFile, VideoDatasetFile
```

For usage and examples, see the [Datasets guide](/guides/datasets/overview.md).

## Datasets

```{eval-rst}
.. automodule:: nominal.core.dataset
```

## Dataset files

```{eval-rst}
.. automodule:: nominal.core.dataset_file
```

## Video dataset files

```{eval-rst}
.. automodule:: nominal.core.video_dataset_file
```

## File types

```{eval-rst}
.. automodule:: nominal.core.filetype
```

## Shared interface

`Dataset` and `Connection` inherit channel, streaming, and export operations from
`DataSource`. Those methods are documented on the concrete classes above and on the
[Streaming](streaming.md) page.

:::{dropdown} Shared DataSource interface
```{eval-rst}
.. automodule:: nominal.core.datasource
```
:::
