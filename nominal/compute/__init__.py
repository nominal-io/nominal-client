"""The `nominal.compute` module re-exports the `nominal_compute` package, which provides core functionality for
composing compute queries.

`nominal_compute` is a compiled package distributed as platform-specific wheels; this module simply re-exports its
public API so it is accessible as a submodule of `nominal`:

```python
from nominal.compute import NumericSeries

series = NumericSeries(...)
series.abs().sqrt()
```
"""

from nominal_compute import *  # noqa: F403
from nominal_compute import __all__ as __all__
