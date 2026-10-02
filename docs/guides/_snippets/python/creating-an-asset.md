To start, create an `Asset` to upload data to, one per glider.
With only a few assets, this could be done via the frontend.
With thousands of gliders, automate the process using Python.

```{literalinclude} /guides/_snippets/code/getting_started/creating_an_asset_1.py
:language: python
```

:::{tip}

This step happens once per asset.
Future test events upload to the same asset created initially.
:::

Once an asset exists, it is useful to write a function that can look up an asset for uploading data to later.
Here is an example that uses the example's `platform` and `serial_num` properties:

```{literalinclude} /guides/_snippets/code/getting_started/creating_an_asset_2.py
:language: python
```

:::{tip}

Later, once data from a test event is in hand, this helper function looks up the correct asset to append data to.
:::

See the {py:obj}`Asset function reference <nominal.core.Asset>` for more information on what you can do with a `nominal.Asset`.
