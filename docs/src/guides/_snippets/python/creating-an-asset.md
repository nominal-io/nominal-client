To start, get or create an `Asset`, one per glider. The properties identify the glider on each upload; if no asset matches, the client creates it. If multiple assets match, it raises `ValueError`.

```{literalinclude} /guides/_snippets/code/getting_started/creating_an_asset_1.py
:language: python
```

:::{tip}

Reuse the returned `asset` for this flight's uploads and run. The name and description are creation defaults; they do not update an existing asset.

Use {py:obj}`NominalClient.search_assets <nominal.core.NominalClient.search_assets>` when your integration needs a custom lookup policy, such as requiring an asset to exist already.
:::

See the {py:obj}`Asset function reference <nominal.core.Asset>` for more information on working with assets.
