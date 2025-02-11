try:
    from nominal.thirdparty.polars._polars import upload_polars as upload_polars
except ModuleNotFoundError:
    raise ModuleNotFoundError("please install the nominal polars extension: `nominal[polars]`")
