try:
    from nominal.thirdparty.polars._polars import upload_dataframe as upload_dataframe
except ModuleNotFoundError:
    raise ModuleNotFoundError("please install the nominal polars extension: `nominal[polars]`")
