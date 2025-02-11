try:
    from nominal.thirdparty.pandas._pandas import upload_dataframe as upload_dataframe
except ModuleNotFoundError:
    raise ModuleNotFoundError("please install the nominal pandas extension: `nominal[pandas]`")
