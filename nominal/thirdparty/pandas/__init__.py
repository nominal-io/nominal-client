try:
    from nominal.thirdparty.pandas._pandas import upload_pandas as upload_pandas
except ModuleNotFoundError:
    raise ModuleNotFoundError("please install the nominal pandas extension: `nominal[pandas]`")
