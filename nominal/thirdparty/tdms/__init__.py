try:
    from nominal.thirdparty.tdms._tdms import upload_tdms as upload_tdms
except ModuleNotFoundError:
    raise ModuleNotFoundError("please install the nominal tdms extension: `nominal[tdms]`")
