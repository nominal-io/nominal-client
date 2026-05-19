import warnings

from nominal.tdms import upload_tdms, upload_tdms_to_dataset

warnings.warn(
    "nominal.thirdparty.tdms is deprecated and will be removed in a future version. Use nominal.tdms instead.",
    DeprecationWarning,
    stacklevel=2,
)

__all__ = [
    "upload_tdms",
    "upload_tdms_to_dataset",
]
