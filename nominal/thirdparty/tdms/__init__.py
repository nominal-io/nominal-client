import warnings

try:
    from nominal.tdms import upload_tdms, upload_tdms_to_dataset
except ModuleNotFoundError as e:
    if e.name in {"nominal.tdms", "nptdms"}:
        raise ImportError("TDMS support requires the 'tdms' extra: pip install 'nominal[tdms]'") from e
    raise

warnings.warn(
    "nominal.thirdparty.tdms is deprecated and will be removed in a future version. Use nominal.tdms instead.",
    DeprecationWarning,
    stacklevel=2,
)

__all__ = [
    "upload_tdms",
    "upload_tdms_to_dataset",
]
