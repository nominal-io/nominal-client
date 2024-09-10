class NominalError(Exception):
    pass


class NominalIngestError(NominalError):
    pass


class NominalIngestFailed(NominalIngestError):
    pass


class NominalMultipartUploadFailed(NominalError):
    pass


class NominalConfigError(NominalError):
    pass
