# Core

`nominal.core` is the object-oriented client for the Nominal platform. Start from a {py:class}`~nominal.core.NominalClient`, and work with the assets, runs, datasets and other resources it returns.

## Client

```{eval-rst}
.. autosummary::
   :toctree: generated
   :nosignatures:

   ~nominal.core.NominalClient
   ~nominal.core.Workspace
   ~nominal.core.User
   ~nominal.core.HeaderProvider
```

## Assets and runs

```{eval-rst}
.. autosummary::
   :toctree: generated
   :nosignatures:

   ~nominal.core.Asset
   ~nominal.core.Run
   ~nominal.core.Attachment
   ~nominal.core.LinkDict
   ~nominal.core.Comment
```

## Datasets and channels

```{eval-rst}
.. autosummary::
   :toctree: generated
   :nosignatures:

   ~nominal.core.Dataset
   ~nominal.core.DatasetFile
   ~nominal.core.DataSource
   ~nominal.core.Channel
   ~nominal.core.ChannelDataType
   ~nominal.core.filter_channels_with_data
   ~nominal.core.Bounds
   ~nominal.core.Unit
   ~nominal.core.UnitLike
   ~nominal.core.ArchiveStatusFilter
```

## Streaming

```{eval-rst}
.. autosummary::
   :toctree: generated
   :nosignatures:

   ~nominal.core.WriteStream
   ~nominal.core._stream.write_stream.DataStream
   ~nominal.core._stream.write_stream.LogStream
   ~nominal.core.LogPoint
   ~nominal.core.Connection
```

## Ingestion

```{eval-rst}
.. autosummary::
   :toctree: generated
   :nosignatures:

   ~nominal.core.IngestionJob
   ~nominal.core.IngestionJobStatus
   ~nominal.core.IngestType
   ~nominal.core.IngestWaitType
   ~nominal.core.FileType
   ~nominal.core.FileTypes
   ~nominal.core.TimestampMetadata
   ~nominal.core.as_files_ingested
   ~nominal.core.wait_for_files_to_ingest
```

## Containerized extractors

```{eval-rst}
.. autosummary::
   :toctree: generated
   :nosignatures:

   ~nominal.core.ContainerizedExtractor
   ~nominal.core.ContainerImage
   ~nominal.core.ContainerImageStatus
   ~nominal.core.FileExtractionInput
   ~nominal.core.FileExtractionParameter
   ~nominal.core.FileOutputFormat
   ~nominal.core.ExitCodeMapping
   ~nominal.core.Secret
```

## Events and checklists

```{eval-rst}
.. autosummary::
   :toctree: generated
   :nosignatures:

   ~nominal.core.Event
   ~nominal.core.EventType
   ~nominal.core.EventDisposition
   ~nominal.core.SearchEventOriginType
   ~nominal.core.Priority
   ~nominal.core.Checklist
   ~nominal.core.CheckViolation
   ~nominal.core.DataReview
   ~nominal.core.DataReviewBuilder
```

## Video

```{eval-rst}
.. autosummary::
   :toctree: generated
   :nosignatures:

   ~nominal.core.Video
   ~nominal.core.VideoFile
   ~nominal.core.VideoDatasetFile
```

## Workbooks

```{eval-rst}
.. autosummary::
   :toctree: generated
   :nosignatures:

   ~nominal.core.Workbook
   ~nominal.core.WorkbookTemplate
   ~nominal.core.WorkbookType
   ~nominal.core.WorkspaceSearchType
```

## Other types

```{eval-rst}
.. autosummary::
   :toctree: generated
   :nosignatures:

   ~nominal.core.Marking
   ~nominal.core.Symbol
   ~nominal.core.SymbolKind
```
