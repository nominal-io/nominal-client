When working with assets, users generally work with data from many distinct test events.
As shown in other sections of the tutorial, this is generally accomplished by repeatedly uploading data to a set of datasources within an asset.
For example, if every test event generates some CSV, Parquet, log, and video files, these will be uploaded to the same set of datasources for each test event.
However, as useful as it is to see all of the data for an asset in a single place, frequently it is useful to investigate a single test event and all of the data associated with it.

This is where Runs come in to play!
A run is a view on the files and datasources attached to an asset, with an absolute start time and an optional end time.
When creating workbooks, running checklists, or doing other validation on your data, it is useful to be able to perform these tasks on a single flight test.
Create these runs explicitly after the files for the test have finished ingesting. Choose one of the following recipes: known bounds, or bounds derived from the retained upload handles.

```{literalinclude} /guides/_snippets/code/getting_started/creating_a_run_1.py
:language: python
```

:::{tip}

If the test's bounds are not known in advance, retain the `DatasetFile` returned by every upload for that test and wait for ingestion before creating the run. Use those files' bounds; `Dataset.bounds` spans every flight accumulated on the dataset.

This alternative uses `uploaded_files` collected in the upload stage, covering every data modality produced by the same test:

```{literalinclude} /guides/_snippets/code/getting_started/creating_a_run_2.py
:language: python
```

Whether you use known bounds or derive them from uploaded files, it is
important to set them correctly, since they influence data
visualization on the website ( e.g. when viewing workbooks on
runs).
:::
