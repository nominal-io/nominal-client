When working with assets, users generally work with data from many distinct test events.
As shown in other sections of the tutorial, this is generally accomplished by repeatedly uploading data to a set of datasources within an asset.
For example, if every test event generates some CSV, Parquet, log, and video files, these will be uploaded to the same set of datasources for each test event.
However, as useful as it is to see all of the data for an asset in a single place, frequently it is useful to investigate a single test event and all of the data associated with it.

This is where Runs come in to play!
A run is defined by a start and end time on an existing asset that is a view on all of the files and datasources attached to that asset.
When creating workbooks, running checklists, or doing other validation on your data, it is useful to be able to perform these tasks on a single flight test.
Create these runs explicitly when uploading data to Nominal.

```{literalinclude} /guides/_snippets/code/getting_started/creating_a_run_1.py
:language: python
```

:::{tip}

Determining the correct start / end bounds for a `nominal.Run` can be challenging when you have a large number of data files being ingested.
A good practice is to create the `nominal.Run` early on in the data ingestion script and to update the bounds as you go.

An example of doing this would look like:

```{literalinclude} /guides/_snippets/code/getting_started/creating_a_run_2.py
:language: python
```

Whether you add run bounds ahead of time or as you go, it is
important to set them correctly, since they influence data
visualization on the website ( e.g. when viewing workbooks on
runs).
:::
