# TDMS

Upload TDMS files to Nominal with the `tdms` extra:

```sh
pip install "nominal[tdms]"
```

Use {py:func}`~nominal.tdms.upload_tdms` to create a {term}`dataset`, or
{py:func}`~nominal.tdms.upload_tdms_to_dataset` to add a file to an existing dataset.
Provide both `timestamp_column` and `timestamp_type`, or omit both to use each {term}`channel`'s
`wf_start_time` and `wf_increment` properties.

## API Reference

```{eval-rst}
.. include:: ../_summaries/nominal.tdms.rst
```
