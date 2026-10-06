Install the Nominal SDK for Core in Python with `pip`:

```shell
pip install nominal
```

<details>
<summary>Install optional features for HDF5 and Protobuf support</summary>

Install extra features, such as:
* HDF5 support for ingesting HDF5 files:
        ```shell
        pip install "nominal[hdf5]"
        ```
* Protobuf support for streaming data using protobuf:
        ```shell
        pip install "nominal[protos]"
        ```

These opt-in features install additional, heavier dependencies on your system.

</details>
