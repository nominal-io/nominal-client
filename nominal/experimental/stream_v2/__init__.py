
    def get_multiprocessed_write_stream(
        self,
        max_batch_size: int = 30000,
        max_wait: timedelta = timedelta(seconds=1),
        max_queue_size: int = 0,
        max_workers: int = 10,
    ) -> WriteStreamV2:
        """Stream to write non-blocking messages to a datasource.

        Args:
        ----
            max_batch_size (int): How big the batch can get before writing to Nominal. Default 50,000
            max_wait (timedelta): How long a batch can exist before being flushed to Nominal. Default 1 second
            data_format (Literal["json", "protobuf"]): Send data as protobufs or as json. Default json
            backpressure_mode (BackpressureMode): How to handle queue overflow. Default BLOCK
            max_queue_size (int): Maximum number of items that can be queued (0 for unlimited). Default 0
            max_workers (int): Maximum number of threads to use for parallel processing. Default 4
        """
        try:
            from nominal.core.batch_processor_proto import serialize_batch
        except ImportError:
            raise ImportError("nominal-api-protos is required to use get_write_stream_v2 with data_format='protobuf'")

        return WriteStreamV2.create(
            nominal_data_source_rid=self.nominal_data_source_rid,
            serialize_batch=serialize_batch,
            max_batch_size=max_batch_size,
            max_wait=max_wait,
            max_queue_size=max_queue_size,
            client_factory=lambda: self._clients.proto_write,
            auth_header=self._clients.auth_header,
            max_workers=max_workers,
        )
