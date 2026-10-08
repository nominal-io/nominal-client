for dataset_file in job.as_files_ingested():
    print(f"{dataset_file.name}: {dataset_file.ingest_status}")
