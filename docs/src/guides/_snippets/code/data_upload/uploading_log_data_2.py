# Reuse the same refname for the next file.
dataset = asset.get_or_create_dataset(
    "flight_controller_logs", name="Flight Controller Logs"
)
log_file = dataset.add_journal_json("path/to/other/log.jsonl")
uploaded_files.append(log_file)
