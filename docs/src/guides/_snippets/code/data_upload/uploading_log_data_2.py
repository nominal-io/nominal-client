# NOTE: refname needs to match the refname used to add the dataset to the asset
dataset = asset.get_dataset("flight_controller_logs")
dataset.add_journal_json("path/to/other/log.jsonl")
