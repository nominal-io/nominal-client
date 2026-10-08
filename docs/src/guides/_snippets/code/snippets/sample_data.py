from huggingface_hub import hf_hub_download

dataset_path = hf_hub_download(
    repo_id=f"{dataset_repo_id}", filename=f"{dataset_filename}", repo_type="dataset"
)

print(f"File saved to: {dataset_path}")
