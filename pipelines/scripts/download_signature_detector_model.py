import argparse
import os

from huggingface_hub import hf_hub_download

MODEL_REPO_ID = "tech4humans/conditional-detr-50-signature-detector"
REVISION = "9582471a229a9cbbe721e56dcd4e95e1fef7cb8b"  # pragma: allowlist secret


def download_model(
    model_dir: str, model_repo_id: str = MODEL_REPO_ID, revision: str = REVISION
) -> None:
    os.makedirs(model_dir, exist_ok=True)
    for f in ["config.json", "model.safetensors", "preprocessor_config.json"]:
        hf_hub_download(
            repo_id=model_repo_id,
            filename=f,
            revision=revision,
            local_dir=model_dir,
        )

    import shutil

    cache = os.path.join(model_dir, ".cache")
    if os.path.exists(cache):
        shutil.rmtree(cache)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--model-dir", type=str)
    parser.add_argument("--model-repo-id", type=str, default=MODEL_REPO_ID)
    parser.add_argument("--revision", type=str, default=REVISION)
    args = parser.parse_args()

    download_model(args.model_dir, args.model_repo_id, args.revision)
