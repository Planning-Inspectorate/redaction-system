import io
import json
import logging
import os

# Prevent transformers/huggingface_hub from making any network calls
os.environ["TRANSFORMERS_OFFLINE"] = "1"
os.environ["HF_HUB_OFFLINE"] = "1"

import torch
from PIL import Image
from transformers import AutoImageProcessor, AutoModelForObjectDetection
from transformers.models.conditional_detr import ConditionalDetrConfig

logger = logging.getLogger(__name__)

MODEL_NAME = "conditional-detr-50-signature-detector"
VERSION = "2"

processor = None
model = None


def init():
    global processor, model

    model_dir = os.getenv("AZUREML_MODEL_DIR")
    model_name = os.getenv("MODEL_NAME", MODEL_NAME)
    model_path = os.path.join(model_dir, model_name)

    logger.info(f"Loading model from: {model_path}")

    processor = AutoImageProcessor.from_pretrained(model_path, local_files_only=True)  # nosec: B615

    # Load config from JSON and inject backbone_config to prevent HF API calls
    config_file = os.path.join(model_path, "config.json")
    with open(config_file) as f:
        config_dict = json.load(f)

    # Replace backbone string with inline config to avoid repo_exists() call
    config_dict["use_pretrained_backbone"] = False
    config_dict["backbone"] = None
    config_dict["use_timm_backbone"] = True
    config_dict["backbone_config"] = {
        "model_type": "timm_backbone",
        "backbone": "resnet50",
        "num_channels": 3,
        "features_only": True,
        "use_pretrained_backbone": False,
        "out_indices": [1, 2, 3, 4],
    }

    config = ConditionalDetrConfig(**config_dict)
    model = AutoModelForObjectDetection.from_pretrained(
        model_path, config=config, local_files_only=True
    )  # nosec: B615

    model.eval()

    logger.info("Model loaded successfully")


def run(raw_data):
    try:
        request = json.loads(raw_data)
        image_bytes = bytes.fromhex(request["image"])
        threshold = request.get("threshold", 0.5)

        image = Image.open(io.BytesIO(image_bytes)).convert("RGB")

        inputs = processor(images=image, return_tensors="pt")

        with torch.no_grad():
            outputs = model(**inputs)

        target_sizes = torch.tensor([image.size[::-1]])
        results = processor.post_process_object_detection(
            outputs, target_sizes=target_sizes, threshold=threshold
        )[0]

        detections = []
        for score, label, box in zip(
            results["scores"], results["labels"], results["boxes"]
        ):
            detections.append(
                {
                    "score": round(score.item(), 4),
                    "label": model.config.id2label[label.item()],
                    "box": {
                        "x_min": round(box[0].item(), 2),
                        "y_min": round(box[1].item(), 2),
                        "x_max": round(box[2].item(), 2),
                        "y_max": round(box[3].item(), 2),
                    },
                }
            )

        return json.dumps({"detections": detections})

    except Exception as e:  # noqa: BLE001
        logger.error(f"Error during inference: {e}")
        return json.dumps({"error": "An internal error has occurred."})
