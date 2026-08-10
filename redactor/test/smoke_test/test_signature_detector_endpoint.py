"""Send a test request to the local signature detector endpoint."""

import argparse
import json
from pathlib import Path

import requests


def main():
    parser = argparse.ArgumentParser(
        description="Send a test request to the local signature detector endpoint."
    )
    parser.add_argument(
        "--endpoint-url",
        type=str,
        help="URL of the local signature detector endpoint.",
    )
    parser.add_argument(
        "--image",
        type=str,
        help="Path to the image file to send to the endpoint.",
    )
    args = parser.parse_args()

    image_path = Path(args.image)
    image_bytes = image_path.read_bytes()
    payload = json.dumps(
        {
            "image": image_bytes.hex(),
            "threshold": 0.5,
        }
    )

    response = requests.post(
        args.endpoint_url,
        data=payload,
        headers={"Content-Type": "application/json"},
        timeout=300,
    )

    result = response.json()
    print(json.dumps(result, indent=2))

    assert response.status_code == 200, (
        f"Request failed with status code {response.status_code} and response: {response.text}"
    )


if __name__ == "__main__":
    main()
