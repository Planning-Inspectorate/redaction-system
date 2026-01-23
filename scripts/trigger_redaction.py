import time
import requests


"""
Use this script to send a HTTP request to the function app running on localhost

Please check the readme for instructions for running the function app locally

# Example usage
`python3 scripts/trigger_redaction.py`

"""

START_URL = "http://localhost:7071/api/redact"

resp = requests.post(
    START_URL,
    json={
        "tryApplyProvisionalRedactions": True,
        "skipRedaction": True,
        "ruleName": "default",
        "fileKind": "pdf",
        "readDetails": {
            "storageKind": "AzureBlob",
            "teamEmail": "someAccount@planninginspectorate.gov.uk",
            "properties": {
                "blobPath": "samples/PINS_anon_samples_source.pdf",
                "storageName": "pinsstredactiontestuks",
                "containerName": "pinsfuncredactionsystemtestuks-applease",
            },
        },
        "writeDetails": {
            "storageKind": "AzureBlob",
            "teamEmail": "someAccount@planninginspectorate.gov.uk",
            "properties": {
                "blobPath": "samples/PINS_anon_samples_source_REDACTED.pdf",
                "storageName": "pinsstredactiontestuks",
                "containerName": "pinsfuncredactionsystemtestuks-applease",
            },
        },
    },
)

resp.raise_for_status()
data = resp.json()

status_url = data["pollEndpoint"]
print(f"Started: {data['id']}")

while True:
    status = requests.get(status_url).json()
    state = status["runtimeStatus"]

    print(f"Status: {state}")

    if state in ("Completed", "Failed", "Terminated"):
        print("\nFinal result:")
        print(status.get("output") or status.get("customStatus"))
        break

    time.sleep(2)
