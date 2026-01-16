import requests

"""
Use this script to send a HTTP request to the function app running on localhost

Please check the readme for instructions for running the function app locally

# Example usage
`python3 scripts/trigger_redaction.py`

"""


url = "http://localhost:7071/api/redact"
resp = requests.post(
    url,
    json={
        "tryApplyProvisionalRedactions": True,
        "skipRedaction": True,
        "ruleName": "default",
        "fileKind": "pdf",
        "readDetails": {
            "storageKind": "AzureBlob",
            "teamEmail": "someAccount@planninginspectorate.gov.uk",
            "properties": {
                "blobPath": "somefile.pdf",
                "storageName": "pinsstredactiondevuks",
                "containerName": "somecontainer",
            },
        },
        "writeDetails": {
            "storageKind": "AzureBlob",
            "teamEmail": "someAccount@planninginspectorate.gov.uk",
            "properties": {
                "blobPath": "somefile_PROPOSED_REDACTIONS.pdf",
                "storageName": "pinsstredactiondevuks",
                "containerName": "somecontainer",
            },
        },
    },
)
print(resp.json())
