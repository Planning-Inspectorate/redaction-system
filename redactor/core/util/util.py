import os


class Util:
    @classmethod
    def get_storage_account(cls, storage_name: str = None) -> str:
        """
        Return the Blob service endpoint for the given storage account.
        Example: https://{storage_name}.blob.core.windows.net
        """
        if not storage_name:
            storage_name = os.environ.get("AZURE_STORAGE_ACCOUNT")
        if not storage_name:
            raise ValueError(
                "Storage account name not provided and AZURE_STORAGE_ACCOUNT environment variable is not set"
            )
        return f"https://{storage_name}.blob.core.windows.net"
