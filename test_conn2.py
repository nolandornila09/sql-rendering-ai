from azure.storage.blob import BlobServiceClient
import os
from dotenv import load_dotenv

load_dotenv()

account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
account_key = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")

blob_service_client = BlobServiceClient(
    account_url=f"https://{account_name}.blob.core.windows.net",
    credential=account_key
)

container_name = "container-cci-bc-cronus"
folder = "Nolan Test Parquet/"

container_client = blob_service_client.get_container_client(container_name)

print("Blobs in folder:")
for blob in container_client.list_blobs(name_starts_with=folder):
    print(blob.name)
