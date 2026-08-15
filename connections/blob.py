"""Conexão com o Azure Blob Storage."""

import logging
import os

from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv


def connect_azure_blob_storage():
    load_dotenv()
    logging.info("Conectando ao Azure Blob Storage.")

    client = BlobServiceClient(
        account_url=f"https://{os.getenv('account_name')}.blob.core.windows.net",
        credential=os.getenv("sas_token"),
    )
    logging.info(
        "Cliente do Azure Blob Storage configurado; "
        "o acesso será validado na primeira operação."
    )
    return client


def get_blob_container_name():
    load_dotenv()
    return os.getenv("container_name")


def build_blob_path(file_name: str):
    load_dotenv()
    directory = os.getenv("directory_name", "").strip("/")
    return f"{directory}/{file_name}" if directory else file_name
