"""Conexão com o Microsoft Graph."""

import logging
import os

import msal
from dotenv import load_dotenv


def connect_microsoft_graph():
    load_dotenv()
    logging.info("Conectando ao Microsoft Graph.")

    app = msal.ConfidentialClientApplication(
        client_id=os.getenv("MICROSOFTGRAPH_CLIENTID"),
        authority=os.getenv("MICROSOFTGRAPH_AUTHO"),
        client_credential=os.getenv("MICROSOFTGRAPH_SECRET"),
    )
    result = app.acquire_token_for_client(
        scopes=[os.getenv("SCOPE", "https://graph.microsoft.com/.default")]
    )

    if "access_token" not in result:
        raise RuntimeError(result.get("error_description", "Falha na autenticação Microsoft."))

    logging.info("Autenticação no Microsoft Graph concluída com sucesso.")
    return {
        "access_token": result["access_token"],
        "graph_url": os.getenv("MICROSOFTGRAPH_URL"),
        "ext_appid": os.getenv("EXT_APPID"),
    }
