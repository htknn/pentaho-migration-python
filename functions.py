import json
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
from azure.core.exceptions import AzureError
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_fixed

from connections.blob import (
    build_blob_path,
    connect_azure_blob_storage,
    get_blob_container_name,
)
from connections.microsoft import connect_microsoft_graph
from connections.snowflake import connect_snowflake
from operacoes_snowflake import (
    aplicar_historico_scd2_na_sat_pessoa_adb2c,
    buscar_bks_pessoa_existentes,
    buscar_dimensoes_de_referencia_de_localizacao,
)
from tratamentos import normalizar_cpfs, transformar_usuarios_adb2c

LIMITE_PAGINAS_MICROSOFT_GRAPH = 0


@retry(
    stop=stop_after_attempt(10),
    wait=wait_fixed(60),
    retry=retry_if_exception_type(requests.exceptions.RequestException),
)
def buscar_paginas_de_usuarios_no_microsoft_graph(
    access_token, ext_appid, graph_url, limite_paginas
):
    campos = [
        "displayName", "mail", "createdDateTime", "mobilePhone", "id",
        "creationType", f"extension_{ext_appid}_CPF",
        f"extension_{ext_appid}_DataNascimento", f"extension_{ext_appid}_Celular",
        f"extension_{ext_appid}_Genero", "streetAddress", "city", "country", "state",
        f"extension_{ext_appid}_Numero", f"extension_{ext_appid}_Bairro",
        f"extension_{ext_appid}_Complemento", "identities", "signInActivity",
    ]
    parametros = {
        "$top": 999,
        "$select": ",".join(campos),
        "$filter": "signInActivity/lastSignInDateTime ge 2026-02-26T00:00:00Z",
    }
    cabecalhos = {"Authorization": f"Bearer {access_token}"}
    with requests.Session() as sessao:
        proxima_url = graph_url
        proximos_parametros = parametros
        numero_da_pagina = 0
        while proxima_url:
            numero_da_pagina += 1
            logging.info(
                "Consultando página %s do Microsoft Graph.",
                numero_da_pagina,
            )
            resposta = sessao.get(
                proxima_url,
                headers=cabecalhos,
                params=proximos_parametros,
                timeout=180,
            )
            resposta.raise_for_status()
            pagina = resposta.json()
            logging.info(
                "Página %s recebida com %s registros.",
                numero_da_pagina,
                len(pagina.get("value", [])),
            )
            yield pagina
            if limite_paginas is not None and numero_da_pagina >= limite_paginas:
                logging.info(
                    "Limite de %s página(s) atingido; consulta de teste encerrada.",
                    limite_paginas,
                )
                break
            proxima_url = pagina.get("@odata.nextLink")
            proximos_parametros = None


def buscar_usuarios_adb2c_no_microsoft_graph():
    microsoft = connect_microsoft_graph()
    limite_configurado = LIMITE_PAGINAS_MICROSOFT_GRAPH
    limite_paginas = limite_configurado if limite_configurado > 0 else None
    if limite_paginas is not None:
        logging.info(
            "Modo de teste ativo: consulta limitada a %s página(s) do Microsoft Graph.",
            limite_paginas,
        )
    usuarios = [
        usuario
        for pagina in buscar_paginas_de_usuarios_no_microsoft_graph(
            microsoft["access_token"],
            microsoft["ext_appid"],
            microsoft["graph_url"],
            limite_paginas,)
        for usuario in pagina.get("value", [])
    ]
    logging.info(
        "Consulta ao Microsoft Graph concluída: %s usuários encontrados.",
        len(usuarios),
    )
    dataframe = pd.DataFrame(usuarios)
    prefixo = f"extension_{microsoft['ext_appid']}_"
    dataframe.rename(columns={
        coluna: coluna.removeprefix(prefixo)
        for coluna in dataframe.columns
        if coluna.startswith(prefixo)
    }, inplace=True)
    for coluna in ("identities", "signInActivity"):
        if coluna in dataframe:
            dataframe[coluna] = dataframe[coluna].map(json.dumps)
    return dataframe


def salvar_usuarios_adb2c_em_parquet(dataframe):
    caminho_do_arquivo = Path.cwd() / "data" / f"Adb2c_{datetime.now().date()}.parquet"
    logging.info(
        "Salvando %s usuários no arquivo Parquet %s.",
        len(dataframe),
        caminho_do_arquivo,
    )
    caminho_do_arquivo.parent.mkdir(parents=True, exist_ok=True)
    dataframe.to_parquet(caminho_do_arquivo, compression="gzip", index=False)
    logging.info("Arquivo Parquet salvo com sucesso em %s.", caminho_do_arquivo)
    return caminho_do_arquivo


def enviar_parquet_adb2c_para_blob_storage(caminho_do_arquivo):
    servico_blob = connect_azure_blob_storage()
    cliente_blob = servico_blob.get_blob_client(
        get_blob_container_name(),
        build_blob_path(caminho_do_arquivo.name),)
    enviar_arquivo_parquet_para_blob(caminho_do_arquivo, cliente_blob)


def enviar_arquivo_parquet_para_blob(caminho_do_arquivo, cliente_blob):
    try:
        logging.info("Enviando %s ao Blob Storage.", caminho_do_arquivo.name)
        with caminho_do_arquivo.open("rb") as arquivo:
            cliente_blob.upload_blob(arquivo, overwrite=True)
        logging.info(
            "Arquivo %s enviado com sucesso ao Azure Blob Storage.",
            caminho_do_arquivo.name,
        )
    except (OSError, AzureError):
        logging.info("Falha ao enviar o arquivo ao Blob Storage.")
        raise


def ler_parquet_adb2c_mais_recente():
    pasta_de_dados = Path.cwd() / "data"
    arquivos_parquet = (
        list(pasta_de_dados.glob("*.parquet")) if pasta_de_dados.exists() else []
    )
    if not arquivos_parquet:
        raise FileNotFoundError(
            f"Nenhum arquivo parquet encontrado em {pasta_de_dados}"
        )
    arquivo_mais_recente = max(
        arquivos_parquet,
        key=lambda arquivo: arquivo.stat().st_mtime,
    )
    logging.info("Lendo o Parquet mais recente: %s.", arquivo_mais_recente)
    dataframe = pd.read_parquet(arquivo_mais_recente)
    logging.info("Leitura concluída com %s registros.", len(dataframe))
    return dataframe


def transformar_e_carregar_usuarios_adb2c_na_sat_pessoa_adb2c():
    engine = connect_snowflake("DW_PRD_FLAMENGO")
    try:
        usuarios_brutos = ler_parquet_adb2c_mais_recente()
        usuarios_brutos.columns = usuarios_brutos.columns.str.upper()
        cpfs_recebidos = (
            normalizar_cpfs(usuarios_brutos["CPF"])
            .dropna()
            .drop_duplicates()
            .tolist()
        )
        bks_existentes = buscar_bks_pessoa_existentes(engine, cpfs_recebidos)
        dimensoes = buscar_dimensoes_de_referencia_de_localizacao(engine)
        usuarios_tratados = transformar_usuarios_adb2c(
            usuarios_brutos,
            dimensoes,
            bks_existentes,
        )
        aplicar_historico_scd2_na_sat_pessoa_adb2c(usuarios_tratados, engine)
    finally:
        engine.dispose()
        logging.info("Conexão com o Snowflake encerrada e recursos liberados.")
