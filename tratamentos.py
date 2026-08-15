import hashlib
import json
import logging
import multiprocessing
from datetime import datetime
from functools import partial

import pandas as pd
from rapidfuzz import process
from rapidfuzz import utils as rapidfuzz_utils

from contracts.user_contract import UserSchema

RECORD_SOURCE = "ADB2C"

COLUNAS_DESCRITIVAS_PESSOA = [
    "FLA_ID", "NOME", "EMAIL", "DT_CRIACAO", "CREATION_TYPE",
    "DT_ULTIMO_LOGIN", "TELEFONE", "DT_NASCIMENTO", "GENERO", "PAIS",
    "UF", "CIDADE", "BAIRRO", "LOGRADOURO", "COMPLEMENTO", "NUMERO",
]


def extrair_valor_do_json(valor_json, chave):
    try:
        valor_convertido = json.loads(valor_json) if isinstance(valor_json, str) else valor_json
        if isinstance(valor_convertido, list):
            return next(
                (item.get("issuerAssignedId") for item in valor_convertido
                 if item.get("signInType") == "emailAddress"),
                None,
            )
        return valor_convertido.get(chave) if isinstance(valor_convertido, dict) else None
    except (json.JSONDecodeError, TypeError):
        return None


def encontrar_texto_mais_proximo(valor, textos_validos):
    if pd.isna(valor) or not str(valor).strip():
        return valor
    resultado = process.extractOne(
        str(valor),
        textos_validos,
        score_cutoff=80,
        processor=rapidfuzz_utils.default_process,
    )
    return resultado[0] if resultado else valor


def encontrar_textos_mais_proximos_em_paralelo(valores, textos_validos):
    with multiprocessing.Pool() as pool:
        return pool.map(
            partial(encontrar_texto_mais_proximo, textos_validos=list(textos_validos)),
            valores,
        )


def normalizar_valor_para_hash(valor):
    if pd.isna(valor):
        return ""
    if isinstance(valor, (pd.Timestamp, datetime)):
        return valor.isoformat()
    return str(valor).strip()


def gerar_md5_das_colunas(dataframe, colunas):
    valores_concatenados = dataframe[list(colunas)].apply(
        lambda linha: "".join(normalizar_valor_para_hash(valor) for valor in linha),
        axis=1,
    )
    return valores_concatenados.map(
        lambda valor: hashlib.md5(valor.encode("utf-8")).hexdigest()
    )


def normalizar_cpfs(cpfs):
    return cpfs.astype("string").str.replace(r"\D", "", regex=True).str.zfill(11)


def adicionar_hashes_da_pessoa(usuarios, bks_existentes):
    usuarios = usuarios.copy()
    usuarios["LOAD_DATE"] = pd.Timestamp.now(tz="UTC").tz_localize(None)
    usuarios["RECORD_SOURCE"] = RECORD_SOURCE
    bk_por_cpf = (
        bks_existentes.drop_duplicates("CPF").set_index("CPF")["BK_PESSOA"]
        if not bks_existentes.empty
        else pd.Series(dtype="string")
    )
    usuarios["BK_PESSOA"] = usuarios["CPF"].map(bk_por_cpf)
    possui_cpf = usuarios["CPF"].notna()
    precisa_gerar_bk = possui_cpf & usuarios["BK_PESSOA"].isna()
    logging.info(
        "Reutilizando %s BKs existentes, gerando %s novos e mantendo %s sem BK por falta de CPF.",
        usuarios["BK_PESSOA"].notna().sum(),
        precisa_gerar_bk.sum(),
        (~possui_cpf).sum(),
    )
    usuarios.loc[precisa_gerar_bk, "BK_PESSOA"] = gerar_md5_das_colunas(
        usuarios.loc[precisa_gerar_bk],
        ["CPF", "RECORD_SOURCE", "LOAD_DATE"],
    )
    logging.info(
        "Gerando CONTENT_HASH com %s atributos de negócio.",
        len(COLUNAS_DESCRITIVAS_PESSOA),
    )
    usuarios["CONTENT_HASH"] = gerar_md5_das_colunas(
        usuarios,
        COLUNAS_DESCRITIVAS_PESSOA,
    )
    return usuarios


def transformar_usuarios_adb2c(usuarios, dimensoes, bks_existentes):
    quantidade_inicial = len(usuarios)
    logging.info("Iniciando tratamento de %s registros.", quantidade_inicial)
    usuarios = usuarios.copy()
    usuarios.columns = usuarios.columns.str.upper()
    usuarios = usuarios.loc[usuarios["CREATIONTYPE"] == "LocalAccount"].copy()
    logging.info("Contas LocalAccount: %s de %s.", len(usuarios), quantidade_inicial)
    usuarios.drop(columns=["MAIL", "MOBILEPHONE"], errors="ignore", inplace=True)
    colunas_de_texto = [
        "DISPLAYNAME", "STREETADDRESS", "CITY", "COUNTRY", "STATE",
        "GENERO", "BAIRRO", "COMPLEMENTO",
    ]
    for coluna in colunas_de_texto:
        if coluna in usuarios:
            usuarios[coluna] = (
                usuarios[coluna]
                .astype("string")
                .str.replace(r"\s{2,}", " ", regex=True)
                .str.strip()
            )
    usuarios["CPF"] = normalizar_cpfs(usuarios["CPF"])
    usuarios = usuarios.loc[usuarios["CPF"].notna()].copy()
    usuarios["IDENTITIES"] = usuarios["IDENTITIES"].map(
        lambda valor: extrair_valor_do_json(valor, "issuerAssignedId")
    )
    usuarios["SIGNINACTIVITY"] = usuarios["SIGNINACTIVITY"].map(
        lambda valor: extrair_valor_do_json(valor, "lastSignInDateTime")
    )
    for coluna in ["DISPLAYNAME", "STREETADDRESS", "CITY", "COUNTRY", "GENERO", "BAIRRO"]:
        usuarios[coluna] = usuarios[coluna].str.title()
    usuarios["STATE"] = usuarios["STATE"].str.upper()
    usuarios["IDENTITIES"] = usuarios["IDENTITIES"].astype("string").str.lower()
    paises, estados, cidades = dimensoes
    usuarios["COUNTRY"] = encontrar_textos_mais_proximos_em_paralelo(
        usuarios["COUNTRY"], paises["NOME_PT"].dropna().unique()
    )
    usuarios["STATE"] = encontrar_textos_mais_proximos_em_paralelo(
        usuarios["STATE"], estados["UF"].dropna().unique()
    )
    usuarios["CITY"] = encontrar_textos_mais_proximos_em_paralelo(
        usuarios["CITY"], cidades["NOME"].dropna().unique()
    )
    generos_aceitos = {"Masculino", "Feminino", "Prefiro Não Declarar", "Outro"}
    usuarios["GENERO"] = usuarios["GENERO"].where(
        usuarios["GENERO"].isin(generos_aceitos),
        "Não Informado",
    )
    usuarios["DATANASCIMENTO"] = pd.to_datetime(
        usuarios["DATANASCIMENTO"],
        dayfirst=True,
        errors="coerce",
    )
    usuarios.loc[
        usuarios["DATANASCIMENTO"] > pd.Timestamp.now(),
        "DATANASCIMENTO",
    ] = pd.NaT
    usuarios["DATANASCIMENTO"] = usuarios["DATANASCIMENTO"].dt.date
    usuarios["NUMERO"] = (
        usuarios["NUMERO"].astype("string").str.replace(r"\.0$", "", regex=True)
    )
    usuarios.rename(columns={
        "DISPLAYNAME": "NOME",
        "IDENTITIES": "EMAIL",
        "CREATEDDATETIME": "DT_CRIACAO",
        "ID": "FLA_ID",
        "CREATIONTYPE": "CREATION_TYPE",
        "STREETADDRESS": "LOGRADOURO",
        "CITY": "CIDADE",
        "COUNTRY": "PAIS",
        "STATE": "UF",
        "DATANASCIMENTO": "DT_NASCIMENTO",
        "CELULAR": "TELEFONE",
        "SIGNINACTIVITY": "DT_ULTIMO_LOGIN",
    }, inplace=True)
    for coluna in COLUNAS_DESCRITIVAS_PESSOA:
        if coluna not in usuarios:
            usuarios[coluna] = None
    logging.info("Validando contrato dos dados refinados.")
    usuarios = UserSchema.schema.validate(usuarios, lazy=True)
    usuarios = adicionar_hashes_da_pessoa(usuarios, bks_existentes)
    usuarios["LOAD_END_DATE"] = pd.NaT
    return usuarios[[
        "BK_PESSOA",
        "LOAD_DATE",
        "RECORD_SOURCE",
        "LOAD_END_DATE",
        "CONTENT_HASH",
        "CPF",
        *COLUNAS_DESCRITIVAS_PESSOA,
    ]]
