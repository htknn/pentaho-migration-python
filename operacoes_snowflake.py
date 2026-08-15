import logging

import pandas as pd
from sqlalchemy import bindparam, text

from tratamentos import RECORD_SOURCE, normalizar_cpfs

BANCO_DESTINO = "DW_PRD_FLAMENGO"
SCHEMA_DESTINO = "DV_MKT"
TABELA_SAT_PESSOA_ADB2C = "SAT_PESSOA_ADB2C"
TABELA_HUB_PESSOA = "HUB_PESSOA"


def buscar_bks_pessoa_existentes(engine, cpfs):
    cpfs_unicos = list(dict.fromkeys(cpf for cpf in cpfs if pd.notna(cpf) and cpf))
    if not cpfs_unicos:
        return pd.DataFrame(columns=["CPF", "BK_PESSOA"])
    logging.info("Consultando BK_PESSOA de %s CPFs na %s.%s.%s.",
        len(cpfs_unicos),
        BANCO_DESTINO,
        SCHEMA_DESTINO,
        TABELA_HUB_PESSOA,)
    consulta = text(
        f"""
        SELECT CPF, BK_PESSOA
        FROM {BANCO_DESTINO}.{SCHEMA_DESTINO}.{TABELA_HUB_PESSOA}
        WHERE RECORD_SOURCE = :record_source
          AND CPF IN :cpfs
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY CPF
            ORDER BY LOAD_DATE, BK_PESSOA
        ) = 1
        """
    ).bindparams(bindparam("cpfs", expanding=True))
    resultados = []
    with engine.connect() as conexao:
        for inicio in range(0, len(cpfs_unicos), 5_000):
            lote_cpfs = cpfs_unicos[inicio : inicio + 5_000]
            resultados.append(
                pd.read_sql(
                    consulta,
                    conexao,
                    params={"record_source": RECORD_SOURCE, "cpfs": lote_cpfs},
                )
            )
    bks_existentes = pd.concat(resultados, ignore_index=True)
    bks_existentes.columns = bks_existentes.columns.str.upper()
    bks_existentes["CPF"] = normalizar_cpfs(bks_existentes["CPF"])
    logging.info("Encontrados %s BKs existentes na HUB_PESSOA.", len(bks_existentes))
    return bks_existentes


def buscar_dimensoes_de_referencia_de_localizacao(engine):
    logging.info("Carregando dimensões de país, estado e cidade.")
    with engine.connect() as conexao:
        paises = pd.read_sql(
            "SELECT NOME_PT FROM DW_PRD_FLAMENGO.DM_MKT.DIM_PAIS",
            conexao,
        )
        estados = pd.read_sql(
            "SELECT UF FROM DW_PRD_FLAMENGO.DM_MKT.DIM_ESTADO",
            conexao,
        )
        cidades = pd.read_sql(
            "SELECT NOME FROM DW_PRD_FLAMENGO.DM_MKT.DIM_CIDADE",
            conexao,
        )
    for dimensao in (paises, estados, cidades):
        dimensao.columns = dimensao.columns.str.upper()
    logging.info("Dimensões de país, estado e cidade carregadas com sucesso.")
    return paises, estados, cidades


def buscar_hashes_ativos_da_sat_pessoa_adb2c(engine, bks_pessoa):
    bks_unicos = list(dict.fromkeys(bk for bk in bks_pessoa if pd.notna(bk) and bk))
    if not bks_unicos:
        return pd.DataFrame(columns=["BK_PESSOA", "CONTENT_HASH"])
    consulta = text(
        f"""
        SELECT BK_PESSOA, CONTENT_HASH
        FROM {BANCO_DESTINO}.{SCHEMA_DESTINO}.{TABELA_SAT_PESSOA_ADB2C}
        WHERE RECORD_SOURCE = :record_source
          AND LOAD_END_DATE IS NULL
          AND BK_PESSOA IN :bks_pessoa
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY BK_PESSOA
            ORDER BY LOAD_DATE DESC
        ) = 1
        """
    ).bindparams(bindparam("bks_pessoa", expanding=True))
    resultados = []
    with engine.connect() as conexao:
        for inicio in range(0, len(bks_unicos), 5_000):
            lote_bks = bks_unicos[inicio : inicio + 5_000]
            resultados.append(
                pd.read_sql(
                    consulta,
                    conexao,
                    params={
                        "record_source": RECORD_SOURCE,
                        "bks_pessoa": lote_bks,
                    },
                )
            )
    hashes_ativos = pd.concat(resultados, ignore_index=True)
    hashes_ativos.columns = hashes_ativos.columns.str.upper()
    return hashes_ativos


def aplicar_historico_scd2_na_sat_pessoa_adb2c(usuarios, engine):
    if usuarios.empty:
        logging.info("Nenhum registro recebido para aplicar na SAT_PESSOA_ADB2C.")
        return
    linhas_repetidas = usuarios["BK_PESSOA"].notna() & usuarios.duplicated(
        "BK_PESSOA",
        keep="last",
    )
    quantidade_repetida = linhas_repetidas.sum()
    if quantidade_repetida:
        logging.info(
            "Removendo %s registros repetidos por BK_PESSOA na carga atual.",
            quantidade_repetida,
        )
        usuarios = usuarios.loc[~linhas_repetidas].copy()
    hashes_ativos = buscar_hashes_ativos_da_sat_pessoa_adb2c(
        engine,
        usuarios["BK_PESSOA"].tolist(),
    )
    bks_ativos = set(hashes_ativos["BK_PESSOA"])
    hash_por_bk = hashes_ativos.set_index("BK_PESSOA")["CONTENT_HASH"]
    possui_versao_ativa = usuarios["BK_PESSOA"].isin(bks_ativos)
    hash_armazenado = usuarios["BK_PESSOA"].map(hash_por_bk).fillna("")
    conteudo_alterado = possui_versao_ativa & usuarios["CONTENT_HASH"].fillna("").ne(
        hash_armazenado
    )
    pessoa_nova = ~possui_versao_ativa
    pessoa_sem_alteracao = possui_versao_ativa & ~conteudo_alterado
    linhas_para_inserir = usuarios.loc[pessoa_nova | conteudo_alterado].copy()
    bks_alterados = (
        usuarios.loc[conteudo_alterado, "BK_PESSOA"].drop_duplicates().tolist()
    )
    logging.info(
        "SCD2: %s novos, %s alterados e %s sem alteração.",
        pessoa_nova.sum(),
        conteudo_alterado.sum(),
        pessoa_sem_alteracao.sum(),
    )
    if linhas_para_inserir.empty:
        logging.info("Nenhuma alteração encontrada para gravar na SAT_PESSOA_ADB2C.")
        return
    data_da_carga = usuarios["LOAD_DATE"].max().to_pydatetime()
    fechar_versoes_ativas = text(
        f"""
        UPDATE {BANCO_DESTINO}.{SCHEMA_DESTINO}.{TABELA_SAT_PESSOA_ADB2C}
        SET LOAD_END_DATE = :load_end_date
        WHERE RECORD_SOURCE = :record_source
          AND LOAD_END_DATE IS NULL
          AND BK_PESSOA IN :bks_pessoa
        """
    ).bindparams(bindparam("bks_pessoa", expanding=True))
    with engine.begin() as conexao:
        for inicio in range(0, len(bks_alterados), 5_000):
            lote_bks = bks_alterados[inicio : inicio + 5_000]
            conexao.execute(
                fechar_versoes_ativas,
                {
                    "load_end_date": data_da_carga,
                    "record_source": RECORD_SOURCE,
                    "bks_pessoa": lote_bks,
                },
            )
        logging.info("Inserindo %s novas versões na SAT_PESSOA_ADB2C.", len(linhas_para_inserir))
        linhas_para_inserir.to_sql(
            TABELA_SAT_PESSOA_ADB2C,
            conexao,
            schema=SCHEMA_DESTINO,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=10_000,)
        
    logging.info("Histórico SCD2 aplicado com sucesso em %s.%s.%s.",
        BANCO_DESTINO,
        SCHEMA_DESTINO,
        TABELA_SAT_PESSOA_ADB2C,)
