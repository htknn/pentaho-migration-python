import logging
import time
import warnings
from datetime import datetime

import pandas as pd
from sqlalchemy.exc import SAWarning

from connections.snowflake import connect_snowflake


def init_logging():
    logging.basicConfig(
        filename="app.log",
        filemode="w",
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        encoding="utf-8",
    )
    logging.getLogger("snowflake.connector").setLevel(logging.WARNING)
    logging.getLogger("azure").setLevel(logging.WARNING)
    warnings.filterwarnings(
        "ignore",
        category=SAWarning,
        message=(
            "The GenericFunction 'flatten' is already registered and is going "
            "to be overridden."),
    )
    logging.info("### START ###")


def execution_time(start_time: float):
    elapsed = time.time() - start_time
    minutes = int(elapsed // 60)
    seconds = int(elapsed % 60)
    return round(minutes + (seconds / 100), 2)


def save_logs(
    programa: str,
    tabela: str,
    duracao: float,
    error: str,
    categoria: str,
    grupo: str,
    finalizado: bool,
    periodicidade: str,
):
    with open("app.log", encoding="utf-8") as log_file:
        logs = log_file.read()

    if finalizado and error == "S":
        finalizado = False

    dataframe = pd.DataFrame([{
        "processo": f"{programa.upper()}.{tabela.upper()}",
        "duracao": duracao,
        "alerta": error == "S",
        "categoria": categoria.upper(),
        "dthr_processo": datetime.now(),
        "logs": logs,
        "grupo": grupo.upper(),
        "dt_particao": datetime.now(),
        "finalizado": finalizado,
        "origem": "JOB_LAKE",
        "periodicidade": periodicidade.upper(),
    }])

    engine = connect_snowflake()
    try:
        with engine.begin() as connection:
            dataframe.to_sql(
                "logs_processos",
                connection,
                schema="stg_mkt",
                if_exists="append",
                index=False,
                method="multi",
                chunksize=1,)
            
            logging.info("Log de execução salvo no banco de dados.")
    finally:
        engine.dispose()
