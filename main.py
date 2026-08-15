import logging
import time

from dotenv import load_dotenv

from functions import (
    buscar_usuarios_adb2c_no_microsoft_graph,
    enviar_parquet_adb2c_para_blob_storage,
    salvar_usuarios_adb2c_em_parquet,
    transformar_e_carregar_usuarios_adb2c_na_sat_pessoa_adb2c,
)
from utils import execution_time, init_logging

load_dotenv()


def main():
    init_logging()
    start_time = time.time()
    error = "N"

    try:
        logging.info("Buscando usuários no ADB2C.")
        users_df = buscar_usuarios_adb2c_no_microsoft_graph()

        logging.info("Gerando arquivo Parquet.")
        parquet_path = salvar_usuarios_adb2c_em_parquet(users_df)

        logging.info("Enviando arquivo ao Blob Storage.")
        enviar_parquet_adb2c_para_blob_storage(parquet_path)

        logging.info("Tratando os dados do Parquet e carregando a SAT_PESSOA_ADB2C.")
        transformar_e_carregar_usuarios_adb2c_na_sat_pessoa_adb2c()

        logging.info("Pipeline ADB2C executado com sucesso.")

    except Exception as exception:
        error = "S"
        logging.info("Pipeline ADB2C finalizado com erro: %s", exception)

    finally:
        duration = execution_time(start_time)
        logging.info("Fim do processo. Tempo de execução: %s minutos.", duration)

    return error


if __name__ == "__main__":
    raise SystemExit(0 if main() == "N" else 1)
