"""Conexão com o Snowflake usando key pair."""

import logging
import os
import time
from datetime import datetime as SystemDateTime
from datetime import timedelta, timezone
from email.utils import parsedate_to_datetime

import requests
import snowflake.connector.auth.keypair as snowflake_keypair_auth
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from dotenv import load_dotenv
from snowflake.connector.errors import DatabaseError as SnowflakeDatabaseError
from sqlalchemy import create_engine, text
from sqlalchemy.exc import DBAPIError, OperationalError


class SnowflakeServerDateTime(SystemDateTime):
    """Fornece ao JWT o horário retornado pelo servidor Snowflake."""

    time_offset = timedelta(0)

    @classmethod
    def now(cls, tz=None):
        current_timezone = tz if tz is not None else timezone.utc
        adjusted_datetime = SystemDateTime.now(current_timezone) + cls.time_offset

        if tz is None:
            return adjusted_datetime.replace(tzinfo=None)

        return adjusted_datetime


def synchronize_jwt_time_with_snowflake(host: str):
    """Calcula a diferença do relógio local e a aplica somente ao JWT."""
    request_started_at = SystemDateTime.now(timezone.utc)
    response = requests.head(
        f"https://{host}.snowflakecomputing.com",
        timeout=15,
        allow_redirects=False,
    )
    request_finished_at = SystemDateTime.now(timezone.utc)

    server_date_header = response.headers.get("Date")
    if not server_date_header:
        raise RuntimeError("O Snowflake não retornou o horário do servidor.")

    server_datetime = parsedate_to_datetime(server_date_header)
    request_midpoint = request_started_at + (
        request_finished_at - request_started_at
    ) / 2
    SnowflakeServerDateTime.time_offset = server_datetime - request_midpoint

    # O conector usa este objeto somente para preencher iat e exp do JWT.
    snowflake_keypair_auth.datetime = SnowflakeServerDateTime
    logging.info(
        "Horário do JWT alinhado ao servidor Snowflake; ajuste aplicado: %.2f segundos.",
        SnowflakeServerDateTime.time_offset.total_seconds(),
    )


def gen_private_key_flamengo():
    passphrase = os.getenv("DB_PRIVATE_KEY_PASSPHRASE_FLA")
    key_content = os.getenv("DB_PRIVATE_KEY_CONTENT_FLA")
    private_key_decoded = key_content.replace("\\n", "\n")

    private_key = serialization.load_pem_private_key(
        private_key_decoded.encode(),
        password=passphrase.encode(),
        backend=default_backend(),
    )
    private_key_der = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )

    return private_key_der


def connect_snowflake(database="STG_PRD_FLAMENGO", retries=3, delay=5):
    load_dotenv()

    db_url = (
        f"snowflake://{os.getenv('DB_USER')}"
        f"@{os.getenv('DB_HOST')}/"
        f"{database}"
        f"?warehouse={os.getenv('DB_WAREHOUSE')}"
    )
    synchronize_jwt_time_with_snowflake(os.getenv("DB_HOST"))

    for attempt in range(1, retries + 1):
        engine = None

        try:
            logging.info(
                "Tentativa %s de %s para conectar ao Snowflake com um novo JWT.",
                attempt,
                retries,
            )
            engine = create_engine(
                db_url,
                connect_args={"private_key": gen_private_key_flamengo()},
                pool_pre_ping=True,
            )

            with engine.connect() as connection:
                connection.execute(text("SELECT 1"))

            logging.info("Conexão com o Snowflake validada com sucesso.")
            return engine

        except (OperationalError, DBAPIError, SnowflakeDatabaseError) as exception:
            if engine is not None:
                engine.dispose()

            logging.info(
                "Não foi possível conectar ao Snowflake na tentativa %s de %s: %s",
                attempt,
                retries,
                exception,
            )

            if attempt == retries:
                raise

            logging.info(
                "Aguardando %s segundos antes de gerar um novo JWT e tentar novamente.",
                delay,
            )
            time.sleep(delay)

    raise RuntimeError("Não foi possível iniciar a conexão com o Snowflake.")
