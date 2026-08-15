"""Contrato dos dados de pessoa após o refinamento do ADB2C."""

from pandera import Check, Column, DataFrameSchema
from pandera.dtypes import Date, DateTime


class UserSchema:
    schema = DataFrameSchema(
        columns={
            "NOME": Column(str, nullable=True, checks=Check.str_length(0, 120)),
            "EMAIL": Column(str, nullable=True, checks=Check.str_length(0, 120)),
            "DT_CRIACAO": Column(DateTime, nullable=False),
            "FLA_ID": Column(str, nullable=False, checks=Check.str_length(1, 60)),
            "CREATION_TYPE": Column(str, nullable=False, required=True),
            "DT_ULTIMO_LOGIN": Column(DateTime, nullable=True, coerce=True),
            "LOGRADOURO": Column(str, nullable=True, required=False),
            "CIDADE": Column(str, nullable=True, required=False),
            "PAIS": Column(str, nullable=True, required=False),
            "UF": Column(str, nullable=True, required=False),
            "GENERO": Column(str, nullable=True, required=False),
            "BAIRRO": Column(str, nullable=True, required=False),
            "COMPLEMENTO": Column(str, nullable=True, required=False),
            "DT_NASCIMENTO": Column(Date, nullable=True, required=False),
            "CPF": Column(str, nullable=True, checks=Check.str_length(11, 11)),
            "NUMERO": Column(str, nullable=True, required=False),
            "TELEFONE": Column(str, nullable=True, required=False),
        },
        add_missing_columns=True,
        coerce=True,
        strict=False,
        name="User ADB2C",
    )
