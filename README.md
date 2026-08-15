# Pipeline ADB2C

Pipeline de ingestão e refinamento dos cadastros de pessoa do Azure AD B2C.

## Ambiente com uv

O projeto usa `uv`, e o `pyproject.toml` é a fonte oficial das dependências.
O arquivo `uv.lock` deve ser versionado após executar `uv lock` em um ambiente
com acesso ao índice de pacotes.

```powershell
uv sync
```

Para executar os processos:

```powershell
uv run main.py
```

Para validar o código:

```powershell
uv run ruff check .
uv run python -m compileall -q main.py functions.py tratamentos.py operacoes_snowflake.py utils.py connections contracts scripts
```

Para testar Microsoft Graph, Blob e Snowflake sem gravar dados:

```powershell
uv run scripts/test_connections.py
```

## Ordem de execução

1. `buscar_usuarios_adb2c_no_microsoft_graph()`,
   `salvar_usuarios_adb2c_em_parquet()` e
   `enviar_parquet_adb2c_para_blob_storage()` em `functions.py`
   - autentica no Microsoft Graph;
   - pagina os usuários do ADB2C;
   - salva `data/Adb2c_YYYY-MM-DD.parquet`;
   - envia o arquivo bruto ao Azure Blob Storage.
2. `transformar_e_carregar_usuarios_adb2c_na_sat_pessoa_adb2c()` em `functions.py`
   - lê somente o Parquet mais recente da pasta `data`;
   - filtra contas locais e padroniza os atributos;
   - consulta país, estado e cidade no Snowflake para normalização;
   - valida o contrato com Pandera;
   - gera `BK_PESSOA` e `CONTENT_HASH`;
   - aplica o histórico SCD2 em
     `DW_PRD_FLAMENGO.DV_MKT.SAT_PESSOA_ADB2C`.

## Organização do código

- `functions.py`: Microsoft Graph, Parquet, Blob Storage e orquestração da carga.
- `tratamentos.py`: limpeza dos dados, contrato, `BK_PESSOA` e `CONTENT_HASH`.
- `operacoes_snowflake.py`: consultas auxiliares e aplicação do histórico SCD2.

## Conectores

As integrações externas ficam isoladas em `connections`:

- `microsoft.py`: `connect_microsoft_graph()` autentica no Microsoft Graph;
- `blob.py`: `connect_azure_blob_storage()` cria o cliente do Blob Storage;
- `snowflake.py`: `connect_snowflake()` cria a engine autenticada por key pair.

O arquivo `utils.py` contém somente funções compartilhadas de logging e tempo.

## Campos técnicos

- `LOAD_DATE`: instante UTC da execução.
- `RECORD_SOURCE`: valor fixo `ADB2C`.
- `BK_PESSOA`: reutiliza o BK existente na `DW_PRD_FLAMENGO.DV_MKT.HUB_PESSOA` para o mesmo
  `CPF + RECORD_SOURCE`. Para CPF novo, gera o MD5 de
  `CPF + RECORD_SOURCE + LOAD_DATE`.
- Registros sem CPF são descartados antes da geração dos hashes e não são
  inseridos na SAT.
- `CONTENT_HASH`: MD5 da concatenação ordenada dos atributos definidos em
  `COLUNAS_DESCRITIVAS_PESSOA`. Não inclui `CPF`, `LOAD_DATE`, `RECORD_SOURCE` nem os hashes.
  Valores nulos são tratados como string vazia e datas usam ISO 8601.

O `CONTENT_HASH` controla o histórico SCD Tipo 2. Hash igual não gera nova linha.
Quando o hash muda, a versão ativa anterior recebe a data de encerramento e uma
nova versão é inserida com `LOAD_END_DATE` nulo.

## Autenticação Snowflake

A conexão usa key pair por meio de `connections.snowflake.connect_snowflake`. No
arquivo `.env`, preencha `DB_USER`, `DB_HOST`, `DB_WAREHOUSE`,
`DB_PRIVATE_KEY_PASSPHRASE_FLA` e `DB_PRIVATE_KEY_CONTENT_FLA`. No conteúdo da
chave, as quebras de linha podem ser armazenadas como `\\n`.

## Custo e sessões

As dimensões e somente os BKs dos CPFs da carga são consultados em conexões
curtas. A limpeza e os hashes são processados localmente. O fechamento das versões
alteradas e a inserção em lote usam uma única transação. Não existe `TRUNCATE`,
e a engine é descartada ao final, inclusive em caso de erro.

## Periodicidade

O código gera um arquivo diário. A periodicidade do orquestrador não está definida
neste repositório e deve ser confirmada no agendador utilizado pelo squad.
