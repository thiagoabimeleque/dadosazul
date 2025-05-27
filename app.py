import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import datetime

# Carrega as credenciais diretamente do dicionário
credentials = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"])

# Inicializa cliente BigQuery com credenciais
client = bigquery.Client(credentials=credentials, project=credentials.project_id)

# Inicializa o cliente BigQuery
# client = bigquery.Client()

st.title("Consulta de Dados - dbdadosazul")

# Campos de filtro
cpf = st.text_input("CPF")
nome = st.text_input("Nome")
mae = st.text_input("Mãe")
pai = st.text_input("Pai")
municipio = st.text_input("Municipio Nascimento")
sexo = st.selectbox("Sexo", ["", "M", "F"])
data_nascimento = st.date_input(
    "Data de Nascimento",
    value=None,
    min_value=datetime.date(1900, 1, 1),
    max_value=datetime.date.today()
)

# Monta a query dinamicamente
base_query = "SELECT AZUL.cpf, nome, mae, pai, nasc, sexo, municipioNasci FROM `dadosazul.dadosazul.dbdadosazul` as AZUL FULL OUTER JOIN `dadosazul.dadosazul.dbsus` as SUS ON AZUL.cpf = SUS.cpf WHERE 1=1"
params = {}

if cpf:
    base_query += " AND AZUL.cpf = @cpf"
    params["cpf"] = cpf

if nome:
    base_query += " AND AZUL.nome LIKE @nome"
    nome = str('%'.join(nome.split())).upper()
    params["nome"] = f"%{nome}%"

if mae:
    base_query += " AND SUS.mae LIKE @mae"
    mae = str('%'.join(mae.split())).upper()
    params["mae"] = f"%{mae}%"

if pai:
    base_query += " AND SUS.pai LIKE @nome"
    pai = str('%'.join(pai.split())).upper()
    params["pai"] = f"%{pai}%"

if data_nascimento:
    base_query += " AND AZUL.nasc = @data_nascimento"
    params["data_nascimento"] = str(data_nascimento)

if municipio:
    base_query += " AND SUS.municipioNasci LIKE @municipio"
    municipio = str('%'.join(municipio.split())).upper()
    params["municipio"] = f"%{municipio}%"

if sexo:
    base_query += " AND AZUL.sexo = @sexo"
    params["sexo"] = sexo

# Executa a consulta
if st.button("Consultar"):
    try:
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(key, "STRING" if isinstance(value, str) else "DATE", value)
                for key, value in params.items()
            ]
        )
        query_job = client.query(base_query, job_config=job_config)
        df = query_job.to_dataframe()

        st.success(f"{len(df)} registros encontrados.")
        st.dataframe(df)

    except Exception as e:
        st.error(f"Erro na consulta: {e}")
