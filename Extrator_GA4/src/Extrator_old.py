print(">>> O script começou a ler o arquivo...")

import os 
import pandas as pd
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest
from sqlalchemy import create_engine

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
KEY_PATH = os.path.join(BASE_DIR, 'config', 'ga4_keys.json')

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = KEY_PATH

PROPERTY_ID = '400960026'

def extrair_dados_ga4(property_id, data_inicio, data_fim):
    print(f"📡 Iniciando conexão com GA4 para o período: {data_inicio} a {data_fim}")

    client = BetaAnalyticsDataClient()

    request = RunReportRequest(
        property=f"properties/{property_id}",
        date_ranges=[DateRange(start_date=data_inicio, end_date=data_fim)],

        dimensions=[
            Dimension(name="date"), 
            Dimension(name="sessionSource"),
            Dimension(name="deviceCategory")
        ],
        metrics=[
            Metric(name="sessions"),
            Metric(name="activeUsers"),
            Metric(name="totalRevenue")
        ]
    )

    response = client.run_report(request)

    print("✅ Dados recebidos do Google. Iniciando Processamento...")
    return response

def parse_e_salvar(response):

    data = []

    for row in response.rows:
        item={}

        item['Data_Log'] = row.dimension_values[0].value
        item['Origem'] = row.dimension_values[1].value
        item['Dispositivo'] = row.dimension_values[2].value

        item['Sessoes'] = int(row.metric_values[0].value)
        item['Usuarios'] = int(row.metric_values[1].value)
        item['Receita'] = float(row.metric_values[2].value)

        data.append(item)

        df = pd.DataFrame(data)

        df['Data_Log'] = pd.to_datetime(df['Data_Log'], format = '%Y%m%d')

        print(f"📊 Tabela gerada com {len(df)} linhas.")

        SERVER = 'Renan_RFS_DEV\\SQLEXPRESS'

        DATABASE = 'PECADIRETA_BI'

        conn_str = f'mssql+pyodbc://@{SERVER}/{DATABASE}?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes'

        engine = create_engine(conn_str)

        df.to_sql('stg_GA4_Trafego', con=engine, if_exists='append', index='false')

        print("🚀 Sucesso! Dados inseridos no SQL Server.")

if __name__ == '__main__':
    resposta_google = extrair_dados_ga4(PROPERTY_ID, 'yesterday', 'yesterday')
    parse_e_salvar(resposta_google)