import os
import time
import pandas as pd
from datetime import date, timedelta, datetime
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest
from sqlalchemy import create_engine

# --- CONFIGURAÇÕES ---
# 1. Defina o período que você quer buscar
DATA_INICIO = date(2024, 1, 1)  # Ano, Mês, Dia (Ex: 1º de Jan de 2024)
DATA_FIM = date(2024, 12, 31)   # Até quando?

# 2. Configurações de Acesso (Igual ao outro script)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
KEY_PATH = os.path.join(BASE_DIR, 'config', 'ga4_keys.json')
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = KEY_PATH
PROPERTY_ID = '400960026'


# --- FUNÇÃO DE EXTRAÇÃO (Adaptada para 1 dia específico) ---
def buscar_dia_especifico(client, data_alvo):
    str_data = data_alvo.strftime('%Y-%m-%d') # Transforma em texto '2024-01-01'
    
    request = RunReportRequest(
        property=f"properties/{PROPERTY_ID}",
        date_ranges=[DateRange(start_date=str_data, end_date=str_data)],
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

    try:
        response = client.run_report(request)
        return response
    except Exception as e:
        print(f"❌ Erro ao baixar {str_data}: {e}")
        return None

# --- FUNÇÃO DE SALVAMENTO ---
def salvar_no_banco(response):
    if not response or not response.rows:
        return False # Não tinha dados nesse dia

    data = []
    for row in response.rows:
        item = {}
        # MANTENHA OS NOMES IGUAIS AO OUTRO SCRIPT PARA NÃO DUPLICAR COLUNAS ERRADAS
        item['Data_Log'] = row.dimension_values[0].value
        item['Origem'] = row.dimension_values[1].value
        item['Dispositivo'] = row.dimension_values[2].value
        item['Sessoes'] = int(row.metric_values[0].value)
        item['Usuarios'] = int(row.metric_values[1].value)
        item['Receita'] = float(row.metric_values[2].value)
        data.append(item)

    df = pd.DataFrame(data)
    
    # Tratamento de Data (Igual corrigimos antes)
    df['Data_Log'] = pd.to_datetime(df['Data_Log'], format='%Y%m%d')

    # Conexão SQL
    SERVER = 'Renan_RFS_DEV\\SQLEXPRESS'

    DATABASE = 'PECADIRETA_BI'
    conn_str = f'mssql+pyodbc://@{SERVER}/{DATABASE}?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes'
    engine = create_engine(conn_str)

    # O PULO DO GATO: if_exists='append'
    # Isso diz: "Adicione ao final da tabela, NÃO apague o que já tem".
    df.to_sql('stg_GA4_Trafego', con=engine, if_exists='append', index=False)
    
    return len(df) # Retorna quantas linhas salvou

# --- LOOP PRINCIPAL (A MÁQUINA DO TEMPO) ---
if __name__ == '__main__':
    client = BetaAnalyticsDataClient()
    
    # Calcula quantos dias tem no total
    diferenca = DATA_FIM - DATA_INICIO
    total_dias = diferenca.days + 1
    
    print(f"🚀 Iniciando Carga Histórica de {total_dias} dias...")
    print(f"📅 De {DATA_INICIO} até {DATA_FIM}")

    for i in range(total_dias):
        # Calcula qual é o dia da vez
        dia_atual = DATA_INICIO + timedelta(days=i)
        
        print(f"⏳ Processando: {dia_atual} ... ", end="")
        
        # 1. Busca
        resposta = buscar_dia_especifico(client, dia_atual)
        
        # 2. Salva
        if resposta:
            linhas = salvar_no_banco(resposta)
            if linhas:
                print(f"✅ Sucesso! (+{linhas} linhas)")
            else:
                print("⚠️ Dia vazio (sem tráfego).")
        
        # 3. Descanso (Para o Google não bloquear a gente)
        time.sleep(1.5) 

    print("\n🏁 Carga Histórica Finalizada com Sucesso!")