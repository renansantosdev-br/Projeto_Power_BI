import os
import time
import pandas as pd
from datetime import date, timedelta, datetime
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest
from sqlalchemy import create_engine

# --- 1. CONFIGURAÇÕES GERAIS ---
# Ajuste de datas conforme a necessidade
DATA_INICIO = date(2025, 2, 1)
DATA_FIM = date(2025, 12, 31)


SERVER = 'Renan_RFS_DEV\\SQLEXPRESS'
DATABASE = 'PECADIRETA_BI'
PROPERTY_ID = '400960026'

# Autenticação
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
KEY_PATH = os.path.join(BASE_DIR, 'config', 'ga4_keys.json')
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = KEY_PATH

# --- 2. O MAPA DOS RELATÓRIOS (A INTELIGÊNCIA DO SCRIPT) ---
CONFIG_RELATORIOS = {
    'f_Trafego': {
        # CORREÇÃO AQUI: Mudamos 'sessionCampaign' para 'sessionCampaignName'
        'dimensions': ['date', 'sessionSource', 'sessionMedium', 'sessionCampaignName', 'deviceCategory'],
        'metrics': ['sessions', 'activeUsers', 'newUsers', 'totalRevenue'],
        'colunas_sql': ['Data_Log', 'Origem', 'Midia', 'Campanha', 'Dispositivo', 'Sessoes', 'Usuarios_Ativos', 'Novos_Usuarios', 'Receita']
    },
    'f_Acessos_sessao': {
        'dimensions': ['date', 'pagePath', 'pageTitle'],
        'metrics': ['screenPageViews', 'activeUsers', 'averageSessionDuration'],
        'colunas_sql': ['Data_Log', 'Pagina_Caminho', 'Pagina_Titulo', 'Visualizacoes', 'Usuarios', 'Tempo_Medio_Segundos']
    },
    'f_Acessos_Item': {
        'dimensions': ['date', 'itemId', 'itemName', 'itemBrand', 'itemCategory'],
        'metrics': ['itemsViewed', 'itemsAddedToCart', 'itemsPurchased', 'itemRevenue'],
        'colunas_sql': ['Data_Log', 'Item_ID', 'Item_Nome', 'Item_Marca', 'Item_Categoria', 'Vistos', 'Adicionados_Carrinho', 'Comprados', 'Receita_Item']
    },
    'f_Eventos_Geral': {
        'dimensions': ['date', 'eventName', 'sessionSource'],
        'metrics': ['eventCount', 'totalUsers'],
        'colunas_sql': ['Data_Log', 'Nome_Evento', 'Origem', 'Quantidade_Eventos', 'Total_Usuarios']
    }
}

# --- 3. FUNÇÃO GENÉRICA DE BUSCA ---
def buscar_dados_ga4(client, property_id, data_str, config):
    """
    Faz o pedido ao Google baseado na configuração passada.
    """
    # Monta as listas de objetos Dimension e Metric dinamicamente
    dim_list = [Dimension(name=d) for d in config['dimensions']]
    met_list = [Metric(name=m) for m in config['metrics']]

    request = RunReportRequest(
        property=f"properties/{property_id}",
        date_ranges=[DateRange(start_date=data_str, end_date=data_str)],
        dimensions=dim_list,
        metrics=met_list
    )

    try:
        return client.run_report(request)
    except Exception as e:
        print(f"   ❌ Erro na API: {e}")
        return None

# --- 4. FUNÇÃO DE PROCESSAMENTO E CARGA (CORRIGIDA) ---
def processar_e_salvar(response, nome_tabela, colunas_sql, engine):
    """
    Transforma JSON em DataFrame, converte tipos e salva no SQL.
    """
    if not response or not response.rows:
        return 0

    data = []
    for row in response.rows:
        item = []
        # Adiciona os valores das Dimensões
        for dim in row.dimension_values:
            item.append(dim.value)
        
        # Adiciona os valores das Métricas (BLINDAGEM CONTRA NOTAÇÃO CIENTÍFICA)
        for met in row.metric_values:
            try:
                # 1. Tenta converter tudo para Float primeiro (aceita '10.5', '100', '7e-06')
                val_float = float(met.value)
                
                # 2. Verifica se é um número inteiro "disfarçado" (ex: 5.0)
                if val_float.is_integer():
                    item.append(int(val_float))
                else:
                    item.append(val_float)
            except ValueError:
                # Se tudo falhar, coloca 0 para não quebrar o script
                item.append(0)
        
        data.append(item)

    # Cria o DataFrame
    df = pd.DataFrame(data, columns=colunas_sql)

    # Tratamento específico de Data
    df['Data_Log'] = pd.to_datetime(df['Data_Log'], format='%Y%m%d')

    # Salva no SQL
    df.to_sql(nome_tabela, con=engine, if_exists='append', index=False)
    
    return len(df)

# --- 5. EXECUÇÃO PRINCIPAL (LOOP) ---
if __name__ == '__main__':
    print("🚀 Iniciando Carga Histórica Multi-Tabelas...")
    
    # Prepara conexão com Banco e API
    conn_str = f'mssql+pyodbc://@{SERVER}/{DATABASE}?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes'
    engine = create_engine(conn_str)
    client = BetaAnalyticsDataClient()

    diferenca = DATA_FIM - DATA_INICIO
    total_dias = diferenca.days + 1
    
    print(f"📅 Período: {DATA_INICIO} até {DATA_FIM} ({total_dias} dias)")

    # Loop pelos dias
    for i in range(total_dias):
        dia_atual = DATA_INICIO + timedelta(days=i)
        str_dia = dia_atual.strftime('%Y-%m-%d')
        
        print(f"\n⏳ Processando: {str_dia}")

        # Loop pelas tabelas (Trafego, Sessao, Item, Eventos)
        for tabela, config in CONFIG_RELATORIOS.items():
            print(f"   > Atualizando {tabela}... ", end="")
            
            # 1. Busca
            resposta = buscar_dados_ga4(client, PROPERTY_ID, str_dia, config)
            
            # 2. Salva
            qtd = processar_e_salvar(resposta, tabela, config['colunas_sql'], engine)
            
            if qtd > 0:
                print(f"✅ Ok (+{qtd} linhas)")
            else:
                print("⚠️ Vazio")
            
            # Pequena pausa para não estourar cota entre tabelas
            time.sleep(1)

        # Pausa maior entre dias
        time.sleep(1)

    print("\n🏁 Carga Histórica Finalizada! Todas as tabelas foram populadas.")