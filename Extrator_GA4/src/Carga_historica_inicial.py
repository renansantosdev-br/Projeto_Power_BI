import os
import time
import pandas as pd
from datetime import date, timedelta, datetime
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest
from sqlalchemy import create_engine

# --- 1. CONFIGURAÇÕES GERAIS ---
# Ajuste de datas conforme a necessidade da Carga Histórica
DATA_INICIO = date(2024, 1, 1)
DATA_FIM = date(2026, 1, 12)

SERVER = 'Renan_RFS_DEV\\SQLEXPRESS'
DATABASE = 'PECADIRETA_BI'
PROPERTY_ID = '400960026'

# Autenticação
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
KEY_PATH = os.path.join(BASE_DIR, 'config', 'ga4_keys.json')
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = KEY_PATH

# --- 2. O MAPA DOS RELATÓRIOS (ATUALIZADO) ---
CONFIG_RELATORIOS = {
    'f_Trafego': {
        # ATUALIZADO: Inclui Geografia, Tempo e Engajamento
        'dimensions': [
            'date', 'sessionSource', 'sessionMedium', 'sessionCampaignName', 'deviceCategory',
            'city', 'region', 'dayOfWeek', 'hour' # <--- Novos Campos
        ],
        'metrics': [
            'sessions', 'activeUsers', 'newUsers', 'totalRevenue',
            'engagedSessions', 'averageSessionDuration', 'engagementRate', 'screenPageViews' # <--- Novos Campos
        ],
        # A ordem aqui DEVE ser: Dimensões seguidas de Métricas
        'colunas_sql': [
            'Data_Log', 'Origem', 'Midia', 'Campanha', 'Dispositivo',
            'Cidade', 'Regiao', 'Dia_Semana', 'Hora',                # Dimensões
            'Sessoes', 'Usuarios_Ativos', 'Novos_Usuarios', 'Receita',
            'Sessoes_Engajadas', 'Tempo_Medio_Sessao', 'Taxa_Engajamento', 'Visualizacoes_Pagina' # Métricas
        ]
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

# --- CLASSE AUXILIAR PARA PAGINAÇÃO ---
class SimpleResponse:
    """Simula a resposta do GA4 para juntar várias páginas."""
    def __init__(self, rows):
        self.rows = rows

# --- 3. FUNÇÃO DE BUSCA COM PAGINAÇÃO (BLINDADA) ---
def buscar_dados_ga4(client, property_id, data_str, config):
    """
    Faz o pedido ao Google COM PAGINAÇÃO AUTOMÁTICA para evitar limite de 10k linhas.
    """
    dim_list = [Dimension(name=d) for d in config['dimensions']]
    met_list = [Metric(name=m) for m in config['metrics']]
    
    todas_as_linhas = []
    offset = 0
    limit = 100000 # Pedimos blocos grandes (padrão da API é 10k, mas suporta até 100k)

    while True:
        # print(f"      ... buscando offset {offset}") # Descomente se quiser ver o progresso detalhado
        request = RunReportRequest(
            property=f"properties/{property_id}",
            date_ranges=[DateRange(start_date=data_str, end_date=data_str)],
            dimensions=dim_list,
            metrics=met_list,
            limit=limit,
            offset=offset
        )

        try:
            response = client.run_report(request)
            
            # Se não veio nada, paramos
            if not response.rows:
                break
                
            # Adiciona as linhas desse lote na lista principal
            todas_as_linhas.extend(response.rows)
            
            # Se a quantidade retornada for menor que o limite, acabaram os dados
            if len(response.rows) < limit:
                break
            
            # Prepara o próximo salto
            offset += limit
            
        except Exception as e:
            print(f"   ❌ Erro na API GA4: {e}")
            return None

    # Retorna um objeto compatível com a função de processamento
    if todas_as_linhas:
        return SimpleResponse(todas_as_linhas)
    else:
        return None

# --- 4. FUNÇÃO DE PROCESSAMENTO E CARGA ---
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
        
        # Adiciona os valores das Métricas
        for met in row.metric_values:
            try:
                val_float = float(met.value)
                if val_float.is_integer():
                    item.append(int(val_float))
                else:
                    item.append(val_float)
            except ValueError:
                item.append(0)
        
        data.append(item)

    # Cria o DataFrame garantindo a ordem das colunas
    df = pd.DataFrame(data, columns=colunas_sql)

    # Tratamento específico de Data
    df['Data_Log'] = pd.to_datetime(df['Data_Log'], format='%Y%m%d')

    # OBS: Não precisamos enviar Data_Carga aqui se o SQL já tiver DEFAULT GETDATE()
    # Se precisar enviar pelo Python, descomente a linha abaixo:
    # df['Data_Carga'] = datetime.now()

    # Salva no SQL
    df.to_sql(nome_tabela, con=engine, if_exists='append', index=False)
    
    return len(df)

# --- 5. EXECUÇÃO PRINCIPAL ---
if __name__ == '__main__':
    print("🚀 Iniciando Carga Histórica Otimizada (Com Paginação)...")
    
    conn_str = f'mssql+pyodbc://@{SERVER}/{DATABASE}?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes'
    engine = create_engine(conn_str)
    client = BetaAnalyticsDataClient()

    diferenca = DATA_FIM - DATA_INICIO
    total_dias = diferenca.days + 1
    
    print(f"📅 Período: {DATA_INICIO} até {DATA_FIM} ({total_dias} dias)")

    for i in range(total_dias):
        dia_atual = DATA_INICIO + timedelta(days=i)
        str_dia = dia_atual.strftime('%Y-%m-%d')
        
        print(f"\n⏳ Processando: {str_dia}")

        for tabela, config in CONFIG_RELATORIOS.items():
            print(f"   > {tabela}... ", end="")
            
            # 1. Busca com Paginação
            resposta = buscar_dados_ga4(client, PROPERTY_ID, str_dia, config)
            
            # 2. Salva
            qtd = processar_e_salvar(resposta, tabela, config['colunas_sql'], engine)
            
            if qtd > 0:
                print(f"✅ Ok (+{qtd} linhas)")
            else:
                print("⚠️ Vazio")
            
            time.sleep(1) # Pausa leve

        time.sleep(1) # Pausa entre dias

    print("\n🏁 Carga Histórica Finalizada com Sucesso!")