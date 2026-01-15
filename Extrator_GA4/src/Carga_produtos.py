import pandas as pd
from sqlalchemy import create_engine
import urllib

# 1. Configuração das Conexões
# A: Servidor do Cliente (REMOTO - Onde estão os produtos)
# Ajuste 'usuario' e 'senha' com os dados reais de acesso ao 10.39...
params_remoto = urllib.parse.quote_plus(
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=10.39.132.4;'
    'DATABASE=pecadireta;'
    'UID=renan.santos;'  # <--- Coloque o usuário do banco remoto
    'PWD=ELP&bk&rdurNK#uUD4YGc$;'    # <--- Coloque a senha do banco remoto
)
engine_remoto = create_engine(f"mssql+pyodbc:///?odbc_connect={params_remoto}")

# B: Seu Servidor (LOCAL - Onde está o BI)
params_local = urllib.parse.quote_plus(
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=Renan_RFS_DEV\\SQLEXPRESS;'
    'DATABASE=PECADIRETA_BI;'
    'Trusted_Connection=yes;'
)
engine_local = create_engine(f"mssql+pyodbc:///?odbc_connect={params_local}")

print("📥 Iniciando download da tabela de produtos do servidor remoto...")

# 2. Busca os dados lá no cliente (CORRIGIDO: Tipo_Peca em vez de Item_Categoria)
query = "SELECT ID_Peca, SKU, Nome_Peca, Marca_Peca, Tipo_Peca FROM vw_BI_Dim_Produtos_Estoque"

try:
    df_produtos = pd.read_sql(query, engine_remoto)
    print(f"✅ Download concluído! {len(df_produtos)} produtos encontrados.")
    
    # 3. Salva uma cópia no SEU banco local (Tabela Espelho)
    print("💾 Salvando cópia local (d_Produtos_Espelho)...")
    df_produtos.to_sql('d_Produtos_Espelho', engine_local, if_exists='replace', index=False)
    print("🚀 Sucesso! Tabela d_Produtos_Espelho criada no PECADIRETA_BI.")

except Exception as e:
    print(f"❌ Erro: {e}")