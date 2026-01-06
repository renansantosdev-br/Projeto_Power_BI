USE PECADIRETA_BI
GO

-- 1. Tabela de Tráfego Geral (Origem, Mídia e Campanhas)
-- Alimenta: f_Trafego
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'f_Trafego')
CREATE TABLE f_Trafego (
    Data_Log DATE,
    Origem VARCHAR(100),         -- sessionSource
    Midia VARCHAR(100),          -- sessionMedium
    Campanha VARCHAR(150),       -- sessionCampaign
    Dispositivo VARCHAR(50),     -- deviceCategory
    Sessoes INT,                 -- sessions
    Usuarios_Ativos INT,         -- activeUsers
    Novos_Usuarios INT,          -- newUsers
    Receita DECIMAL(18,2),       -- totalRevenue
    Data_Carga DATETIME DEFAULT GETDATE()
);

-- 2. Tabela de Comportamento de Página (Onde o usuário andou)
-- Alimenta: f_Acessos_sessao
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'f_Acessos_sessao')
CREATE TABLE f_Acessos_sessao (
    Data_Log DATE,
    Pagina_Caminho VARCHAR(MAX), -- pagePath
    Pagina_Titulo VARCHAR(MAX),  -- pageTitle
    Visualizacoes INT,           -- screenPageViews
    Usuarios INT,                -- activeUsers
    Tempo_Medio_Segundos FLOAT,  -- averageSessionDuration
    Data_Carga DATETIME DEFAULT GETDATE()
);

-- 3. Tabela de Produtos (Performance do E-commerce)
-- Alimenta: f_Acessos_Item e f_Eventos_Fornecedor (Via Agrupamento)
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'f_Acessos_Item')
CREATE TABLE f_Acessos_Item (
    Data_Log DATE,
    Item_ID VARCHAR(100),        -- itemId
    Item_Nome VARCHAR(255),      -- itemName
    Item_Marca VARCHAR(100),     -- itemBrand (Para f_Eventos_Fornecedor)
    Item_Categoria VARCHAR(100), -- itemCategory
    Vistos INT,                  -- itemsViewed
    Adicionados_Carrinho INT,    -- itemsAddedToCart
    Comprados INT,               -- itemsPurchased
    Receita_Item DECIMAL(18,2),  -- itemRevenue
    Data_Carga DATETIME DEFAULT GETDATE()
);

-- 4. Tabela de Eventos Globais (Interações e Leads)
-- Alimenta: f_Interacoes e f_Leads (Separamos via filtro no Power BI ou View)
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'f_Eventos_Geral')
CREATE TABLE f_Eventos_Geral (
    Data_Log DATE,
    Nome_Evento VARCHAR(100),    -- eventName
    Origem VARCHAR(100),         -- sessionSource (Para saber de onde veio o Lead)
    Quantidade_Eventos INT,      -- eventCount
    Total_Usuarios INT,          -- totalUsers
    Data_Carga DATETIME DEFAULT GETDATE()
);