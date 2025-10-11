📊 Big Data - Análise de Transações Financeiras

Projeto de análise de dados de transações financeiras utilizando Apache Hadoop MapReduce. 
Desenvolvido como trabalho avaliativo para a disciplina de Big Data.

📋 Sobre o Projeto

Este projeto implementa diversas rotinas MapReduce para processar e analisar um dataset de transações financeiras, demonstrando conceitos fundamentais e intermediários de processamento distribuído de dados em larga escala.

🎯 Objetivos

- Processar grandes volumes de dados de transações financeiras
- Demonstrar padrões de MapReduce (agregação, contagem, ranking)
- Implementar Custom Writables para estruturas de dados complexas
- Otimizar desempenho com Combiners
- Realizar pipelines multi-step encadeados

📁 Estrutura do Projeto
```
src/main/java/routines/
├── basic/                         # Rotinas básicas de MapReduce
│   ├── amountbycity/              # Soma de valores por cidade
│   ├── amountbyclient/            # Soma de valores por cliente
│   ├── chipusagecount/            # Contagem por tipo de transação
│   ├── errorcountbymcc/           # Contagem de erros por categoria
│   └── transactioncountbystate/   # Contagem por estado
│
├── intermediate/                  # Rotinas intermediárias (Custom Writables)
│   ├── citystatistics/            # Estatísticas completas por cidade
│   ├── citytimeperiod/            # Análise temporal por cidade
│   ├── topcategoriesbycity/       # Top 3 categorias por cidade
│   ├── topcategoriesbycountry/    # Top 3 categorias por país
│   └── topcategoriesbystate/      # Top 3 categorias por estado
│
└── advanced/                      # Rotinas avançadas (Multi-step pipelines)
    ├── categorybytimeperiod/      # Top 3 categorias por período e cidade (2 jobs)
    ├── clientbehaviorchipuse/     # Perfil de risco por cliente e UF (2 jobs)
    ├── merchanthrisk/             # Radar de saúde e risco por UF (2 jobs)
    ├── rfmbyuf/                   # Análise RFM por UF (2 jobs)
    └── riskanalysis/              # Pipeline de análise de risco (3 jobs)
```
### 🚀 Rotinas Implementadas

### 📌 Rotinas Básicas
```
1. AmountByCity
   Calcula o valor total transacionado em cada cidade.

Output: CIDADE    $XX,XXX.XX
Conceitos: Agregação simples, precisão monetária (centavos)

2. AmountByClient
   Calcula o valor total transacionado por cada cliente.

Output: CLIENT_ID    $XX,XXX.XX
Conceitos: Agregação por chave, análise de volume por cliente

3. ChipUsageCount
   Conta transações por tipo (Chip, Swipe, Online, etc).

Output: TIPO_TRANSACAO    CONTAGEM
Conceitos: Classificação, análise de distribuição

4. ErrorCountByMCC
   Conta erros por categoria de comerciante (MCC codes).

Output: MCC_CODE    CONTAGEM_ERROS
Conceitos: Filtragem, mapeamento de códigos para descrições

5. TransactionCountByState
   Conta transações por estado americano.

Output: ESTADO    CONTAGEM
Conceitos: Validação de dados, análise geográfica por região
```

### ⭐ Rotinas Intermediárias
```
1. CityStatistics (Custom Writable)
   Calcula estatísticas completas por cidade: contagem, total e ticket médio.

Output: CIDADE    Transações: N | Total: $X.XX | Média: $Y.YY
Conceitos: Custom Writable com múltiplos campos, agregação complexa

2. CityTimePeriod (Análise Temporal)
   Analisa transações por período do dia (Manhã, Tarde, Noite).

Output: CIDADE    Manhã: X (Y%) | Tarde: Z (W%) | Noite: A (B%) | Pico: [Período]
Conceitos: Parsing de timestamps, múltiplos contadores independentes

3. TopCategoriesByCity (Ranking)
   Identifica as 3 categorias mais populares em cada cidade.

Output: CIDADE    Top-1: MCC (Descrição) N | Top-2: ... | Top-3: ...
Conceitos: Agregação com HashMap, sorting, ranking

4. TopCategoriesByState (Ranking)
   Identifica as 3 categorias mais populares em cada estado dos EUA.

Output: ESTADO    Top-1: MCC (Descrição) N | Top-2: ... | Top-3: ...
Conceitos: Validação de estados, reutilização de Custom Writable

5. TopCategoriesByCountry (Ranking)
   Identifica as 3 categorias mais populares em cada país (exceto EUA).

Output: PAÍS    Top-1: MCC (Descrição) N | Top-2: ... | Top-3: ...
Conceitos: Filtragem por país, análise de transações internacionais
```

### 🔥 Rotinas Avançadas (Multi-step)
```
**1. CategoryByTimePeriod** (2 Jobs)
- Job 1: Agrega transações por cidade-período-MCC
- Job 2: Identifica top 3 categorias por cidade e período
- Demonstra: Chave composta, SequenceFile, Pipeline encadeado

**2. ClientBehaviorChipUse** (2 Jobs)
- Job 1: Calcula perfil do cliente (taxa online, erros, ticket médio)
- Job 2: Agrega por UF com classificação LOW/MED/HIGH e hotspots
- Demonstra: Métricas comportamentais, classificação de risco

**3. MerchantHealthRisk** (2 Jobs)
- Job 1: Classifica comerciantes por saúde (A/B/C) e risco (LOW/MED/HIGH)
- Job 2: Consolida por UF com top-K merchants e hotspots
- Demonstra: Dupla classificação, top-K dinâmico

**4. RfmByUF** (2 Jobs)
- Job 1: Análise RFM (Recency, Frequency, Monetary) por cliente
- Job 2: Agrega por UF com classificação de valor
- Demonstra: Análise temporal, segmentação de clientes

**5. RiskAnalysisPipeline** (3 Jobs)
- Job 1: Constrói perfis comportamentais dos clientes
- Job 2: Classifica em categorias de risco (LOW/MED/HIGH/CRITICAL)
- Job 3: Gera relatórios consolidados com rankings
- Demonstra: Pipeline complexo de 3 etapas, análise de fraude
```

### 📊 Dataset

O projeto utiliza um dataset de transações financeiras em formato CSV com a seguinte estrutura:
```
csvid,date,client_id,card_id,amount,use_chip,merchant_id,merchant_city,merchant_state,zip,mcc,errors
```
Campos:
```
id: ID único da transação
date: Data e hora da transação (YYYY-MM-DD HH:MM:SS)
client_id: ID do cliente
card_id: ID do cartão
amount: Valor da transação ($XX.XX)
use_chip: Tipo de transação (chip, swipe, online)
merchant_id: ID do comerciante
merchant_city: Cidade do comerciante
merchant_state: Estado/País do comerciante
zip: CEP
mcc: Merchant Category Code (código da categoria)
errors: Campo de erros/validações
```
### 📍 Localização
```
src/main/resources/transactions_data.csv
```
### 🛠️ Tecnologias Utilizadas

Java 8+

Apache Hadoop 3.4.2

MapReduce Framework

Maven (gerenciamento de dependências)

### ⚙️ Como Executar
Pré-requisitos
```
# Hadoop instalado e configurado
# Java 8 ou superior
# Maven (opcional, se usar build)
```
Execução em Modo Local (Standalone)

Todas as rotinas podem ser executadas em modo local para testes:
```
# Rotina Básica - Exemplo: AmountByCity
java -cp target/classes routines.basic.amountbycity.AmountByCity \
src/main/resources/transactions_data.csv \
output/amount_by_city \
1 \
local

# Rotina Intermediária - Exemplo: CityStatistics
java -cp target/classes routines.intermediate.citystatistics.CityStatistics \
src/main/resources/transactions_data.csv \
output/city_statistics \
1 \
local

# Rotina Avançada (Multi-step) - Exemplo: RiskAnalysis
java -cp target/classes routines.advanced.riskanalysis.RiskAnalysisPipeline \
  src/main/resources/transactions_data.csv \
  output/risk_pipeline \
  local
```
Parâmetros de Execução
```
<input_path>     : Caminho do arquivo CSV
<output_path>    : Diretório de saída (será criado)
[num_reducers]   : Número de reducers (opcional, padrão: 1)
[local]          : Modo local (opcional, omitir para cluster)
```
Ver Resultados
```
# Rotinas básicas e intermediárias
cat output/[nome_rotina]/part-r-00000

# Rotinas avançadas (multi-step)
cat output/[nome_rotina]_step3_final/part-r-00000
```
