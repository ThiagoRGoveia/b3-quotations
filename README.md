# B3 Quotations Ingestion and API


Este projeto é um sistema completo para ingestão, armazenamento e consulta de dados históricos de cotações da B3. Ele é totalmente containerizado usando Docker e gerenciado com um `Makefile` simples para fácil configuração e operação.


## Tecnologias


- [Go](https://golang.org/)
- [Docker](https://docs.docker.com/get-docker/)
- [Docker Compose](https://docs.docker.com/compose/install/)
- [PostgreSQL](https://www.postgresql.org/)




## Arquitetura do Projeto


O projeto é composto por três componentes principais:




### Componentes da Aplicação


1. **API (cmd/api)**
  - Servidor REST que fornece endpoints para consulta dos dados de cotações da B3.
  - Permite filtrar cotações por ticker, data de início e data de fim.


2. **Data Ingestion (cmd/data_ingestion)**
  - Sistema de ingestão de dados responsável por processar arquivos CSV da B3.
  - Utiliza uma arquitetura de processamento assíncrono para alta performance.
  - Garante idempotência através de verificações de checksums dos arquivos.


3. **Database Setup (cmd/setup)**
  - Script para inicialização da estrutura do banco de dados.
  - Cria as tabelas, índices e partições necessárias.


### Fluxo de Ingestão de Dados


O processo de ingestão de dados é altamente otimizado e segue estas etapas:


1. **Escaneamento de Arquivos**
  - Identifica todos os arquivos CSV na pasta especificada.
  - Extrai datas de referência para preparar partições no banco de dados.


2. **Pré-processamento**
  - Calcula checksums dos arquivos para evitar duplicações.
  - Verifica se o arquivo já foi processado anteriormente.
  - Registra os arquivos no banco de dados com status "processing".


3. **Processamento Paralelo**
  - Utiliza múltiplos worker pools para processamento paralelo:
    - **Parser Workers**: Leem os arquivos CSV e extraem os dados.
    - **DB Workers**: Inserem dados no banco de dados.
    - **Error Worker**: Gerencia erros durante o processo de ingestão.


4. **Otimizações de ingestão**
  - Usa particionamento da tabela trade_records por data para melhorar performance de consultas.
  - Implementa tabelas temporárias (staging) para cargas em lote.
  - Remove e recria índices estrategicamente durante o carregamento para maximizar a performance.


### Da estrutura do banco de dados


O banco de dados do projeto é composto por duas tabelas principais, projetadas para armazenar os registros de arquivos processados e os dados de negociações de forma otimizada:


#### 1. Tabela `file_records`


Tabela responsável por armazenar o registro de todos os arquivos processados pelo sistema, garantindo idempotência no nível de arquivo:


```sql
CREATE TABLE file_records (
   id SERIAL PRIMARY KEY,
   file_name VARCHAR(255) NOT NULL,
   processed_at TIMESTAMP NOT NULL,
   status VARCHAR(50) NOT NULL CHECK (status IN ('DONE', 'DONE_WITH_ERRORS', 'PROCESSING', 'FATAL')),
   checksum VARCHAR(64),
   reference_date TIMESTAMP,
   errors jsonb
);
```


**Campos:**
- `id`: Identificador único para cada arquivo processado
- `file_name`: Caminho do arquivo processado
- `processed_at`: Data e hora do processamento
- `status`: Status do processamento (DONE, DONE_WITH_ERRORS, PROCESSING, FATAL)
- `checksum`: Hash SHA-256 do arquivo para identificação única e verificação de idempotência
- `reference_date`: Data de referência extraída do arquivo
- `errors`: Erros ocorridos durante o processamento (formato JSONB)


#### 2. Tabela `trade_records`


Tabela principal que armazena os registros de negociações, utilizando particionamento por data de referência para otimizar o desempenho:


```sql
CREATE TABLE trade_records (
   id BIGSERIAL NOT NULL,
   reference_date TIMESTAMP NOT NULL,
   transaction_date TIMESTAMP NOT NULL,
   ticker VARCHAR(255) NOT NULL,
   identifier VARCHAR(255) NOT NULL,
   price NUMERIC(18, 2) NOT NULL,
   quantity BIGINT NOT NULL,
   closing_time VARCHAR(50) NOT NULL,
   file_id INTEGER,
   checksum VARCHAR(64) NOT NULL
) PARTITION BY RANGE (reference_date);
```


**Campos:**
- `id`: Identificador único para cada registro de negociação
- `reference_date`: Data de referência do arquivo (usado para particionamento)
- `transaction_date`: Data e hora da transação
- `ticker`: Código do ativo negociado
- `identifier`: Identificador da transação
- `price`: Preço da negociação
- `quantity`: Quantidade negociada
- `closing_time`: Horário de fechamento da negociação
- `file_id`: Referência ao arquivo de origem (relação com file_records)
- `checksum`: Hash para verificação de idempotência no nível de registro


#### Sistema de Particionamento


A tabela `trade_records` utiliza um sistema de particionamento por intervalo baseado no campo `reference_date`. Este valor é sempre único para cada arquivo processado, o que apresenta uma oportunidade de otimização significativa. Cada partição armazena dados de um único dia, o que otimiza significativamente as consultas por data.


**Funcionamento do particionamento:**


1. **Naming Convention**: As partições seguem o padrão de nomenclatura `trade_records_YYYYMMDD` (ex: `trade_records_20250918` para dados do dia 18/09/2025).


2. **Criação Dinâmica**: As partições são criadas automaticamente durante o processo de ingestão, baseadas nas datas de referência encontradas nos arquivos:
  ```sql
  CREATE TABLE trade_records_YYYYMMDD PARTITION OF trade_records
  FOR VALUES FROM ('YYYY-MM-DD 00:00:00') TO ('YYYY-MM-DD+1 00:00:00');
  ```


3. **Otimização de Performance**:
  - Cada partição contém aproximadamente 8 milhões de registros (estimativa para um dia de negociações)
  - Consultas filtradas por data acessam apenas as partições relevantes, evitando varredura completa da tabela
  - Índices são aplicados individualmente em cada partição, reduzindo o overhead


4. **Rastreamento de Primeira Escrita**: O sistema mantém controle de quais partições foram criadas durante a execução atual, permitindo otimizações adicionais:
  - Para partições recém-criadas, utiliza-se o método `InsertAllStagingTableData` (mais rápido)
  - Para partições já existentes, utiliza-se o método `InsertDiffFromStagingTable` (com verificação de duplicidade)


#### Tabelas de Staging


Para otimizar o processo de ingestão, o sistema utiliza tabelas temporárias de staging:


```sql
CREATE UNLOGGED TABLE trade_records_staging_worker_N (LIKE trade_records INCLUDING DEFAULTS);
```


- Cada worker de banco de dados recebe sua própria tabela de staging
- Tabelas são configuradas como `UNLOGGED` para máxima performance de escrita
- O sistema utiliza operações COPY em massa para carregar os dados nestas tabelas
- A verificação de idempotência é realizada comparando checksums entre staging e tabela principal quando necessário.
**OBS**: Aqui seria possível uma otimização adicional, quando a verificação for identificada como desnecessária, a cópia pode ser feita diretamente na tabela principal. Porém isso é costumeiramente considerado má prática, já que a tabela staging apresenta uma boa oportunidade de manipular e validar dados, além de trazer complexidade adicional ao código, portanto decidi contra a diferenciação.


### Performance e Escalabilidade


A aplicação foi projetada com foco em alto desempenho:


- **Processamento Assíncrono**: Utiliza goroutines e channels para paralelismo.
- **Configurabilidade**: Parâmetros ajustáveis para número de workers, tamanho de batch e buffers.
- **Particionamento**: Dados organizados por data para consultas eficientes.
- **Idempotência**: Garante que arquivos não sejam processados mais de uma vez, adicionalmente, quando identificada uma partição já existente, o processo de ingestão verifica o checksum de cada linha do arquivo para evitar duplicações.
- **Batch Processing**: Insere dados em lotes para reduzir o overhead de transações.


### Diagrama de Arquitetura


![Diagrama de Arquitetura](/architectrure_dark.png)


### Tratamento de Erros

Os errors emitidos pelos workers durantes o processamento de arquivos são coletados e processados pelo `ErrorWorker`, armazenados em memória até o final do processamento, quando são persistidos no banco de dados.

#### 1. Arquitetura de Tratamento de Erros

- **Worker Dedicado**: Um worker assíncrono especializado (`ErrorWorker`) é responsável por coletar e processar todos os erros que ocorrem durante a ingestão de dados.
- **Canal Centralizado**: Todos os erros são enviados para um canal único (`Errors`), permitindo processamento assíncrono e evitando bloqueios nos workers de processamento.
- **Associação com Arquivos**: Cada erro é associado ao arquivo que o originou.

#### 2. Persistência de Erros

- **Armazenamento em Banco de Dados**: Todos os erros são persistidos na tabela `file_records` em um campo `errors` no formato JSONB.
- **Status do Arquivo**: Os registros de arquivo têm seu status atualizado conforme os erros:
  - `PROCESSING`: Durante o processamento do arquivo
  - `DONE`: Processamento concluído sem erros
  - `DONE_WITH_ERRORS`: Processamento concluído com erros (mas alguns dados podem ter sido carregados)
  - `FATAL`: Erros críticos que impediram completamente o processamento

#### 3. Limitação e Proteção

- **Proteção contra Sobrecarga**: O sistema limita a coleta a 100 erros por arquivo para evitar sobrecarga de memória.
- **Log de Erros**: Todos os erros são registrados no log do sistema, mesmo quando não salvos no banco de dados.
- **Detalhes Ricos**: Os erros armazenam informações detalhadas, incluindo:
  - Mensagem descritiva
  - ID do arquivo associado
  - Detalhes do erro original
  - Dados do trade que causou o erro (quando aplicável)


### Considerações e Desafios

- **Idempotência**: A garantia de idempotência representou o grande desafio do projeto, já que a checagem de unicidade, especialmente na ausência de dados com chaves primárias torna o processo de ingestão mais pesado. Para grandes volumes de dados, PostgresSQL se torna notoriamente lento em gerenciar os índices em tempo hábil para garantir a alta velocidade de ingestão. Como alternativa, foi implementada a checagem de idempotência na camada de aplicação, empregando o uso de checksums para verificar a unicidade dos dados.
- **Performance** A adoção do sistema de particionamento em conjunto com a estratégia de carregamento dos dados seccionados por data foi crucial para eliminar a queda de performance progressiva causada pela explosão de índices.
- **Sobre o uso de checksums**: O uso de checksums é prática comum para comparar a unicidade de arquivos, nesse caso a escolha de calcular o checksum também da linha se deu devido à falta de uma chave identificadora da linha que garanta sua unicidade sem angariar o risco de falsas colisões, portanto, o uso de checksums foi a escolha mais viável.


## Conclusões


Como esperado foi observado um trade-off entre performance, complexidade e robustez da idempotência implementada. Para o volume de dados esperado (em torno de 59 Milhões de linhas) por carga, considero inviável checar a unicidade de cada linha do arquivo na camada de dados. A solução atual surgiu da minha observação de que os arquivos são, em geral bem formados e portanto, é improvável que uma mesma data de referência e portanto a mesma partição da base receba duas cargas de arquivos diferentes, ficando essas para casos em que ocorra falhas no processo de ingestão em si acarretando ingestão parcial dos dados. Portanto podemos esperar uma performance superior para o caso geral, isto é, aquele em que há a carga fria e total dos dados, com sacrifício de performance no caso de ingestão parcial dos dados.






---


## Pré-requisitos


Antes de começar, certifique-se de que você tem o seguinte instalado:
- [Docker](https://docs.docker.com/get-docker/)
- [Docker Compose](https://docs.docker.com/compose/install/)


---


## Setup


Siga estes passos para colocar a aplicação em funcionamento:


### 1. Set up Environment Variables


O projeto usa um arquivo `env-example` como modelo. Um arquivo `.env` será criado a partir deste modelo automaticamente quando você executar os comandos `start` ou `setup`. Você pode modificar o `env-example` antes de executar, se necessário.


### 2. Set up the Database


Após iniciar os serviços, execute o script de configuração para criar as tabelas necessárias no banco de dados:


```bash
make setup
```


### 3. Build and Start the Services


Execute o seguinte comando para construir as imagens Docker e iniciar os serviços `postgres` e `api`:


```bash
make start
```


Isso iniciará o banco de dados e a API principal em segundo plano.






### 4. Ingest Data


Coloque seus arquivos de dados da B3 no diretório `files/`. Em seguida, execute o comando de ingestão:


AVISO: Este software assume que os arquivos estão em formato CSV, já descompactados, e colocados como são baixados do [site da B3](https://www.b3.com.br/pt_br/market-data-e-indices/servicos-de-dados/market-data/cotacoes/cotacoes/).


```bash
make ingest
```


Isso iniciará o serviço de ingestão de dados, que processará os arquivos e os carregará no banco de dados. Você também pode especificar um diretório diferente:


```bash
make ingest FILES_PATH=./path/to/your/files
```


Seu sistema agora está configurado e populado com dados. A API está em execução e pronta para aceitar requisições em `http://localhost:8080`.


Example request:


```bash
curl --location 'http://localhost:8080/tickers/ETRU25?data_inicio=2025-09-05'
```
---

### Running Tests


Testes unitários:


```bash
make test
```





## Usage (Makefile Commands)


Todas as tarefas comuns são gerenciadas através do `Makefile`.


| Command      | Description                                                                                                                             |
|--------------|-----------------------------------------------------------------------------------------------------------------------------------------|
| `make start`   | Constrói as imagens Docker (se ainda não foram construídas) e inicia os serviços `api` e `postgres` em modo detached.                                |
| `make stop`    | Para todos os contêineres Docker em execução definidos no arquivo `docker-compose.yml`.                                                           |
| `make setup`   | Garante que os serviços estejam em execução e, em seguida, executa o script de configuração do banco de dados em um contêiner temporário para criar as tabelas necessárias.       |
| `make ingest`  | Executa o script de ingestão de dados em um contêiner temporário. Por padrão, processa os arquivos no diretório `./files`.                       |
| `make clean-db`| Para todos os serviços e **exclui permanentemente o volume do banco de dados**. Isso é útil para uma redefinição completa do ambiente do banco de dados. |


---


## Project Structure


```
├── cmd/                    # Main applications
│   ├── api/                # API server source code
│   ├── data_ingestion/     # Data ingestion script source code
│   └── setup/              # Database setup script source code
├── internal/               # Internal Go packages (business logic, database, etc.)
├── files/                  # Default directory for placing data files for ingestion
├── .env                    # Environment variables file (auto-generated from env-example)
├── env-example             # Template for environment variables
├── Dockerfile              # Multi-stage Dockerfile for building Go applications
├── docker-compose.yml      # Defines and orchestrates the application services
├── go.mod                  # Go module definition
├── Makefile                # Command-line interface for managing the project
```


---


## Environment Variables


O arquivo `env-example` contém as variáveis de configuração para a aplicação. Quando você executa `make start` ou `make setup`, um arquivo `.env` é criado a partir deste modelo.


- `DATABASE_URL`: A string de conexão para as aplicações Go se conectarem ao banco de dados.
- `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`: Usado pelo contêiner `postgres` para sua configuração inicial.
- `NUM_PARSER_WORKERS`, `NUM_DB_WORKERS_PER_DATE`, `RESULTS_CHANNEL_SIZE`, `DB_BATCH_SIZE`: Parâmetros de configuração para o desempenho do serviço de ingestão de dados.
