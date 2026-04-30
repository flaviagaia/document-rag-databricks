# document-rag-databricks

Este projeto mostra como montar uma **RAG documental usando o ecossistema Databricks de ponta a ponta**.

Ele foi desenhado para representar um fluxo completo de lakehouse:

- ingestão de documentos em tabelas Delta;
- organização em camadas `bronze`, `silver` e `gold`;
- governança via `Unity Catalog`;
- tabela-fonte para **Mosaic AI Vector Search**;
- app em **Streamlit** no estilo **Databricks Apps**;
- e uma rota local reproduzível para manter o repositório executável no GitHub.

Hoje o projeto já existe em duas formas complementares:

- uma **versão local reproduzível**, ideal para GitHub, testes e demonstrações rápidas;
- uma **versão conectada ao Databricks real**, com `Unity Catalog`, `Delta tables`, `Mosaic AI Vector Search` e `Databricks Apps`.

## Storytelling básico

Quando uma empresa quer construir um assistente de documentação, normalmente ela precisa mais do que um chatbot. Ela precisa de:

- um lugar governado para guardar os documentos;
- um pipeline para quebrar e preparar o conteúdo;
- um índice vetorial para recuperação semântica;
- uma interface final para consulta;
- e um caminho claro entre dado bruto e resposta final.

Esse projeto existe para mostrar exatamente isso.

## Interface

![Demo do app Databricks RAG](assets/RAG_Databricks.jpg)

## Base pública escolhida

A inspiração da base foi:

- [ibm-research/watsonxDocsQA](https://huggingface.co/datasets/ibm-research/watsonxDocsQA)

Motivo da escolha:

- é um dataset voltado para **document QA / RAG**;
- tem corpus documental e benchmark de perguntas;
- usa documentação corporativa, o que combina muito bem com um caso de uso de Databricks em ambiente enterprise;
- tem licença `Apache 2.0`.

No repositório, eu mantive uma amostra local `watsonxDocsQA-style` para deixar o projeto leve e reproduzível.

## O que o projeto cobre do ecossistema Databricks

- **Delta Lake**
- **bronze / silver / gold**
- **Unity Catalog**
- **Delta Change Data Feed**
- **Mosaic AI Vector Search**
- **Databricks Apps com Streamlit**
- **Databricks Asset Bundles**
- **app service principal**
- **runtime híbrido com degradação controlada**

## Arquitetura

```mermaid
flowchart LR
    A["Raw enterprise documents"] --> B["Bronze Delta table"]
    B --> C["Silver chunked documents"]
    C --> D["Gold vector index source"]
    D --> E["Mosaic AI Vector Search"]
    E --> F["Databricks App (Streamlit)"]
    F --> G["Grounded answer"]
```

## Camadas do lakehouse

### Bronze
Representa a ingestão documental crua.

Exemplos de colunas:
- `doc_id`
- `url`
- `title`
- `document`
- `md_document`
- `ingested_at`

Objetivo:
- preservar o documento como entrou;
- manter rastreabilidade;
- permitir reprocessamento.

### Silver
Representa a camada de transformação.

O documento é quebrado em chunks com metadados como:
- `chunk_id`
- `doc_id`
- `chunk_order`
- `chunk_text`

Objetivo:
- preparar o conteúdo para retrieval;
- preservar fronteiras semânticas;
- melhorar precisão do RAG.

### Gold
Representa a camada pronta para indexação vetorial.

Exemplos de colunas:
- `chunk_id`
- `doc_id`
- `title`
- `url`
- `chunk_text`
- `embedding`

Objetivo:
- servir como tabela-fonte para o índice vetorial;
- desacoplar a indexação da etapa de chunking;
- permitir atualização incremental.

## Papel do Unity Catalog

Neste projeto, o `Unity Catalog` aparece como a camada conceitual de governança do fluxo:

- catálogo e schema para as tabelas do pipeline;
- permissões para consulta e leitura;
- integração com o app;
- integração com o índice vetorial.

Isso é importante porque o valor do Databricks não está só no compute. Está também em unir dados, governança e IA dentro da mesma plataforma.

## Papel do Delta Change Data Feed

O projeto também considera o uso de `Change Data Feed` no Delta, porque ele é especialmente útil quando o índice vetorial precisa ser atualizado sem reconstrução total.

No fluxo real:

- a tabela Delta registra alterações;
- apenas os documentos/chunks alterados são propagados;
- a tabela `gold` pode ser atualizada incrementalmente;
- o índice vetorial pode ser sincronizado com menos custo.

## Papel do Mosaic AI Vector Search

O **Mosaic AI Vector Search** é a peça que transforma a tabela `gold` em um índice de recuperação semântica.

No fluxo de produção:

1. a tabela `gold` contém texto + embedding + metadados;
2. um endpoint vetorial é criado;
3. um índice é criado a partir da tabela Delta;
4. o app consulta esse índice para recuperar chunks semanticamente próximos da pergunta.

Tecnicamente, este projeto usa a API de Vector Search via SDK do Databricks com uma chamada `POST` para:

- `/api/2.0/vector-search/indexes/{index_name}/query`

com um payload contendo:

- `query_vector`
- `num_results`
- `columns`

Isso foi necessário porque o índice publicado neste projeto é do tipo `DELTA_SYNC` com coluna de embedding já materializada. Nesse cenário, a consulta precisa enviar vetor explícito em vez de `query_text`.

O parser do projeto também trata explicitamente três formatos possíveis de resposta:

- `dict`
- objeto da SDK com `as_dict()`
- objeto tipado com atributos estruturados

Isso evita um problema comum em MVPs de Databricks: assumir um único formato de payload e cair no fallback mesmo com o índice já operacional.

Além disso, o projeto trata explicitamente a resposta real da API, em que `manifest` pode vir no topo do payload e `data_array` dentro de `result`. Esse detalhe foi importante para evitar respostas vazias no app mesmo quando o modo de busca principal estava ativo.

## Papel do Databricks App com Streamlit

O app final representa a camada de experiência do usuário.

Ele pode:

- receber a pergunta;
- consultar a base vetorial;
- exibir os chunks mais relevantes;
- montar uma resposta grounded;
- mostrar links, títulos e evidências.

Neste repositório, a interface em `Streamlit` funciona em dois modos:

- **modo Databricks real**: consulta o índice `workspace.document_rag.document_rag_index`;
- **modo local**: usa o fallback lexical reproduzível se o runtime Databricks não estiver disponível.

Isso deixa a demo mais honesta e mais útil: o mesmo app serve tanto para o GitHub quanto para o workspace real.

## Autenticação e autorização do app

Um ponto técnico importante deste projeto é que o app não depende de `PAT` hardcoded.

No runtime real do Databricks Apps, a aplicação usa o service principal do próprio app. Isso significa que o ambiente pode expor credenciais e metadados como:

- `DATABRICKS_HOST`
- `DATABRICKS_CLIENT_ID`
- `DATABRICKS_CLIENT_SECRET`

Além disso, o projeto usa:

- `VECTOR_SEARCH_INDEX`

para desacoplar o nome físico do índice do código Python.

Para o app consultar o índice vetorial com sucesso, o service principal precisa de:

- `USE CATALOG` no catálogo pai
- `USE SCHEMA` no schema pai
- `SELECT` no índice vetorial

No caso deste projeto, esses grants foram validados para:

- catálogo `workspace`
- schema `workspace.document_rag`
- índice `workspace.document_rag.document_rag_index`

Também foram mantidos grants coerentes sobre as tabelas Delta relacionadas ao fluxo.

## API e app compartilham a mesma lógica

Uma melhoria importante deste projeto é que:

- a API em [app.py](/Users/flaviagaia/Documents/CV_FLAVIA_CODEX/document-rag-databricks/app.py)
- e a interface em [streamlit_app.py](/Users/flaviagaia/Documents/CV_FLAVIA_CODEX/document-rag-databricks/streamlit_app.py)

usam a mesma camada de execução híbrida em [runtime_query.py](/Users/flaviagaia/Documents/CV_FLAVIA_CODEX/document-rag-databricks/src/runtime_query.py).

Na prática, isso significa:

- se o projeto estiver rodando no Databricks, ele tenta usar `Vector Search`;
- se o índice ainda estiver aquecendo ou se houver algum erro transitório, ele volta para o fallback local;
- o comportamento fica consistente entre frontend e backend.

Esse desenho reduz dois tipos de inconsistência muito comuns:

- a UI dizer uma coisa e a API devolver outra;
- o app funcionar localmente, mas cair em comportamento diferente no deployment real.

## Estrutura do repositório

- [main.py](/Users/flaviagaia/Documents/CV_FLAVIA_CODEX/document-rag-databricks/main.py)  
  Executa o pipeline local ponta a ponta.

- [app.py](/Users/flaviagaia/Documents/CV_FLAVIA_CODEX/document-rag-databricks/app.py)  
  API local simples para consulta.

- [streamlit_app.py](/Users/flaviagaia/Documents/CV_FLAVIA_CODEX/document-rag-databricks/streamlit_app.py)  
  Interface de demo no estilo de um app de consulta documental.

- [src/sample_data.py](/Users/flaviagaia/Documents/CV_FLAVIA_CODEX/document-rag-databricks/src/sample_data.py)  
  Gera a amostra local inspirada no `watsonxDocsQA`.

- [src/rag_pipeline.py](/Users/flaviagaia/Documents/CV_FLAVIA_CODEX/document-rag-databricks/src/rag_pipeline.py)  
  Faz chunking, retrieval local, grounding e artefato final.

- [src/runtime_query.py](/Users/flaviagaia/Documents/CV_FLAVIA_CODEX/document-rag-databricks/src/runtime_query.py)  
  Centraliza a decisão entre `Vector Search` real e fallback local.

- [app.yaml](/Users/flaviagaia/Documents/CV_FLAVIA_CODEX/document-rag-databricks/app.yaml)  
  Define o comando do app e injeta o nome do índice vetorial em `VECTOR_SEARCH_INDEX`.

- [databricks.yml](/Users/flaviagaia/Documents/CV_FLAVIA_CODEX/document-rag-databricks/databricks.yml)  
  Exemplo de Asset Bundle para orquestração do pipeline.

- [notebooks/01_bronze_ingestion.py](/Users/flaviagaia/Documents/CV_FLAVIA_CODEX/document-rag-databricks/notebooks/01_bronze_ingestion.py)  
  Exemplo de notebook Databricks para ingestão bronze.

- [notebooks/02_silver_chunking.py](/Users/flaviagaia/Documents/CV_FLAVIA_CODEX/document-rag-databricks/notebooks/02_silver_chunking.py)  
  Exemplo de notebook de chunking.

- [notebooks/03_gold_index_source.py](/Users/flaviagaia/Documents/CV_FLAVIA_CODEX/document-rag-databricks/notebooks/03_gold_index_source.py)  
  Exemplo de notebook para publicação da tabela-fonte do índice.

- [notebooks/04_query_vector_search.py](/Users/flaviagaia/Documents/CV_FLAVIA_CODEX/document-rag-databricks/notebooks/04_query_vector_search.py)  
  Exemplo de consulta ao Mosaic AI Vector Search.

- [sql/](/Users/flaviagaia/Documents/CV_FLAVIA_CODEX/document-rag-databricks/sql)  
  Scripts SQL para schema, tabelas Delta e notas do índice vetorial.

## Execução local

```bash
python3 main.py
```

API local:

```bash
uvicorn app:app --reload
```

Interface de demo:

```bash
streamlit run streamlit_app.py
```

## Resultado atual

- `dataset_source = watsonxdocsqa_style_local_sample`
- `runtime_mode = local_retrieval_fallback`
- `document_count = 6`
- `qa_count = 2`
- `chunk_count >= 6`
- `top_doc_id = DOC-1002`

## Estado validado no Databricks real

No workspace pessoal em que este projeto foi validado, o estado final ficou assim:

- catálogo: `workspace`
- schema: `workspace.document_rag`
- tabelas Delta:
  - `workspace.document_rag.bronze_documents`
  - `workspace.document_rag.silver_document_chunks`
  - `workspace.document_rag.gold_vector_index_source`
- índice vetorial:
  - `workspace.document_rag.document_rag_index`
- endpoint vetorial:
  - `document-rag-endpoint`
- app:
  - `document-rag-app`
- commit do app service principal:
  - grants aplicados em catálogo, schema e índice

Snapshot validado:

- `bronze_documents = 6`
- `silver_document_chunks = 12`
- `gold_vector_index_source = 12`
- `document_rag_index indexed_row_count = 12`
- `document_rag_index ready = true`
- `document-rag-app status = RUNNING`

## Contrato do runtime híbrido

O arquivo [runtime_query.py](/Users/flaviagaia/Documents/CV_FLAVIA_CODEX/document-rag-databricks/src/runtime_query.py) implementa três responsabilidades separadas:

### 1. Detecção de ambiente

Considera o runtime como Databricks real quando encontra pelo menos um destes sinais:

- `DATABRICKS_RUNTIME_VERSION`
- `DB_IS_JOB_CLUSTER`
- `DATABRICKS_HOST`
- `DATABRICKS_CLIENT_ID`
- `DATABRICKS_CLIENT_SECRET`
- `VECTOR_SEARCH_INDEX`

Essa escolha é deliberadamente redundante para cobrir diferenças entre:

- notebook/job runtime
- Databricks Apps
- execução local

### 2. Consulta principal

Executa `query_index` contra o índice vetorial configurado e normaliza o payload retornado.

### 3. Degradação controlada

Se a consulta vetorial falhar:

- captura a exceção
- registra o erro em `vector_error`
- faz fallback para o retrieval local
- devolve `runtime_mode = local_retrieval_fallback_after_vector_error`

Isso mantém o app utilizável sem esconder que a rota principal falhou.

## Variáveis e contrato do app

O app Databricks usa:

- `VECTOR_SEARCH_INDEX`

Valor padrão:

- `workspace.document_rag.document_rag_index`

Se a variável não existir, o código já usa esse mesmo índice como default.

Hoje o [app.yaml](/Users/flaviagaia/Documents/CV_FLAVIA_CODEX/document-rag-databricks/app.yaml) está assim, de forma propositalmente simples:

```yaml
command:
  - streamlit
  - run
  - streamlit_app.py
env:
  - name: VECTOR_SEARCH_INDEX
    value: workspace.document_rag.document_rag_index
```

Isso funciona bem para o MVP, mas existe uma distinção importante:

- `value`: injeta um nome fixo de índice
- `valueFrom`: seria a forma mais nativa quando o índice é adicionado como **App Resource** no Databricks Apps

Ou seja, a implementação atual já roda, mas o próximo endurecimento técnico natural é transformar o índice em um recurso formal do app e trocar:

- `value`

por:

- `valueFrom`

quando a configuração de recursos estiver plenamente modelada pelo Apps UI ou por bundle declarativo compatível.

## Fluxo operacional real

No runtime Databricks, o caminho esperado é:

1. documentos entram na `bronze`;
2. o chunking publica trechos na `silver`;
3. a tabela `gold` vira a fonte do índice vetorial;
4. o app pergunta ao índice vetorial;
5. o usuário recebe resposta grounded + evidência textual.

Num cenário de produção mais completo, esse fluxo normalmente seria expandido com:

- observabilidade de latência por consulta
- taxa de fallback
- monitoramento de refresh do índice
- versionamento de embedding
- avaliação offline de retrieval

Se a consulta vetorial falhar temporariamente:

1. a camada híbrida registra o erro;
2. faz fallback para a amostra local;
3. mantém a demo responsiva em vez de simplesmente quebrar.

## O que esse projeto demonstra

- arquitetura de RAG documental no estilo Databricks;
- separação clara entre ingestão, chunking e indexação;
- fluxo pronto para Vector Search;
- interface de consulta;
- ponte clara entre demo local e stack real de plataforma.
- preocupação com autenticação do app e permissões no Unity Catalog
- parsing robusto da SDK do Databricks
- fallback resiliente sem quebrar a experiência

## Leitura extremamente técnica

O projeto foi intencionalmente desenhado com duas realidades:

### 1. Runtime local reproduzível
Usado para:

- execução no GitHub;
- testes automatizados;
- demonstração rápida;
- validação de contrato.

Nesse modo:

- a base é uma amostra local;
- o retrieval é um fallback lexical determinístico;
- a resposta grounded é montada sobre os chunks retornados.

### 2. Runtime Databricks de produção
Pensado para:

- Delta tables no Unity Catalog;
- Change Data Feed para atualização incremental;
- Mosaic AI Vector Search;
- app com Streamlit no Databricks Apps;
- governança integrada.

O ganho desse desenho é que ele separa muito bem:

- **data plane**
- **vector retrieval plane**
- **application plane**

Também existe uma separação importante entre:

- **runtime path**
  - lógica que decide se a consulta vai para o Databricks real ou para o fallback local
- **retrieval path**
  - consulta vetorial quando o índice está disponível
- **resilience path**
  - fallback seguro quando o índice ainda não está pronto ou quando o ambiente não expõe a SDK/credenciais esperadas

Esse desenho é especialmente útil em ambientes enterprise porque reduz o risco de demos frágeis e melhora muito a transição entre desenvolvimento local e deployment real.

## Próximas evoluções técnicas naturais

Se a ideia for evoluir este projeto além do MVP atual, eu faria nesta ordem:

1. transformar o índice vetorial em **App Resource** nativo e usar `valueFrom` em vez de `value`
2. adicionar telemetria de:
   - latência da consulta vetorial
   - taxa de fallback
   - top-k hit behavior
3. incluir um benchmark explícito de retrieval com:
   - `Recall@k`
   - `MRR`
   - `Hit Rate@k`
4. armazenar avaliações em tabela Delta para observabilidade histórica
5. adicionar camada gerativa real acima da recuperação para fechar o ciclo de RAG completo

Isso deixa o projeto mais próximo de uma arquitetura real de enterprise RAG.

## Referências oficiais usadas no desenho

- [Mosaic AI Vector Search overview](https://docs.databricks.com/gcp/en/generative-ai/vector-search)
- [Create vector search endpoints and indexes](https://docs.databricks.com/gcp/generative-ai/create-query-vector-search)
- [Use Delta Lake change data feed](https://docs.databricks.com/en/delta/delta-change-data-feed.html)
- [Databricks Apps with Streamlit](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/tutorial-streamlit)
- [watsonxDocsQA dataset](https://huggingface.co/datasets/ibm-research/watsonxDocsQA)

## English

This project shows how to build a **document RAG architecture using the Databricks ecosystem end to end**.

It is structured around:

- Delta Lake ingestion
- bronze / silver / gold layering
- Unity Catalog governance
- Delta Change Data Feed for incremental refresh
- Mosaic AI Vector Search as the retrieval layer
- Streamlit as the Databricks App experience

The repository also includes a **local reproducible fallback** so the project remains runnable on GitHub without requiring a live Databricks workspace.
