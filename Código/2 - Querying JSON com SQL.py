# Databricks notebook source
# MAGIC %md
# MAGIC # Querying JSON & Hierarchical Data com SQL
# MAGIC 
# MAGIC Apache Spark&trade; Databricks&reg; torna mais fácil trabalhar com dados hierárquicos, como registros JSON aninhados.

# COMMAND ----------

# MAGIC %fs head /FileStore/tables/data.json

# COMMAND ----------

# MAGIC %md
# MAGIC Para expor o arquivo JSON como uma tabela, use a tabela de criação de SQL padrão usando a sintaxe apresentada abaixo:

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS DatabricksAnalytics
# MAGIC   USING json
# MAGIC   OPTIONS (
# MAGIC     path "dbfs:/FileStore/tables/data.json",
# MAGIC     inferSchema "true"
# MAGIC   )

# COMMAND ----------

# MAGIC %md
# MAGIC Verificando a tabela criada com a função `DESCRIBE`

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DatabricksAnalytics

# COMMAND ----------

# MAGIC %md
# MAGIC Rodando uma query para verificar o conteúdo da tabela.
# MAGIC 
# MAGIC Aviso:
# MAGIC * A coluna `totals` é um array contendo multiplos valores aninhados

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT
# MAGIC channelGrouping,
# MAGIC customDimensions,
# MAGIC date,
# MAGIC device,
# MAGIC fullVisitorId,
# MAGIC geoNetwork,
# MAGIC hits,
# MAGIC socialEngagementType,
# MAGIC totals,
# MAGIC trafficSource,
# MAGIC visitId,
# MAGIC visitNumber,
# MAGIC visitStartTime
# MAGIC FROM DatabricksAnalytics

# COMMAND ----------

# MAGIC %md
# MAGIC Pense em dados aninhados como colunas dentro de colunas.
# MAGIC 
# MAGIC Por exemplo, observe a coluna `totals`.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT totals FROM DatabricksAnalytics

# COMMAND ----------

# MAGIC %md
# MAGIC Para retirar um subcampo específico vamos usar notação de "ponto"

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT
# MAGIC totals.bounces,
# MAGIC totals.hits,
# MAGIC totals.newVisits,
# MAGIC totals.pageviews,
# MAGIC --totals.screenviews, (NÃO EXISTE)
# MAGIC totals.sessionQualityDim,
# MAGIC --totals.timeOnScreen, (NÃO EXISTE)
# MAGIC totals.timeOnSite,
# MAGIC totals.totalTransactionRevenue,
# MAGIC totals.transactionRevenue,
# MAGIC totals.transactions,
# MAGIC --totals.UniqueScreenViews, (NÃO EXISTE)
# MAGIC totals.visits
# MAGIC FROM DatabricksAnalytics

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC CONTAGEM DE PAGEVIEWS

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT
# MAGIC sum(totals.pageviews) as CONTAGEM_PAGEVIEWS
# MAGIC FROM DatabricksAnalytics

# COMMAND ----------

# MAGIC %md
# MAGIC NÚMERO DE SESSÕES POR USUÁRIO

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT
# MAGIC visitId,
# MAGIC fullVisitorId,
# MAGIC SUM(visitNumber) AS NUMERO_DE_SESSOES
# MAGIC FROM DatabricksAnalytics
# MAGIC GROUP BY visitId, fullVisitorId
# MAGIC LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC SESSÕES DISTINTAS POR DATA

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT DISTINCT
# MAGIC date_format(to_date(date, 'yyyymmdd'), "MMM dd, yyyy") AS SESSAO_DATA,
# MAGIC fullVisitorId,
# MAGIC visitId
# MAGIC FROM DatabricksAnalytics
# MAGIC LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC MÉDIA DE DURAÇÃO DA SESSÃO POR DATA

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT
# MAGIC date_format(to_date(date, 'yyyymmdd'), "MMM dd, yyyy") AS SESSAO_DATA,
# MAGIC MEAN(totals.timeOnSite) AS MEDIA_SESSAO_DATA_SEGUNDOS
# MAGIC FROM DatabricksAnalytics
# MAGIC GROUP BY date_format(to_date(date, 'yyyymmdd'), "MMM dd, yyyy")
# MAGIC LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC SESSÕES DIÁRIAS POR TIPO DE BROWSER

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT
# MAGIC date_format(to_date(date, 'yyyymmdd'), "MMM dd, yyyy") AS SESSAO_DATA,
# MAGIC device.browser AS BROWSER,
# MAGIC count(device.browser) AS NUMERO_VEZES_USADO
# MAGIC FROM DatabricksAnalytics
# MAGIC GROUP BY date, device.browser
# MAGIC LIMIT 5
