# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Convertendo JSON para Apache Parquet
# MAGIC > recebendo os dados da aplicação ou de qualquer outra fonte de dados  
# MAGIC > convertendo para o tipo de arquivo mais otimizado para se trabalhar com Spark
# MAGIC <br>
# MAGIC   
# MAGIC <img width="500px" src ='https://brzluanmoreno.blob.core.windows.net/stgfiles/png_files/parquet-json.png'>
# MAGIC   
# MAGIC <br>
# MAGIC > informações sobre o **Apache Parquet**  
# MAGIC > https://parquet.apache.org/

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <img width="50px" src="https://brzluanmoreno.blob.core.windows.net/stgfiles/png_files/apache-parquet.png">
# MAGIC 
# MAGIC > o **apache parquet** é um tipo de arquivo colunar que acelera e otimiza arquivos no formato *csv* ou *json*  
# MAGIC > esse modelo colunar faz integração com quase todas os frameworks de processamento de dados de big data  
# MAGIC > é extremamente utilizado juntamente com o apache spark

# COMMAND ----------

# MAGIC %md
# MAGIC Listando o arquivo [JSON]

# COMMAND ----------

# MAGIC %fs ls "dbfs:/FileStore/tables/data.json"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC > os arquivos dentro do DBFS estão no format JSON, como explicado anteriormente para que você possa ganhar em otimização  
# MAGIC > é extremamente importante que você sempre realize a conversão dos tipos de dados, se estiver trabalhando com spark então definitivamente   
# MAGIC > utilize apache parquet

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC > o Apache Spark possibilita diversos tipos de leituras diferentes, segue opções disponíveis utilizando = **(spark.read.)**
# MAGIC <br>
# MAGIC 
# MAGIC * Parquet
# MAGIC * JSON
# MAGIC * CSV
# MAGIC * ORC
# MAGIC * JDBC

# COMMAND ----------

# MAGIC %md
# MAGIC Lendo Arquivos no Format JSON

# COMMAND ----------

df_analytics = spark.read.json("dbfs:/FileStore/tables/data.json")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC > os dados são lidos pela engine e colocados dentro de um **dataframe** que é uma estrutura organizada em colunas, como se fosse uma tabela de banco de dados  
# MAGIC > porém distribuídas entre nós de computação
# MAGIC 
# MAGIC > **spark.read.json** automaticamente infere o schema do arquivo json utilizando  
# MAGIC > também disponível com **sql**  
# MAGIC > https://spark.apache.org/docs/latest/sql-data-sources-json.html
# MAGIC 
# MAGIC <br>
# MAGIC 
# MAGIC > CREATE TEMPORARY VIEW jsonTable  
# MAGIC   USING org.apache.spark.sql.json  
# MAGIC   OPTIONS (path "examples/src/main/resources/people.json")  
# MAGIC 
# MAGIC > SELECT * FROM jsonTable

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Lendo DataFrame = analytics

# COMMAND ----------

display(df_analytics)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC > convertendo os dataframes para o apache parquet  
# MAGIC > algumas vantagens do **Apache Parquet**
# MAGIC 
# MAGIC > **1** - aceita estrutura complexas para arquivo  
# MAGIC > **2** - eficiente para compressão e encoding de schema  
# MAGIC > **3** - permite menos acesso ao disco e consultas mais rápidas no arquivo  
# MAGIC > **4** - menos overhead de I/O  
# MAGIC > **5** - aumento na velocidade de scan  

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Lendo DataFrames e Escrevendo em parquet no DBFS

# COMMAND ----------

df_analytics.write.mode("overwrite").parquet("dbfs:/FileStore/tables/data.parquet")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Listando os arquivos convertidos de json para parquet no DBFS

# COMMAND ----------

# MAGIC %fs ls "dbfs:/FileStore/tables/"

# COMMAND ----------

# MAGIC %md
# MAGIC Carregando DataFrame com [spark.read.parquet]

# COMMAND ----------

ds_analytics = spark.read.parquet("dbfs:/FileStore/tables/data.parquet")
display(ds_analytics)

# COMMAND ----------

# MAGIC %md
# MAGIC Analisando com PySpark - Select das Colunas

# COMMAND ----------

ds_analytics_select = ds_analytics.select(['channelGrouping',
'customDimensions',
'date',
'device',
'fullVisitorId',
'geoNetwork',
'hits',
'socialEngagementType',
'totals',
'trafficSource',
'visitId',
'visitNumber',
'visitStartTime'])
display(ds_analytics_select)

# COMMAND ----------

# MAGIC %md
# MAGIC CONTAGEM DE PAGEVIEWS

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import col

display(ds_analytics.select('totals.pageviews') \
        .withColumn('pageviews', col('pageviews').cast('Integer')) \
        .groupby() \
        .sum())

# COMMAND ----------

# MAGIC %md
# MAGIC NÚMERO DE SESSÕES POR USUÁRIO

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import col

ds_analytics_sessoes_user = ds_analytics.select('visitId', 'fullVisitorId','visitNumber') \
  .withColumn('visitNumber', col('visitNumber').cast('Integer')) \
  .groupby('visitId','fullVisitorId') \
  .sum('visitNumber')

display(ds_analytics_sessoes_user.withColumnRenamed('sum(visitNumber)', 'SESSOES_POR_USUARIO'))
  

# COMMAND ----------

# MAGIC %md
# MAGIC SESSÕES DISTINTAS POR DATA

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import col, to_date

ds_analytics_sessoes_data = ds_analytics.select('date', 'visitId', 'fullVisitorId') \
  .withColumn('date', to_date('date', 'yyyyMMdd')) \
  .distinct() \
  .orderBy('date')

display(ds_analytics_sessoes_data)

# COMMAND ----------

# MAGIC %md
# MAGIC MÉDIA DE DURAÇÃO DA SESSÃO POR DATA

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql import functions as f

ds_analytics_sessoes_media = ds_analytics.select('date', 'totals.timeOnSite') \
  .withColumn('date', to_date('date', 'yyyyMMdd')) \
  .withColumn('timeOnSite', col('timeOnSite').cast('Integer')) \
  .groupBy('date') \
  .agg(f.avg('timeOnSite')) \
  .orderBy('date')

display(ds_analytics_sessoes_media.withColumnRenamed('avg(timeOnSite)', 'MEDIA_SESSAO_DATA'))

# COMMAND ----------

# MAGIC %md
# MAGIC SESSÕES DIÁRIAS POR TIPO DE BROWSER

# COMMAND ----------



ds_analytics_sessoes_sql = spark.sql('''SELECT
date_format(to_date(date, 'yyyymmdd'), "MMM dd, yyyy") AS SESSAO_DATA,
device.browser AS BROWSER,
count(device.browser) AS NUMERO_VEZES_USADO
FROM DatabricksAnalytics
GROUP BY date, device.browser
ORDER BY date
LIMIT 5''')

ds_analytics_sessoes_sql.show()


# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import col, to_date

ds_analytics_sessoes_browser = ds_analytics \
  .groupBy('date', 'device.browser') \
  .agg({'device.browser':'count'}) \
  .orderBy('date')
  
display(ds_analytics_sessoes_browser)

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import count

ds_analytics_sessoes_browser = ds_analytics \
  .groupBy('date', 'device.browser') \
  .agg(count('device.browser').alias('browser'))\
  .orderBy('date')

ds_analytics_sessoes_browser.show()
