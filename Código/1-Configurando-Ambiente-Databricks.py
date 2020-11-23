# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Azure Databricks
# MAGIC > configuração do ambiente com o Azure Databricks

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC > nesse tutorial você iremos configurar o ambiente Databricks para receber os dados data.json

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### ETL com Databricks e Spark
# MAGIC 
# MAGIC O processo ** extrair, transformar, carregar (ETL) ** pega dados de uma ou mais fontes, transforma-os, normalmente adicionando estrutura e, em seguida, carrega-os em um banco de dados de destino.
# MAGIC 
# MAGIC Um trabalho ETL comum pega arquivos de log de um servidor da web, analisa campos pertinentes para que possam ser consultados prontamente e carrega-os em um banco de dados.
# MAGIC 
# MAGIC ETL pode parecer simples: aplicar estrutura aos dados para que fiquem na forma desejada. No entanto, a complexidade do ETL está nos detalhes. Os engenheiros de dados que criam pipelines ETL devem compreender e aplicar os seguintes conceitos: <br> <br>
# MAGIC 
# MAGIC * Otimizando formatos de dados e conexões
# MAGIC * Determinando o esquema ideal
# MAGIC * Tratamento de registros corrompidos
# MAGIC * Automatização de cargas de trabalho
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/eLearning/ETL-Part-1/ETL-overview.png" style="border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa"/>

# COMMAND ----------

# MAGIC %md
# MAGIC O Databricks File System (DBFS) é uma interface semelhante a HDFS para armazenamentos de dados em massa como o S3 da Amazon e o serviço de armazenamento Blob do Azure.
# MAGIC 
# MAGIC Passe o caminho `/FileStore/tables` em` spark.read.json` para acessar os dados armazenados no DBFS.

# COMMAND ----------

# MAGIC %md
# MAGIC Depois de importar o arquivo para o `Databricks File System (DBFS)` podemos ver o conteúdo pelo comando abaixo

# COMMAND ----------

# MAGIC %fs ls /FileStore/tables

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Imprimindo as primeiras linhad do arquivo JSON data.json

# COMMAND ----------

# MAGIC %fs head /FileStore/tables/data.json

# COMMAND ----------

# MAGIC %md
# MAGIC ###Schema Inference
# MAGIC 
# MAGIC Importe dados como um DataFrame e veja seu esquema com o método `printSchema()` DataFrame.

# COMMAND ----------

analyticsDF = spark.read.json("/FileStore/tables/data.json")
analyticsDF.printSchema()
