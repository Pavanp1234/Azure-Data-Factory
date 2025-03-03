# Databricks notebook source
# MAGIC %md
# MAGIC # Data Reading
# MAGIC

# COMMAND ----------

df = spark.read.format('parquet')\
             .option('inferSchema',True)\
                 .load('abfss://firstbronze@pavansdatalake.dfs.core.windows.net/rawdata/')
           

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Data Transformation

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df = df.withColumn('model_category',split(col('Model_ID'),'-')[0])
df.display()

# COMMAND ----------

df.withColumn('Units_Sold',col('Units_Sold').cast(StringType())).printSchema()

# COMMAND ----------

df = df.withColumn('RevPerUnit',col('Revenue')/col('Units_Sold'))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # AD-HOC
# MAGIC

# COMMAND ----------

df.display()

# COMMAND ----------

display(df.groupBy('Year','BranchName').agg(sum('Units_Sold').alias('Total_Units')).sort('Year','Total_Units',ascending=[1,0]))

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Writing
# MAGIC

# COMMAND ----------

df.write.format('parquet')\
    .mode('overwrite')\
        .option('path','abfss://secondsilver@pavansdatalake.dfs.core.windows.net/carsales')\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC # Querying Silver Data

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM Parquet.`abfss://secondsilver@pavansdatalake.dfs.core.windows.net/carsales`

# COMMAND ----------

