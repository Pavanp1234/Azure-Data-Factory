# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC # CREATE FLAG PARAMETER

# COMMAND ----------

dbutils.widgets.text('incremental_flag','0')

# COMMAND ----------

incremental_flag = dbutils.widgets.get('incremental_flag')
print(incremental_flag) 

# COMMAND ----------

# MAGIC %md
# MAGIC # CREATING DIMENSION MODEL

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fetch Relative Column

# COMMAND ----------

df_src = spark.sql('''
select DISTINCT(Model_ID) as Model_ID, model_category from parquet.`abfss://secondsilver@pavansdatalake.dfs.core.windows.net/carsales`''')

# COMMAND ----------

df_src.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dim_model sink - Initial and incremental

# COMMAND ----------


if spark.catalog.tableExists('pavans_catalog.gold.dim_model') : 
 
 df_sink = spark.sql('''
 select dim_model_key, Model_ID, model_category                    
 from pavans_catalog.gold.dim_model
 ''')
else :
    
 df_sink = spark.sql('''
 select 1 as dim_model_key, Model_ID, model_category                    
 from parquet.`abfss://secondsilver@pavansdatalake.dfs.core.windows.net/carsales`
 where 1=0
 ''')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filtering New records and old records with joins

# COMMAND ----------

df_filter = df_src.join(df_sink, df_src['Model_ID'] == df_sink['Model_ID'], 'left').select(df_src['Model_ID'], df_src['model_category'], df_sink['dim_model_key'])

# COMMAND ----------

# MAGIC %md
# MAGIC **df_filter_old**
# MAGIC

# COMMAND ----------

df_filter_old = df_filter.filter(col('dim_model_key').isNotNull())

# COMMAND ----------

# MAGIC %md
# MAGIC **df_filter_new**

# COMMAND ----------

df_filter_new = df_filter.filter(col('dim_model_key').isNull()).select(df_src['Model_ID'], df_src['model_category'])

# COMMAND ----------

df_filter_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Surrogate Key

# COMMAND ----------

# MAGIC %md
# MAGIC **fetch the max surrogate key from existing table**

# COMMAND ----------

if (incremental_flag == '0') :
    max_value = 1
else :
    max_value_df = spark.sql("select max(dim_model_key) from pavans_catalog.thirdgold.dim_model")    
    max_value = max_value_df.collect()[0][0]

# COMMAND ----------

# MAGIC %md
# MAGIC **Create Surrgate key column and add the max surrogate key**

# COMMAND ----------

df_filter_new = df_filter_new.withColumn('dim_model_key',max_value+monotonically_increasing_id())

# COMMAND ----------

df_filter_new.display()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Final DF- df_filter_old + df_filter_new

# COMMAND ----------

df_final = df_filter_new.union(df_filter_old)

# COMMAND ----------

df_final.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **SCD TYPE -1 (UPSERT)** ##Where upsert means update+insert##

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

#Incremental Run
if spark.catalog.tableExists('pavans_catalog.gold.dim_model') :
    delta_tbl= DeltaTable.forPath(spark, "abfss://thirdgold@pavansdatalake.dfs.core.windows.net/dim_model")
     ##trg means target
    delta_tbl.alias("trg").merge(df_src.alias("src"),"trg.dim_model_key = src.dim_model_key")\
                             .whenMatchedUpdateAll()\
                             .whenNotMatchedInsertAll()\
                             .execute()
#Initial Run
else: 
    df_final.write.format("delta")\
         .mode("overwrite")\
         .option("path","abfss://thirdgold@pavansdatalake.dfs.core.windows.net/dim_model")\
         .saveAsTable("pavans_catalog.thirdgold.dim_model")    

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from pavans_catalog.thirdgold.dim_model