# Databricks notebook source
spark.conf.set("spark.databricks.dataLineage.enabled", "true")
spark.conf.set("spark.databricks.cloudFiles.schemaInference.sampleSize.numFiles", 1000)

# COMMAND ----------

dbutils.widgets.text("environmentWidget","dev","environment:")
environmentWidget = dbutils.widgets.get("environmentWidget")
print(f"The current environment is in {environmentWidget}")

# COMMAND ----------

display(dbutils.fs.ls("/mnt/mchan-demo-t1/demo-sm-investments-container/t0-raw-layer/"))

# COMMAND ----------

rawFilePath = "/mnt/mchan-demo-t1/demo-sm-investments-container/t0-raw-layer/"
 
cloudFilesConfigs = {
  "cloudFiles.format": "csv", 
  "cloudFiles.inferColumnTypes": "true",
  "cloudFiles.schemaLocation": "/mnt/mchan-demo-t1/demo-sm-investments-container/t0-ingestion-checkpoint/",
  "cloudFiles.schemaEvolutionMode": "rescue"
}
 
df = spark.readStream\
            .format("cloudFiles")\
            .options(**cloudFilesConfigs)\
            .load(rawFilePath)

# COMMAND ----------

(
    df.writeStream
            .format("delta")
            .option("checkpointLocation", "/mnt/mchan-demo-t1/demo-sm-investments-container/t0-ingestion-checkpoint/")
            .option("path", "/mnt/mchan-demo-t1/demo-sm-investments-container/t1-bronze-layer/") 
            .option("mergeSchema", "true")
            .partitionBy("state")
            .trigger(availableNow = True)
            .outputMode("append")
            .start()
)

# COMMAND ----------

spark.sql("USE CATALOG mchan_catalog") 
spark.sql(f"CREATE DATABASE IF NOT EXISTS {environmentWidget}_mchan_sm_investments_db")
spark.sql(f"USE {environmentWidget}_mchan_sm_investments_db") 
spark.sql("""
CREATE OR REPLACE TABLE t1_bronze_sales
AS 
SELECT * 
FROM DELTA.`/mnt/mchan-demo-t1/demo-sm-investments-container/t1-bronze-layer/`
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### -- END OF STEP 1 --
