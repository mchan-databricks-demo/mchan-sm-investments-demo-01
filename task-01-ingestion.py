# Databricks notebook source
spark.conf.set("spark.databricks.dataLineage.enabled", "true")
spark.conf.set("spark.databricks.cloudFiles.schemaInference.sampleSize.numFiles", 1000)
dbutils.widgets.text("environmentWidget","DEV","environment:")

# COMMAND ----------

environmentVariable = dbutils.widgets.get("environmentWidget")
print(f"The current environment is in {environmentVariable}")

# COMMAND ----------

display(dbutils.fs.ls("/mnt/mchan-demo-t1/demo-sm-investments-container/t0-raw-layer/"))

# COMMAND ----------

rawFilePath = "/mnt/mchan-demo-t1/demo-sm-investments-container/t0-raw-layer/"
 
cloudFilesConfigs = {
  "cloudFiles.format": "csv", 
  "cloudFiles.inferColumnTypes": "true",
  "cloudFiles.schemaLocation": "/mnt/mchan-demo-t1/demo-sm-investments-container/t0-ingestion-checkpoint/",
  "cloudFiles.schemaEvolutionMode": "addNewColumns", 
  "cloudFiles.maxFilesPerTrigger": 1000
}
 
df = spark.readStream\
            .format("cloudFiles")\
            .options(**cloudFilesConfigs)\
            .load(rawFilePath)

# COMMAND ----------

display(df1)

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
