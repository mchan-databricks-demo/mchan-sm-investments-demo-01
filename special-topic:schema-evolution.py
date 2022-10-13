# Databricks notebook source
# MAGIC %sql
# MAGIC SET spark.databricks.cloudFiles.schemaInference.sampleSize.numFiles = 1

# COMMAND ----------

df = spark.read\
          .format("csv")\
          .option("header","true")\
          .option("inferSchema", "true")\
          .load("/mnt/mchan-demo-t1/demo-schema-evolution/t0-raw-layer/")
display(df)

# COMMAND ----------

df.write.saveAsTable("mchan_schema_evolution.t1_bronze_layer_v3")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pattern 1:

# COMMAND ----------

rawFilePath = "/mnt/mchan-demo-t1/demo-schema-evolution/t0-raw-layer/"
 
cloudFilesConfigs = {
  "cloudFiles.format": "csv", 
  "cloudFiles.inferColumnTypes": "true",
  "cloudFiles.schemaLocation": "/mnt/mchan-demo-t1/demo-schema-evolution/t0-checkpoint/",
  "cloudFiles.schemaEvolutionMode": "rescue",
  "cloudFiles.rescuedDataColumn": "rescued_data"
}
 
df1 = spark.readStream\
            .format("cloudFiles")\
            .options(**cloudFilesConfigs)\
            .load(rawFilePath)

# COMMAND ----------

(
    df1.writeStream
       .option("checkpointLocation", "/mnt/mchan-demo-t1/demo-schema-evolution/t1-checkpoint/")
       .trigger(availableNow=True)
       .format("delta")
       .outputMode("append")
       .toTable("mchan_schema_evolution.t1_bronze_layer_v1")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pattern 2:

# COMMAND ----------

rawFilePath = "/mnt/mchan-demo-t1/demo-schema-evolution/t0-raw-layer/"
 
cloudFilesConfigs = {
  "cloudFiles.format": "csv", 
  "cloudFiles.inferColumnTypes": "true",
  "cloudFiles.schemaLocation": "/mnt/mchan-demo-t1/demo-schema-evolution/t0-checkpoint/",
  "cloudFiles.schemaEvolutionMode": "addnewColumns"
}
 
df2 = spark.readStream\
            .format("cloudFiles")\
            .options(**cloudFilesConfigs)\
            .load(rawFilePath)

(
    df2.writeStream
       .option("checkpointLocation", "/mnt/mchan-demo-t1/demo-schema-evolution/t1-checkpoint/")
       .trigger(availableNow = True)
       .format("delta")
       .outputMode("append")
       .toTable("mchan_schema_evolution.t1_bronze_layer_add_new_columns")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pattern 3:

# COMMAND ----------

rawFilePath = "/mnt/mchan-demo-t1/demo-schema-evolution/t0-raw-layer/"
 
cloudFilesConfigs = {
  "cloudFiles.format": "csv", 
  "cloudFiles.inferColumnTypes": "true",
  "cloudFiles.schemaLocation": "/mnt/mchan-demo-t1/demo-schema-evolution/t0-checkpoint/",
  "cloudFiles.schemaEvolutionMode": "failOnNewColumns"
}
 
df2 = spark.readStream\
            .format("cloudFiles")\
            .options(**cloudFilesConfigs)\
            .load(rawFilePath)

(
    df2.writeStream
       .option("checkpointLocation", "/mnt/mchan-demo-t1/demo-schema-evolution/t1-checkpoint/")
       .trigger(availableNow = True)
       .format("delta")
       .outputMode("append")
       .toTable("mchan_schema_evolution.t1_bronze_layer_fail_new_columns")
)
