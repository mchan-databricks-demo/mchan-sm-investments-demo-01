# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingestion Pattern 1: Databricks reads DB Directly

# COMMAND ----------

hostURL = "jdbc:mysql://mchanmysqldb.mysql.database.azure.com:3306/mchan_superstore_db?useSSL=true&requireSSL=false"
databaseName = "mchan_superstore_db"
tableName = "orders"
userName = "mchanadmin@mchanmysqldb"
password = "Shuaige123#"


df = (
   spark.read
        .format("jdbc") 
        .option("url", f"{hostURL}") 
        .option("databaseName", f"{databaseName}")
        .option("dbTable", f"{tableName}") 
        .option("user", f"{userName}") 
        .option("password", f"{password}") 
        .option("ssl", True) 
        .load() 
)

display(df)

# COMMAND ----------

(
df.write
  .format("delta")
  .partitionBy("segment")
  .mode("overwrite")
  .save("/mnt/mchan-demo-t1/demo-sm-investments-container/t0-raw-mysql-database-export/")
)
