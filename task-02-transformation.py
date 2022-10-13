# Databricks notebook source
spark.conf.set("spark.databricks.dataLineage.enabled", "true")
dbutils.widgets.text("environmentWidget","dev","environment:")
environmentWidget = dbutils.widgets.get("environmentWidget")

# COMMAND ----------

# MAGIC %md
# MAGIC # SILVER layer

# COMMAND ----------

spark.sql(f"USE mchan_catalog.{environmentWidget}_mchan_sm_investments_db")

# COMMAND ----------

spark.sql(f"""
create or replace table mchan_catalog.{environmentWidget}_mchan_sm_investments_db.t2_silver_sales
as 
    with t1 as (
            select 
                date_trunc('MONTH', orderDate)::date as order_date, 
                category, 
                subCategory as sub_category, 
                round(sum(sales),0) AS total_sales
            from mchan_catalog.{environmentWidget}_mchan_sm_investments_db.t1_bronze_sales
            group by 1,2,3
            order by 1,2,4 desc
    ), 
        t2 as (
               select 
            year(order_date) as order_year, 
            month(order_date) as order_month,
            order_date,
            category, 
            sub_category, 
            total_sales,
            rank() over (
                partition by order_date, category 
                order by total_sales desc
            ) sales_rank
               from t1 
     )
    select * 
    from t2 
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### END OF STEP 2 
