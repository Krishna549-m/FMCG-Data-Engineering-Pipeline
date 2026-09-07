# Databricks notebook source
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

bronze_schema = "bronze"
silver_schema = "silver"
gold_schema = "gold"

# COMMAND ----------

dbutils.widgets.text("catalog", "fmcg", "Catalog")
dbutils.widgets.text("data_source", "orders", "Data Source")

catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")

# COMMAND ----------

base_path = f's3://sportsbar-km1/{data_source}'
landing_path = f"{base_path}/landing/"
processed_path = f"{base_path}/processed/"
print("Base Path: ", base_path)
print("Landing Path: ", landing_path)
print("Processed Path: ", processed_path)


# define the tables
bronze_table = f"{catalog}.{bronze_schema}.{data_source}"
silver_table = f"{catalog}.{silver_schema}.{data_source}"
gold_table = f"{catalog}.{gold_schema}.sb_fact_{data_source}"

# COMMAND ----------

df = (
    spark.read
    .option("header" , "true")
    .option("inferSchema", "true")
    .csv(f'{landing_path}/*csv')
    .withColumn("read_timestamp", F.current_timestamp())
    .select("*", "_metadata.file_name", "_metadata.file_size")
)

print("Total Rows: ", df.count())
df.show(5)

# COMMAND ----------

(
    df.write
    .format("delta")
    .option("delta.enableChangeDataFeed", "true")
    .mode("append")
    .saveAsTable(f"{catalog}.{bronze_schema}.{data_source}")
)   

# COMMAND ----------

# MAGIC %md
# MAGIC ### Staging table to process just the arrived incremenal data

# COMMAND ----------

(
    df.write
    .format("delta")
    .option("delta.enableChangeDataFeed", "true")
    .mode("overwrite")
    .saveAsTable(f"{catalog}.{bronze_schema}.staging_{data_source}")
)  

# COMMAND ----------

# MAGIC %md
# MAGIC ### Moving files from source to processed directory

# COMMAND ----------

files = dbutils.fs.ls(f'{landing_path}')
for file_info in files:
    dbutils.fs.mv(file_info.path, f'{processed_path}/{file_info.name}', True)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver Processing 

# COMMAND ----------

df_bronze = spark.read.table(f"{catalog}.{bronze_schema}.staging_{data_source}")
df_bronze.show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC Transformation

# COMMAND ----------

df_silver = df_bronze.filter(F.col("order_qty").isNotNull())
print(df_silver.count())


df_silver = df_silver.withColumn(
    "customer_id",
    F.when(F.col("customer_id").rlike("^[0-9]+$"), F.col("customer_id"))
     .otherwise("999999")
     .cast("string")
)


df_silver = df_silver.withColumn(
    "order_placement_date",
    F.regexp_replace(F.col("order_placement_date"), r"^[A-Za-z]+,\s*", "")
)

df_silver = df_silver.withColumn(
    "order_placement_date",
    F.coalesce(
        F.try_to_date("order_placement_date", "yyyy/MM/dd"),
        F.try_to_date("order_placement_date", "dd-MM-yyyy"),
        F.try_to_date("order_placement_date", "dd/MM/yyyy"),
        F.try_to_date("order_placement_date", "MMMM dd, yyyy"),
    )
)


df_silver = df_silver.dropDuplicates(["order_id", "order_placement_date", "customer_id", "product_id", "order_qty"])

df_silver = df_silver.withColumn('product_id', F.col('product_id').cast('string'))

df_product = spark.read.table(f'{catalog}.{silver_schema}.products')
df_product = df_product.select('product_id', 'product_code')
df_silver = df_silver.join(df_product, on='product_id', how='inner')

display(df_silver.limit(10))


# COMMAND ----------

if not(spark.catalog.tableExists(f'{catalog}.{silver_schema}.{data_source}')):
    (
        df_silver.write
        .format("delta")
        .option("delta.enableChangeDataFeed", "true")
        .option("mergeSchema","true")
        .mode("overwrite")
        .saveAsTable(f"{catalog}.{silver_schema}.{data_source}")
    )
else:
    delta_table = DeltaTable.forName(spark, f"{catalog}.{silver_schema}.{data_source}")
    (
        delta_table.alias("t")
        .merge(df_silver.alias("s"),
                "t.order_placement_date = s.order_placement_date AND s.order_id = t.order_id AND s.product_code = t.product_code AND s.customer_id = t.customer_id" 
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Staging table to process just the arrived incremenal data

# COMMAND ----------

(
    df_silver.write
    .format("delta")
    .option("delta.enableChangeDataFeed", "true")
    .mode("overwrite")
    .saveAsTable(f"{catalog}.{silver_schema}.staging_{data_source}")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold Processing

# COMMAND ----------

df_gold = spark.read.table(f"{catalog}.{silver_schema}.staging_{data_source}")
df_gold.show(10)

# COMMAND ----------

df_gold.count()

# COMMAND ----------

if not (spark.catalog.tableExists(f"{catalog}.{gold_schema}.sb_fact_{data_source}")):
    print("creating New Table")
    df_gold.write.format("delta").option(
        "delta.enableChangeDataFeed", "true"
    ).option("mergeSchema", "true").mode("overwrite").saveAsTable(f"{catalog}.{gold_schema}.sb_fact_{data_source}")
else:
    gold_delta = DeltaTable.forName(spark, f"{catalog}.{gold_schema}.sb_fact_{data_source}")
    gold_delta.alias("source").merge(df_gold.alias("gold"), "source.order_placement_date = gold.order_placement_date AND source.order_id = gold.order_id AND source.product_code = gold.product_code AND source.customer_id = gold.customer_id").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Merging with Parent

# COMMAND ----------

df_child = spark.read.table(f'{catalog}.{silver_schema}.staging_{data_source}')
display(df_child)

# COMMAND ----------

df_child = df_child.select(F.trunc("order_placement_date", "MM").alias("month")).distinct()
display(df_child)

df_child.createOrReplaceTempView("df_months")


# COMMAND ----------

df_parent = spark.sql(f"""
    SELECT order_placement_date, product_code, customer_id, order_qty
    FROM {catalog}.{gold_schema}.sb_fact_orders sbf
    INNER JOIN df_months m
        ON trunc(sbf.order_placement_date, 'MM') = m.month
""")


print("Total Rows: ", df_parent.count())
df_parent.show(10)

# COMMAND ----------


df_parent = (
    df_parent
    .withColumn("order_placement_date",F.trunc(F.col("order_placement_date"), "month"))
    .groupBy(
        F.col("order_placement_date").alias("date"),
        "product_code",
        F.col("customer_id").alias("customer_code")
    )
    .agg(F.sum("order_qty").alias("sold_quantity"))
)

df_parent.show(10)

# COMMAND ----------

df_parent.count()

# COMMAND ----------

gold_parent_delta = DeltaTable.forName(spark, f"{catalog}.{gold_schema}.fact_orders")
gold_parent_delta.alias("parent_gold").merge(df_parent.alias("child_gold"), "parent_gold.date = child_gold.date AND parent_gold.product_code = child_gold.product_code AND parent_gold.customer_code = child_gold.customer_code").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cleanup

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP table fmcg.bronze.staging_orders;
# MAGIC DROP TABLE fmcg.silver.staging_orders;