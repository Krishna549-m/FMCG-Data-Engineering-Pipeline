# Databricks notebook source
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from pyspark.sql.window import Window

# COMMAND ----------

bronze_schema = "bronze"
silver_schema = "silver"
gold_schema = "gold"

# COMMAND ----------

dbutils.widgets.text("catalog", "fmcg", "Catalog")
dbutils.widgets.text("data_source", "orders", "Data_source")

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")

base_path = f's3://sportsbar-km1/{data_source}'
landing_path = f"{base_path}/landing/"
processed_path = f"{base_path}/processed/"
print("Base Path: ", base_path)
print("Landing Path: ", landing_path)
print("Processed Path: ", processed_path)

# COMMAND ----------

df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(f"{landing_path}/*.csv")
    .withColumn("read_timestamp", F.current_timestamp())
    .select("*", "_metadata.file_name", "_metadata.file_size")
)

print("total rows: ", df.count())
df.show(5) 

# COMMAND ----------

df.write\
 .format("delta") \
 .option("delta.enableChangeDataFeed", "true") \
 .mode("overwrite") \
 .saveAsTable(f'{catalog}.{bronze_schema}.{data_source}')

# COMMAND ----------

df.printSchema()

# COMMAND ----------

display(df.limit(10))

# COMMAND ----------

files = dbutils.fs.ls(landing_path)
for file_info in files:
    dbutils.fs.mv(
        file_info.path,
        f"{processed_path}/{file_info.name}",
        True
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver Processing

# COMMAND ----------

df_silver = spark.read.table(f'{catalog}.{bronze_schema}.{data_source}')
display(df_silver.limit(10))

# COMMAND ----------

df_silver = df_silver.filter(F.col("order_qty").isNotNull())
print(df_silver.count())

# COMMAND ----------

df_silver = df_silver.withColumn(
    "customer_id",
    F.when(F.col("customer_id").rlike("^[0-9]+$"), F.col("customer_id"))
     .otherwise("999999")
     .cast("string")
)



# COMMAND ----------


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

display(df_silver.limit(10))

# COMMAND ----------

df_silver = df_silver.dropDuplicates(["order_id", "order_placement_date", "customer_id", "product_id", "order_qty"])

df_silver = df_silver.withColumn('product_id', F.col('product_id').cast('string'))

# COMMAND ----------

df_product = spark.read.table(f'{catalog}.{silver_schema}.products')
df_product = df_product.select('product_id', 'product_code')
df_silver = df_silver.join(df_product, on='product_id', how='inner')

display(df_silver.limit(10))

# COMMAND ----------

if not(spark.catalog.tableExists(f'{catalog}.{silver_schema}.{data_source}')):
    df_silver.write\
        .format("delta")\
        .option("mergeSchema", "true")\
        .option("delta.enableChangeDataFeed", "true")\
        .mode("overwrite")\
        .saveAsTable(f'{catalog}.{silver_schema}.{data_source}')
else:
    delta_table = DeltaTable.forName(spark, f"{catalog}.{silver_schema}.{data_source}")
    delta_table.alias("silver").merge(
        source = df_silver.alias("bronze"),
        condition = "silver.order_placement_date = bronze.order_placement_date AND silver.order_id = bronze.order_id AND silver.product_code = bronze.product_code AND silver.customer_id = bronze.customer_id"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold Processing 

# COMMAND ----------

df_gold = spark.read.table(f"{catalog}.{silver_schema}.{data_source}")
df_gold.show(10)

# COMMAND ----------

if not (spark.catalog.tableExists(f"{catalog}.{gold_schema}.{data_source}")):
    print("creating New Table")
    df_gold.write.format("delta").option(
        "delta.enableChangeDataFeed", "true"
    ).option("mergeSchema", "true").mode("overwrite").saveAsTable(f"{catalog}.{gold_schema}.sb_fact_{data_source}")
else:
    gold_delta = DeltaTable.forName(spark, f"{catalog}.{gold_schema}.sb_fact_{data_source}")
    gold_delta.alias("source").merge(df_gold.alias("gold"), "source.order_placement_date = gold.order_placement_date AND source.order_id = gold.order_id AND source.product_code = gold.product_code AND source.customer_code = gold.customer_code").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Merging with Parent company

# COMMAND ----------

df_child = spark.read.table(f'{catalog}.{gold_schema}.sb_fact_{data_source}')
display(df_child.limit(10))

# COMMAND ----------

df_child = (
    df_child
    .withColumn("order_placement_date",F.trunc(F.col("order_placement_date"), "month"))
    .groupBy(
        F.col("order_placement_date").alias("date"),
        "product_code",
        F.col("customer_id").alias("customer_code")
    )
    .agg(F.sum("order_qty").alias("sold_quantity"))
)

df_child.show(10)

# COMMAND ----------

df_child.count()

# COMMAND ----------

gold_parent_delta = DeltaTable.forName(spark, f"{catalog}.{gold_schema}.fact_orders")
gold_parent_delta.alias("parent_gold").merge(df_child.alias("child_gold"), "parent_gold.date = child_gold.date AND parent_gold.product_code = child_gold.product_code AND parent_gold.customer_code = child_gold.customer_code").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

