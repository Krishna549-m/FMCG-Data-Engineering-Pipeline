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
dbutils.widgets.text("data_source", "gross_price", "Data_source")

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")

base_path = f's3://sportsbar-km1/{data_source}/*.csv'
print(base_path)

# COMMAND ----------

df = (
    spark.read
    .format("csv")
    .option("inferSchema", True)
    .option("header", True)
    .load(base_path)
    .withColumn("read_timestamp", F.current_timestamp())
    .select("*", "_metadata.file_name", "_metadata.file_size")
)

df.show(10)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

(
    df.write
    .format("delta")
    .mode("overwrite")
    .option("deLta.enableChangeDataFeed", True)
    .saveAsTable(f'{catalog}.{bronze_schema}.{data_source}')
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver Processing

# COMMAND ----------

df_bronze = spark.read.table(f'{catalog}.{bronze_schema}.{data_source}')
df_bronze.show(10)

# COMMAND ----------

df_bronze.select("month").distinct().show()

# COMMAND ----------

date_format = ["yyyy/MM/dd" , "dd/MM/yyyy" , "yyyy-MM-dd", "dd-MM-yyyy"]

df_silver = df_bronze.withColumn(
    "month" ,
    F.coalesce(
        F.try_to_date(F.col("month"), date_format[0]),
        F.try_to_date(F.col("month"), date_format[1]),
        F.try_to_date(F.col("month"), date_format[2]),
        F.try_to_date(F.col("month"), date_format[3]),
    )
)

df_silver.select("month").distinct().show()

# COMMAND ----------

df_silver.select("gross_price").distinct().show(10)

# COMMAND ----------

df_silver = df_silver.withColumn(
    "gross_price",
    F.when(F.col("gross_price").rlike(r'^-?\d+(\.\d+)?$'),
           F.when(F.col("gross_price").cast("double") < 0, F.col("gross_price").cast("double") * -1)
            .otherwise(F.col("gross_price").cast("double")))
    .otherwise(0.0)
)


# COMMAND ----------

df_silver.show()

# COMMAND ----------

df_products = spark.table(f"{catalog}.{silver_schema}.products")
df_products = df_products.select("product_id", "product_code")
df_products.show()

# COMMAND ----------

df_silver = df_silver.join(df_products, on = "product_id", how = "inner")
df_silver.show()

# COMMAND ----------

df_silver.write\
 .format("delta") \
 .option("delta.enableChangeDataFeed", "true") \
 .option("mergeSchema", "true") \
 .mode("overwrite") \
 .saveAsTable(f"{catalog}.{silver_schema}.{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold Processing

# COMMAND ----------

df_silver = spark.table(f"{catalog}.{silver_schema}.{data_source}")

# COMMAND ----------

df_gold = df_silver.select("product_code", "gross_price", "month")
df_gold.show(10)

# COMMAND ----------

df_gold.write\
 .format("delta") \
 .option("delta.enableChangeDataFeed", "true") \
 .mode("overwrite") \
 .saveAsTable(f"{catalog}.{gold_schema}.sb_dim_{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Merging 

# COMMAND ----------

df_gold = spark.table(f"{catalog}.{gold_schema}.sb_dim_{data_source}")
df_gold.show(10)

# COMMAND ----------

# use latest month price for price_inr
df_gold = (
    df_gold
    .withColumn("year", F.year(F.col("month")))
    .withColumn("is_zero", 
                F.when(F.col("gross_price") == 0.0, 1)
                .otherwise(0) 
    )
)
df_gold.show(10)

# COMMAND ----------

w = (
    Window
    .partitionBy("product_code" , "year")
    .orderBy(F.col("is_zero").asc(), F.col("month").desc())

)

df_gold = (
    df_gold
    .withColumn("rank", F.row_number().over(w))
    .filter(F.col("rank") == 1)
)

display(df_gold)

# COMMAND ----------

df_gold = df_gold.select("product_code", F.col("gross_price").alias("price_inr"), "year")
display(df_gold)

# COMMAND ----------

delta_table = DeltaTable.forName(spark, "fmcg.gold.dim_gross_price")


delta_table.alias("t").merge(
    source=df_gold.alias("s"),
    condition="t.product_code = s.product_code"
).whenMatchedUpdate(
    set={
        "price_inr": "s.price_inr",
        "year": "s.year"
    }
).whenNotMatchedInsert(
    values={
        "product_code": "s.product_code",
        "price_inr": "s.price_inr",
        "year": "s.year"
    }
).execute()

# COMMAND ----------

