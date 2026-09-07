# Databricks notebook source
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

bronze_schema = "bronze"
silver_schema = "silver"
gold_schema = "gold"

# COMMAND ----------

dbutils.widgets.text("catalog","fmcg","Catalog")
dbutils.widgets.text("data_source","customers","Data_source")



# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")

base_path = f's3://sportsbar-km1/{data_source}/*.csv'
print(base_path)

# COMMAND ----------

df = (
        spark.read.format("csv")
            .option("header", True)
            .option("inferSchema",True)
            .load(base_path)
            .withColumn("read_timestamp", F.current_timestamp())
            .select("*", "_metadata.file_name", "_metadata.file_size")
    )

display(df.limit(10))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.write\
    .format("delta")\
    .option("delta.enableChangeDataFeed","true")\
    .mode("overwrite")\
    .saveAsTable(f"{catalog}.{bronze_schema}.{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver Processing

# COMMAND ----------

df_bronze = spark.sql(f'SELECT * FROM {catalog}.{bronze_schema}.{data_source};')
df_bronze.show(10)

# COMMAND ----------

df_duplicate = df_bronze.groupBy("customer_id").count().filter(F.col("count") > 1)
display(df_duplicate)

# COMMAND ----------

print("Rows before duplicates dropped: ",df_bronze.count())
df_silver = df_bronze.dropDuplicates(['customer_id'])
print("Rows after duplicates dropped: ",df_silver.count())

# COMMAND ----------

df_silver = df_silver.withColumn("customer_name",F.trim("customer_name"))
df_silver.show(10)

# COMMAND ----------

df_silver.select('city').distinct().show()


# COMMAND ----------

city_mapping = {
    'Bengaluruu' : 'Bengaluru',
    'Bengalore' : 'Bengaluru',

    'Hyderbad' : 'Hyderabad',
    'Hyderabadd': 'Hyderabad',

    'NewDheli' : 'New Delhi',
    'NewDelhee' : 'New Delhi',
    'NewDelhi' :  'New Delhi',
}

allowed = ['Bengaluru', 'Hyderabad', 'New Delhi']

df_silver = (
    df_silver
    .replace(city_mapping, subset=["city"])
    .withColumn(
        "city",
        F.when(F.col("city").isNull(),None)
        .when(F.col("city").isin(allowed), F.col("city"))
        .otherwise(None)
    )
)

# COMMAND ----------

df_silver.select('city').distinct().show()

# COMMAND ----------

df_silver.select('customer_name').distinct().show()

# COMMAND ----------

df_silver = (
    df_silver
    .withColumn(
        "customer_name",
        F.when(F.col("customer_name").isNull(),None)
        .otherwise(F.initcap("customer_name"))
    )
)

df_silver.select('customer_name').distinct().show()

# COMMAND ----------

df_silver.select("*").where(F.col("city").isNull()).show(truncate = False)

# COMMAND ----------

null_cus_name = ['Sprintx Nutrition', 'Zenathlete Foods', 'Primefuel Nutrition', 'Recovery Lane']
df_silver.where(F.col("customer_name").isin(null_cus_name)).show(truncate = False)

# COMMAND ----------

cus_null_fix = {
    789403 : "New Delhi",
    789420 : "Bengaluru",
    789521 : "Hyderabad",
    789603 : "Hyderabad"
} 

df_fix = spark.createDataFrame(cus_null_fix.items(), ["customer_id", "fixed_city"])
df_fix.printSchema()


# COMMAND ----------

df_silver = (
    df_silver
    .join(df_fix, "customer_id" , "left")
    .withColumn(
        "city",
        F.coalesce(F.col("city"), "fixed_city")
    )
    .drop("fixed_city")
)

display(df_silver)

# COMMAND ----------

null_cus_name = ['Sprintx Nutrition', 'Zenathlete Foods', 'Primefuel Nutrition', 'Recovery Lane']
df_silver.where(F.col("customer_name").isin(null_cus_name)).show(truncate = False)

# COMMAND ----------

df_silver = df_silver.withColumn("customer_id", F.col("customer_id").cast("string"))
df_silver.printSchema()

# COMMAND ----------

df_silver = (
    df_silver
    .withColumn("customer", F.concat(F.col("customer_name"), F.lit("-"), F.col('city')))
    .withColumn("market", F.lit("India"))
    .withColumn("platform", F.lit("Sport Bar"))
    .withColumn("channel", F.lit("Acquisition"))
)
display(df_silver)

# COMMAND ----------

df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("delta.enableChangeDataFeed","true")\
    .option("mergeSchema", "true")\
    .saveAsTable(f"{catalog}.{silver_schema}.{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold Processing

# COMMAND ----------

df_silver = spark.sql(f"SELECT * from {catalog}.{silver_schema}.{data_source};")

df_gold = df_silver.select("customer_id","customer_name","city","customer","market","platform","channel")

# COMMAND ----------

df_gold.write \
    .format("delta") \
    .mode("overwrite") \
    .option("delta.enableChangeDataFeed", "true")\
    .saveAsTable(f"{catalog}.{gold_schema}.sb_dim{data_source}")

# COMMAND ----------

delta_table = DeltaTable.forName(spark, "fmcg.gold.dim_customers")
child_table = spark.table("fmcg.gold.sb_dim_customers").select(
    F.col("customer_id").alias("customer_code"),
    "customer",
    "market",
    "platform",
    "channel"
)

# COMMAND ----------

delta_table.alias("t").merge(
    source = child_table.alias("s"),
    condition= "t.customer_code = s.customer_code"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

