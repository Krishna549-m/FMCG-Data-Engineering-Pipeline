# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC Create catalog if not exists fmgc; 
# MAGIC use catalog fmgc;

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists fmgc.gold;
# MAGIC create schema if not exists fmgc.bronze;
# MAGIC create schema if not exists fmgc.silver;

# COMMAND ----------

