# Databricks notebook source
from databricks.sdk.runtime import spark, dbutils, display
CATALOGO, SCHEMA = "workspace", "anp"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOGO}.{SCHEMA}.raw")

for sub in ("zip", "csv"):
    dbutils.fs.mkdirs(f"/Volumes/{CATALOGO}/{SCHEMA}/raw/{sub}")
    
# COMMAND ----------
import sys, os
sys.path.append(os.path.abspath("../src"))
from anp_lakehouse.config import PASTA_ZIP, PASTA_CSV
from anp_lakehouse.arquivos import extrair_zips

print(extrair_zips(PASTA_ZIP, PASTA_CSV))

# COMMAND ----------
df = spark.read.option("header", True).option("sep", ";").csv(f"{PASTA_CSV}/")
df.printSchema()
print("linhas:", df.count())
display(df.groupBy("Produto").count())