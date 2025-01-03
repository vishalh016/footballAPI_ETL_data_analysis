import sys
import re
from pyspark.sql import SparkSession
import psycopg2
import pandas as pd
from pyspark.sql.functions import col, lit, coalesce, when



# Initialize Spark session
spark = SparkSession.builder \
    .appName("Normalize local directory data") \
    .getOrCreate()

extr_path = "Data/EXTRACT/Local_output.csv" 
temp = "Data/NORMALIZE/TEMP/"  
norm_path = "Data/NORMALIZE/Normalize.csv" 
# Read CSV file
df_extr = spark.read.csv(extr_path, header=True, inferSchema=True)

df_norm = df_extr.select(
    col("player_name").alias("Player_Name"),
    col("club").alias("Club"),
    col("position").alias("Position"),
    col("goals").alias("Total Goals"),
    col("assists").alias("Total Assists"),
    col("penalties").alias("Penalties"),
    col("match_played").alias("Match_Played"),
    lit("2021-2022").alias("Season"),
    lit("UCL").alias("Competition")  
)

df_norm.show()

pandas_df = df_norm.toPandas()
# pandas_df.fillna(0, inplace=True)
# pandas_df = pandas_df.applymap(lambda x: re.sub(r'[^\w\d.\s]', '0', str(x)))

print(pandas_df)
pandas_df.to_csv('Data/NORMALIZE/Local_output_NORM.csv', index=False)

# Stop the SparkSession
spark.stop()