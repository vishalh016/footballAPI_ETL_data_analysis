import pyspark
import os
import pandas as pd
import numpy as np
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, coalesce, when

class normalize():
    def normalize(self):
        # Create a SparkSession
        spark = SparkSession.builder \
            .appName("ReadData") \
            .getOrCreate()

        # Define the path to the data
        squads_path = "Data/Squads/uefa_champions_league_teams.csv"  
        scorers_path = "Data/Scorers/uefa_champions_league_scorers.csv"
        norm_path = "Data/NORMALIZE/Normalize.csv"
        temp = "Data/NORMALIZE/TEMP/"  

        # Read data into a DataFrame
        # For CSV format
        df_squads = spark.read.format("csv") \
            .option("header", "true").option("inferSchema", "true").load(squads_path)

        df_scorers = spark.read.format("csv") \
            .option("header", "true").option("inferSchema", "true").load(scorers_path)

        # Show the DataFrame's contents
        # df_squads.show()
        # df_scorers.show()

        # Print the DataFrame schema
        # df_squads.printSchema()

        # df_norm = (
        #     df_squads
        #     .join(
        #         df_scorers,
        #         (df_scorers["Player Name"] == df_squads["Player Name"]) & 
        #         (df_scorers["Team Name"] == df_squads["Team Name"]),
        #         how="left"
        #     )
        #     .select(
        #         df_squads["Player Name"].alias("Player"),       
        #         df_squads["Team Name"].alias("Team"),           
        #         df_squads["Position"].alias("Position"),        
        #         df_scorers["Goals"].alias("Total Goals"),       
        #         df_scorers["Assists"].alias("Total Assists")    
        #     )
        # )

        df_norm = df_squads.join(
            df_scorers,
            (df_scorers["Player Name"] == df_squads["Player Name"]) & 
            (df_scorers["Team Name"] == df_squads["Team Name"]),
            how="left"
        ).select(
            df_squads["Player Name"].alias("Player_Name"),       
            df_squads["Team Name"].alias("Club"),           
            df_squads["Position"].alias("Position"),        
            when(df_scorers["Goals"].isNull(), 0).otherwise(df_scorers["Goals"]).alias("Total Goals"),       
            when(df_scorers["Assists"].isNull(), 0).otherwise(df_scorers["Assists"]).alias("Total Assists"),    
            when(df_scorers["Penalties"].isNull(), 0).otherwise(df_scorers["Penalties"]).alias("Penalties"),
            when(df_scorers["Played Matches"].isNull(), 0).otherwise(df_scorers["Played Matches"]).alias("Match_Played"),
            lit("2024-2025").alias("Season"),
            lit("UCL").alias("Competition")
        )

        # df_norm = df_squads.join(df_scorers, ((df_scorers["Player Name"] == df_squads["Player Name"]) & (df_scorers["Team Name"] == df_squads["Team Name"])),how="left") \
        #     .select(
        #         df_squads["Player Name"].alias("Player_Name"),       
        #         df_squads["Team Name"].alias("Club"),           
        #         df_squads["Position"].alias("Position"),        
        #         df_scorers["Goals"].alias("Total Goals"),       
        #         df_scorers["Assists"].alias("Total Assists"),    
        #         df_scorers["Penalties"].alias("Penalties"),
        #         df_scorers["Played Matches"].alias("Match_Played"),
        #         lit("2024-2025").alias("Season"),
        #         lit("UCL").alias("Competition")
        #     )
        # df_norm.show()

        # df_norm = df_norm.withColumn("Total Goals", when(col("Total Goals").isNull(), 0).otherwise(col("Total Goals"))) \
        #                  .withColumn("Total Assists", when(col("Total Assists").isNull(), 0).otherwise(col("Total Assists"))) \
        #                  .withColumn("Penalties", when(col("Penalties").isNull(), 0).otherwise(col("Penalties"))) \
        #                  .withColumn("Match_Played", when(col("Match_Played").isNull(), 0).otherwise(col("Match_Played")))

        pandas_df = df_norm.toPandas()
        # pandas_df.fillna(0, inplace=True)
        # pandas_df = pandas_df.applymap(lambda x: re.sub(r'[^\w\d.\s]', '0', str(x)))

        print(pandas_df)
        pandas_df.to_csv('Data/NORMALIZE/API_output_NORM.csv', index=False)

        # Stop the SparkSession
        spark.stop()

