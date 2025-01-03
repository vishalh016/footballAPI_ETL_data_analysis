import sys
import re
from pyspark.sql import SparkSession
import psycopg2
import pandas as pd
from pyspark.sql.functions import col, lit, coalesce, when


# Initialize Spark session
spark = SparkSession.builder \
    .appName("Extract data from local directory") \
    .getOrCreate()

# Define CSV file path (update with your actual path)
# csv_file_path = "Data/attacking.csv"

# Read CSV file using Spark (efficient for larger datasets)
attacking_df = spark.read.csv("Data/attacking.csv", header=True, inferSchema=True)
attempts_df = spark.read.csv("Data/attempts.csv", header=True, inferSchema=True)
defense_df = spark.read.csv("Data/defending.csv", header=True, inferSchema=True)
discipline_df = spark.read.csv("Data/disciplinary.csv", header=True, inferSchema=True)
distribution_df = spark.read.csv("Data/distributon.csv", header=True, inferSchema=True)
goalkeeping_df = spark.read.csv("Data/goalkeeping.csv", header=True, inferSchema=True)
goals_df = spark.read.csv("Data/goals.csv", header=True, inferSchema=True)
key_stats_df = spark.read.csv("Data/key_stats.csv", header=True, inferSchema=True)

# Combining key stats with attacking
# player_name,club,position,minutes_played,match_played,goals,assists,distance_covered

combined_df = key_stats_df.join(attacking_df,
    (key_stats_df["player_name"] == attacking_df["player_name"]) &
    (key_stats_df["club"] == attacking_df["club"]) &
    (key_stats_df["position"] == attacking_df["position"]),
    how="left") \
.select(key_stats_df["*"], attacking_df["corner_taken"], attacking_df["offsides"], attacking_df["dribbles"])

combined_df = combined_df.join(attempts_df,
    (combined_df["player_name"] == attempts_df["player_name"]) &
    (combined_df["club"] == attempts_df["club"]) &
    (combined_df["position"] == attempts_df["position"]),
    how="left") \
.select(combined_df["*"],attempts_df["total_attempts"], attempts_df["on_target"], attempts_df["off_target"], attempts_df["blocked"])

combined_df = combined_df.join(goals_df,
    (combined_df["player_name"] == goals_df["player_name"]) &
    (combined_df["club"] == goals_df["club"]) &
    (combined_df["position"] == goals_df["position"]),
    how="left") \
.select(combined_df["*"], goals_df["right_foot"], goals_df["left_foot"], goals_df["headers"], goals_df["others"], goals_df["inside_area"],goals_df["outside_areas"],goals_df["penalties"])

combined_df = combined_df.join(distribution_df,
    (combined_df["player_name"] == distribution_df["player_name"]) &
    (combined_df["club"] == distribution_df["club"]) &
    (combined_df["position"] == distribution_df["position"]),
    how="left") \
.select(combined_df["*"], distribution_df["pass_accuracy"], distribution_df["pass_attempted"], distribution_df["pass_completed"], distribution_df["cross_accuracy"], distribution_df["cross_attempted"], distribution_df["cross_completed"], distribution_df["freekicks_taken"])


combined_df = combined_df.join(discipline_df,
    (combined_df["player_name"] == discipline_df["player_name"]) &
    (combined_df["club"] == discipline_df["club"]) &
    (combined_df["position"] == discipline_df["position"]),
    how="left") \
.select(combined_df["*"], discipline_df["fouls_committed"],discipline_df["fouls_suffered"],discipline_df["yellow"],discipline_df["red"]) 


combined_df = combined_df.join(defense_df,
    (combined_df["player_name"] == defense_df["player_name"]) &
    (combined_df["club"] == defense_df["club"]) &
    (combined_df["position"] == defense_df["position"]),
    how="left") \
.select(combined_df["*"], defense_df["balls_recoverd"],defense_df["tackles"],defense_df["tackles_won"],defense_df["tackles_lost"],defense_df["clearance_attempted"])


combined_df = combined_df.join(goalkeeping_df,
    (combined_df["player_name"] == goalkeeping_df["player_name"]) &
    (combined_df["club"] == goalkeeping_df["club"]) &
    (combined_df["position"] == goalkeeping_df["position"]),
    how="left") \
.select(combined_df["*"],goalkeeping_df["saved"], goalkeeping_df["conceded"], goalkeeping_df["saved_penalties"], goalkeeping_df["cleansheets"], goalkeeping_df["punches_made"]) 


# Replace null values with 0 for all columns in the DataFrame
combined_df = combined_df.select([when(col(c).isNull(), 0).otherwise(col(c)).alias(c) for c in combined_df.columns])
combined_df.show(10)
combined_df.printSchema()
# print(combined_df.columns)

# combined_df.write.mode("overwrite").option("nullValue", "NULL").csv("ucl_2122", header=True).option("sep", "|")

# combined_df.write.mode("overwrite").csv("ucl_2122", header=True)
# print("CSV file created successfully!")
# sys.exit(0)


# Convert Spark DataFrame to Pandas DataFrame
pandas_df = combined_df.toPandas()
# pandas_df.fillna(0, inplace=True)
pandas_df = pandas_df.applymap(lambda x: re.sub(r'[^\w\d.\s]', '0', str(x)))

print(pandas_df)
pandas_df.to_csv('Data/EXTRACT/Local_output.csv', index=False)
# sys.exitit(0)   

# Connect to PostgreSQL database
# try:
#     conn = psycopg2.connect(
#         host="localhost",
#         database="UCL",
#         user="postgres",
#         password="Jeet@6291"
#     )
#     cursor = conn.cursor()

#     # Truncate the existing data in the table
#     cursor.execute("TRUNCATE TABLE ucl2122 RESTART IDENTITY CASCADE")
#     conn.commit()
#     print("Data TRUNCATED successfully!")

#     # Assuming you have a Pandas DataFrame named `pandas_df` with the same columns as the `defending_stat` table
#     insert_query = """
#         INSERT INTO ucl2122 (
#             player_name, club, position, minutes_played, match_played, goals, assists, distance_covered,
#             corner_taken, offsides, dribbles, total_attempts, on_target, off_target, blocked, right_foot,
#             left_foot, headers, others, inside_area, outside_areas, penalties, pass_accuracy, pass_attempted,
#             pass_completed, cross_accuracy, cross_attempted, cross_completed, freekicks_taken, fouls_committed,
#             fouls_suffered, yellow, red, balls_recoverd, tackles, tackles_won, tackles_lost, clearance_attempted,
#             saved, conceded, saved_penalties, cleansheets, punches_made
#         ) VALUES (
#             %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
#             %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
#         );
#     """

#     # Create a list of tuples from the Pandas DataFrame
#     data_list = [tuple(row) for row in pandas_df.values]

#     # Execute the parameterized query with the data list
#     cursor.executemany(insert_query, data_list)
    
#     # Commit the changes
#     conn.commit()
#     print("Data inserted successfully!")
# except Exception as e:
#     print(f"Error loading data: {e}")
    

# finally:
#     if conn:
#         conn.close()

spark.stop()

