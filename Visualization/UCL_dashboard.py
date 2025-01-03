from pyspark.sql import SparkSession
import psycopg2  
from pyspark.sql.functions import col, lit, coalesce, when, desc, asc
import matplotlib.pyplot as plt
import streamlit as st
import pandas as pd
import plotly.express as px


spark = SparkSession.builder \
    .appName("UCL dashboard") \
    .getOrCreate()

# PostgreSQL JDBC connection properties
jdbc_url = "jdbc:postgresql://localhost:5432/UCL"
properties = {
    "user": "postgres",
    "password": "Jeet@6291",
    "driver": "org.postgresql.Driver"
}

# Load data from PostgreSQL to Spark DataFrame
try:
    year="2425"
    table = "ucl_"+year
    df = spark.read.jdbc(url=jdbc_url, table=table, properties=properties)
    df.show()
    pandas_df = df.toPandas().reset_index(drop=True)
    print(pandas_df)

    
    # DASHBOARD BUIDING ###############################
    st.title("UCL 24-25 Player Performance Dashboard")
    
    clubs = pandas_df['Club'].unique()
    positions = pandas_df['Position'].unique()
    
    selected_club = st.sidebar.selectbox("Select Club", clubs)
    selected_position = st.sidebar.multiselect("Select Position", positions, default=positions)
    
    # filtered_df = pandas_df[(pandas_df['Club'] == selected_club) & (pandas_df['Position'].isin(selected_position))]
    # filtered_df = filtered_df.reset_index(drop=True)
    filtered_df = pandas_df[
        (pandas_df['Club'] == selected_club) & (pandas_df['Position'].isin(selected_position))
    ][['Player_Name', 'Club', 'Position', 'Season', 'Competition']]

    st.subheader(selected_club)


    
    # Display Player stats based on selected club and position
    st.write("Squad list", filtered_df)

    # # Scatter plot: Goals & Assists vs matches played 
    # fig1 = px.scatter(filtered_df, x="Match_Played", y="Total Goals", color="Player_name",
    #                 title="Goals vs  Matches Played")
    # st.plotly_chart(fig1)

    # fig2 = px.scatter(filtered_df, x="Match_Played", y="Assists", color="Player_name",
    #              title="Assists vs Matches Played")
    # st.plotly_chart(fig2)
    
    
    # Total goals scored by selected_club
    # total_goals = filtered_df['goals'].sum()

    # Display the total goals by each CLUB
    # st.markdown(f"### Total Goals Scored by {selected_club}: {total_goals}")
    
    
    # ---- New Plots for Top 10 Stats ---- #

    # 1. Top 10 Goal Scorers
    top_goal_scorers = pandas_df.nlargest(10, 'Total Goals')
    fig_goal_scorers = px.bar(top_goal_scorers, x="Player_Name", y="Total Goals", color="Club",
                            title="Top 10 Goal Scorers")
    st.plotly_chart(fig_goal_scorers)

    # 2. Top 10 Goal + Assist Providers
    pandas_df['Goals_Assists'] = pandas_df['Total Goals'] + pandas_df['Total Assists']
    top_contributors = pandas_df[pandas_df['Goals_Assists'] > 0]

    # Get the top 10 players by Goals + Assists
    top_contributors = top_contributors.nlargest(10, 'Goals_Assists')
    top_contributors = top_contributors.sort_values(by='Goals_Assists', ascending=False)

    fig_top_contributors = px.bar(
        top_contributors, 
        x="Player_Name", 
        y="Goals_Assists", 
        color="Club",
        title="Top 10 Goal + Assist Contributors",
        category_orders={"Player_Name": top_contributors["Player_Name"].tolist()}
    )
    st.plotly_chart(fig_top_contributors)
    # top_assist_providers = pandas_df.nlargest(10, 'assists')
    # fig_assist_providers = px.bar(top_assist_providers, x="player_name", y="assists", color="club",
    #                             title="Top 10 Assist Providers")
    # st.plotly_chart(fig_assist_providers)

    # 3. Top Clean Sheets 
    # top_clean_sheets = pandas_df.nlargest(10, 'cleansheets')
    # fig_clean_sheets = px.bar(top_clean_sheets, x="player_name", y="cleansheets", color="club",
    #                         title="Top 10 Clean Sheets")
    # st.plotly_chart(fig_clean_sheets)


except Exception as e:
    print(f"Error: {e}")