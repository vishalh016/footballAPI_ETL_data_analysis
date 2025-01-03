# Football API ETL Data Analysis

## Overview
This project is an end-to-end ETL (Extract, Transform, Load) pipeline for football data analysis, leveraging APIs to fetch real-time football statistics. The pipeline processes data for historical analysis and displays it interactively using analytical tools. 

## Features
- **Data Extraction**: Fetch football data from APIs like football-data.org.
- **Data Transformation**: Process and normalize the extracted data using PySpark.
- **Data Loading**: Store transformed data in PostgreSQL for structured queries.
- **Data Analysis**: Perform in-depth analysis using Python.
- **Visualization**: Present insights interactively using Streamlit.

## Technology Stack
- **Python**: For scripting and data manipulation.
- **PySpark**: For data transformation and ETL.
- **PostgreSQL**: For structured data storage.
- **Streamlit**: For creating interactive dashboards.
- **APIs**: Football data fetching.
- **GitHub**: Version control and collaboration.

## Installation
1. Clone the repository:
   ```bash
   git clone https://github.com/vishalh016/footballAPI_ETL_data_analysis.git
   ```

2. Navigate to the project directory:
   ```bash
   cd footballAPI_ETL_data_analysis
   ```

3. Set up Python environment using pyenv:
   ```bash
   pyenv install 3.x.x
   pyenv virtualenv 3.x.x football-etl-env
   pyenv activate football-etl-env
   ```

4. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

5. Set up PostgreSQL:
   - Create a database for the project.
   - Update database credentials in the configuration file.
