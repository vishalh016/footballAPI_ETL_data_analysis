
# Football Data Processing and Visualization

This repository contains the code and resources for processing and visualizing football data, with a focus on the UEFA Champions League. The project involves the extraction, transformation, normalization, and visualization of player and team statistics.

## Folder Structure

```
D:.
│   ETL_GATEWAY.py          # Central script for orchestrating the ETL process
│   README.md               # Project documentation
│
├───Data                    # Contains raw and processed data files
│   │   archive.zip         # Compressed archive of raw data
│   │   attacking.csv       # Attacking statistics for players/teams
│   │   defending.csv       # Defending statistics for players/teams
│   │   disciplinary.csv    # Disciplinary data
│   │   distributon.csv     # Distribution-related statistics
│   │   goalkeeping.csv     # Goalkeeping statistics
│   │   goals.csv           # Goals data
│   │   key_stats.csv       # Key statistics
│   │
│   ├───EXTRACT            # Directory for raw extracted data
│   │       Local_output.csv # Extracted data for local source
│   │
│   ├───NORMALIZE          # Directory for normalized data
│   │       API_output_NORM.csv # Normalized data from API
│   │       Local_output_NORM.csv # Normalized data from local source
│   │
│   ├───Scorers            # Directory for player scoring data
│   │       uefa_champions_league_scorers.csv # Scorers data
│   │
│   └───Squads             # Directory for team squad data
│           uefa_champions_league_teams.csv # Team squad data
│
├───ETL                    # Contains Python scripts for ETL process
│       extract.py          # Script to extract data from source
│       load.py             # Script to load data into target system
│       normalize.py        # Script to normalize the data
│
├───LocalSource            # Scripts for processing local data sources
│       extract_LocalSource.py # Extract data from local source
│       Normalize_LocalSource.py # Normalize local data
│
└───Visualization          # Contains Python scripts for data visualization
        UCL_dashboard.py   # Script to create the UEFA Champions League dashboard
```

## Overview

This project is designed to manage and visualize football data, focusing on the UEFA Champions League. The data covers various aspects of player and team performance, including attacking, defending, goalkeeping, and more. The project includes the following steps:

1. **Data Extraction**: Extract raw data from various sources (local files, external APIs).
2. **Data Normalization**: Transform the raw data into a standardized format for analysis.
3. **Data Loading**: Load the transformed data into the target system for further analysis.
4. **Data Visualization**: Generate insightful visualizations, specifically focusing on key player and team statistics.

## Installation

To set up the project on your local machine:

1. Clone the repository:

    ```bash
    git clone https://github.com/yourusername/football-data-processing.git
    ```

2. Install dependencies: You may need to install required Python libraries. You can use the `requirements.txt` file or install dependencies manually:

    ```bash
    pip install -r requirements.txt
    ```

## Running the Project

### ETL Process

To initiate the extraction, normalization, and loading of data, you only need to run the `ETL_GATEWAY.py` script. This script will automatically call the necessary modules for extraction, normalization, and loading in sequence.

```bash
python ETL_GATEWAY.py
```

### Visualization

Once the data has been processed, you can generate the UEFA Champions League dashboard using the following script:

```bash
python Visualization/UCL_dashboard.py
```

This will create visualizations and display key statistics based on the processed data.

## Data Sources

- **Local Source**: Some data is sourced from local files.
- **UEFA Champions League Data**: Includes player and team performance data (scorers, teams, etc.).

## Contributing

If you'd like to contribute to this project:

1. Fork the repository.
2. Create a new branch (`git checkout -b feature-branch`).
3. Make your changes.
4. Commit your changes (`git commit -am 'Add new feature'`).
5. Push to your branch (`git push origin feature-branch`).
6. Open a pull request.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Data sourced from UEFA Champions League and other football data providers.
- Visualizations powered by libraries like `matplotlib`, `seaborn`, and `plotly`.
- Thanks to the open-source community for their contributions to data processing and visualization tools.
