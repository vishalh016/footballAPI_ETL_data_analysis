import requests
import csv
import json
from dotenv import load_dotenv
import os
import sys

load_dotenv()

# Define constants
SQUADS_URL = "https://api.football-data.org/v4/competitions/CL/teams"
SCORERS_URL = "https://api.football-data.org/v4/competitions/CL/scorers"

# GET API KEY FROM ENVIRONMENT
API_KEY = os.environ.get('FOOTBALLDATAORG_APIKEY')

HEADERS = {'X-Auth-Token': API_KEY}

# Function to fetch data from API
def get_data(url, headers):
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to fetch data. HTTP Status Code: {response.status_code}")
        return None

# Function to parse JSON and write CSV
def squad_list_to_csv(json_data):
    if not json_data:
        print("No data to write.")
        return "!!!...No Data to Write"

    # Extract relevant fields
    csv_file_path ="Data/Squads/uefa_champions_league_teams.csv"
    teams = json_data.get("teams", [])
    
    # Open CSV file for writing
    with open(csv_file_path, mode="w", newline="", encoding="utf-8") as csv_file:
        csv_writer = csv.writer(csv_file)
        
        # Write header
        headers = [
            "Team Name", "Short Name", "TLA", "Country", "Founded",
            "Club Colors", "Venue", "Website", "Coach Name", "Coach Nationality",
            "Player Name", "Position", "Date of Birth", "Nationality"
        ]
        csv_writer.writerow(headers)
        
        # Write rows
        for team in teams:
            coach = team.get("coach", {})
            squad = team.get("squad", [])
            
            # Write details for each player in the squad
            for player in squad:
                row = [
                    team.get("name", ""),
                    team.get("shortName", ""),
                    team.get("tla", ""),
                    team.get("area", {}).get("name", ""),
                    team.get("founded", ""),
                    team.get("clubColors", ""),
                    team.get("venue", ""),
                    team.get("website", ""),
                    coach.get("name", ""),
                    coach.get("nationality", ""),
                    player.get("name", ""),
                    player.get("position", ""),
                    player.get("dateOfBirth", ""),
                    player.get("nationality", "")
                ]
                csv_writer.writerow(row)
    print(f"Data has been written to {csv_file_path}")
 

def scorers_list_to_csv(json_data):
    if not json_data:
        print("No data to write.")
        return
	# Extract relevant fields
    scorers = json_data.get("scorers", [])
    csv_file_path = "Data/Scorers/uefa_champions_league_scorers.csv"
    
    # Open CSV file for writing
    with open(csv_file_path, mode="w", newline="", encoding="utf-8") as csv_file:
        csv_writer = csv.writer(csv_file)
        
        # Write header
        headers = [
            "Player Name", "First Name", "Last Name", "Date of Birth", "Nationality", 
            "Team Name", "Short Name", "TLA", "Played Matches", "Goals", 
            "Assists", "Penalties"
        ]
        csv_writer.writerow(headers)
        
        # Write rows
        for scorer in scorers:
            player = scorer.get("player", {})
            team = scorer.get("team", {})
            
            row = [
                player.get("name", ""),
                player.get("firstName", ""),
                player.get("lastName", ""),
                player.get("dateOfBirth", ""),
                player.get("nationality", ""),
                team.get("name", ""),
                team.get("shortName", ""),
                team.get("tla", ""),
                scorer.get("playedMatches", ""),
                scorer.get("goals", ""),
                scorer.get("assists", ""),
                scorer.get("penalties", "")
            ]
            csv_writer.writerow(row)
    print(f"Data has been written to {csv_file_path}")

# ---------------------------MAIN SCRIPT to fetch Data using Football API ---------------------------
class extract():
    def extract(self):
        # Fetch data from API & Save data to CSV
        squad_list_to_csv(get_data(SQUADS_URL, HEADERS))
        scorers_list_to_csv(get_data(SCORERS_URL, HEADERS))
