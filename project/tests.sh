#!/bin/bash

# remove main/project/ from path to run script locally, this additional path is required for executing the pipeline with GitHub Actions
python main/project/train_station_analysis_data_pipeline.py

# remove main/project/ from path to run script locally, this additional path is required for executing the pipeline with GitHub Actions
set DB_FILE="main/project/data/train_connection_analysis.sqlite"
set TIMETABLE_FOR_STATIONS=timetable_for_stations
set CONNECTION_TIME_GRAPH=connection_time_graph

# Check if DB file exists
# remove main/project/ from path to run script locally, this additional path is required for executing the pipeline with GitHub Actions
if [ -e main/project/data/train_connection_analysis.sqlite ]; then
    # Check if 'timetable_for_stations' table of datasource2 exists
    if sqlite3 "$DB_FILE" "SELECT name FROM sqlite_master WHERE type='table' AND name='timetable_for_stations';"; then
        echo "The 'timetable_for_stations' table exists."
    else
        # For calling the DB API an API Key and Client ID is required. Therefore a file auth.yml is required that includes this information
        # The timetable_for_stations query will be always false in GitHub Actions as this file cannot be provided,
        # so parts of the pipeline fail as credentials are missing
        echo "The 'timetable_for_stations' table does not exist."
    fi

    # Check if 'connection_time_graph' table of datasource1 exists
    if sqlite3 "$DB_FILE" "SELECT name FROM sqlite_master WHERE type='table' AND name='connection_time_graph';"; then
        echo "The 'connection_time_graph' table exists."
    else
        echo "The 'connection_time_graph' table does not exist."
    fi
else
    echo "File does not exist"
fi