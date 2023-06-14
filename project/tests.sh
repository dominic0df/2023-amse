#!/bin/bash

python train_station_analysis_data_pipeline.py

set DB_FILE="data/train_connection_analysis.sqlite"
set TIMETABLE_FOR_STATIONS=timetable_for_stations
set CONNECTION_TIME_GRAPH=connection_time_graph

# Check if DB file exists
if [ -e data/train_connection_analysis.sqlite ]; then
    # Check if 'timetable_for_stations' table of datasource2 exists
    if sqlite3 "$DB_FILE" "SELECT name FROM sqlite_master WHERE type='table' AND name='timetable_for_stations';"; then
        echo "The 'timetable_for_stations' table exists."
    else
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