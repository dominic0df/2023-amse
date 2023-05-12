# Automated Data Pipeline
## Features
The automated data pipeline in `train_station_analysis_data_pipeline.py` should automatically 
* Extract the data from Datasource1 and Datasource2
* Process the data to the required format
* Load the processed data into a SQLite database. 

The resulting dataset can be found in the `/data` folder locally.

## Requirements
### System requirements
* You are required to have Python3 and various packages (f.e. like pandas) installed. The project is developed/tested with Python 3.10.

### Credentials for Datasource2

To access data from Datasource2 a ClientId and ClientSecret for the DB Timetable API are required. To get both keys the Timetables API of the DB must be subscribed. For receiving the credentials do the following:
1. Create a DB API Marketplace Account [usage of a BahnID])
2. Create an Application at the DB Marketplace
3. Subscribe to the Timetables API
4. Copy the credentials and store them secretly (especially the API key!)

If you have a ClientId and ClientSecret, do the following:
1. In the `/data` folder, create a new file called `auth.yml`.
2. Paste the following content into the `auth.yml` file: 
    ```
    auth:
      datasource2:
        clientId: <your ClientId>
        clientSecret: <your CientSecret>
    ```
3. Replace the placeholder for the ClientId and ClientSecret with the credentials you gathered from the subscription to the DB Timetables API.