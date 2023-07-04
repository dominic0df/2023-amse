import zipfile

import pandas as pd
from urllib.request import urlretrieve
from sqlalchemy import create_engine

DATASOURCE_URL = "https://www.mowesta.com/data/measure/mowesta-dataset-20221107.zip"
ZIP_FILE_NAME = "data.zip"
CSV_FILE_NAME = "data.csv"
TABLE_NAME = "temperatures"
COLUMNS = ["Geraet", "Hersteller", "Model", "Monat", "Temperatur in °C (DWD)",
           "Batterietemperatur in °C", "Geraet aktiv"]
CUSTOM_COLUMN_NAMES_MAP = {"Temperatur in °C (DWD)": "Temperatur", "Batterietemperatur in °C": "Batterietemperatur"}

def extract_data():
    urlretrieve(DATASOURCE_URL, ZIP_FILE_NAME)
    with zipfile.ZipFile(ZIP_FILE_NAME) as zipped_data:
        with zipped_data.open(CSV_FILE_NAME) as csv_data:
            df = pd.read_csv(csv_data, sep=";", decimal=',', index_col=False,
                             usecols=COLUMNS)
            return df


def map_celsius_to_fahrenheit(temperature_in_celsius):
    return float((9 / 5) * temperature_in_celsius + 32.0)


def validate(df):
    df = df[df["Geraet"] > 0]
    df = df[df["Monat"] > 0]
    return df


def store_dataframe_in_db(dataframe, table_name):
    engine = create_engine('sqlite:///temperatures.sqlite')
    dataframe.to_sql(table_name, con=engine, if_exists='replace')


df = extract_data()
df = df.rename(columns=CUSTOM_COLUMN_NAMES_MAP)
df["Temperatur"] = df["Temperatur"].apply(map_celsius_to_fahrenheit)
df["Batterietemperatur"] = df["Batterietemperatur"].apply(map_celsius_to_fahrenheit)
df = validate(df)
store_dataframe_in_db(df, TABLE_NAME)
