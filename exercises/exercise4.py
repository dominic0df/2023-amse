import pandas as pd
import io
from urllib.request import urlopen
from sqlalchemy import create_engine
from zipfile import ZipFile

DATASOURCE_URL = "https://www.mowesta.com/data/measure/mowesta-dataset-20221107.zip"
CSV_FILE_NAME = "data.csv"
TABLE_NAME = "temperatures"


def map_celsius_to_fahrenheit(temperature_in_celsius):
    return float((9 / 5) * temperature_in_celsius + 32.0)


def store_dataframe_in_db(dataframe, table_name):
    engine = create_engine('sqlite:///temperatures.sqlite')
    dataframe.to_sql(table_name, con=engine, if_exists='replace')


csv_file = ZipFile(io.BytesIO(urlopen(DATASOURCE_URL).read()))
df = pd.read_csv(csv_file.open(CSV_FILE_NAME), sep=";", on_bad_lines='skip')
print(df.head(2))
df = df[
    ["Geraet", "Hersteller", "Model", "Monat", "Temperatur in °C (DWD)", "Batterietemperatur in °C", "Geraet aktiv"]]
df = df.rename(columns={"Temperatur in °C (DWD)": "Temperatur", "Batterietemperatur in °C": "Batterietemperatur"})
df["Temperatur"] = df["Temperatur"].astype(str).str.replace(',', '.').astype(float)
df["Temperatur"] = df["Temperatur"].apply(map_celsius_to_fahrenheit)
df["Batterietemperatur"] = df["Batterietemperatur"].astype(str).str.replace(',', '.').astype(float)
df["Batterietemperatur"] = df["Batterietemperatur"].apply(map_celsius_to_fahrenheit)
df = df[df["Geraet"].astype(str).str.isdigit() & df["Geraet"] > 0]
print(df.head(2))
store_dataframe_in_db(df, TABLE_NAME)
