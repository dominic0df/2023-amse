import pandas as pd
from sqlalchemy import create_engine

DATASOURCE_URL = "https://download-data.deutschebahn.com/static/datasets/haltestellen/D_Bahnhof_2020_alle.CSV"
TABLE_NAME = "trainstops"
VAlID_VERKEHR_VAlUES = ["FV", "RV", "nur DPN"]


def store_dataframe_in_db(dataframe, table_description):
    engine = create_engine('sqlite:///trainstops.sqlite')
    dataframe.to_sql(table_description, con=engine, if_exists='replace')


def preprocess_data(df):
    df = df.drop('Status', axis=1)
    df = df[df['Verkehr'].isin(VAlID_VERKEHR_VAlUES)]
    df['Laenge'] = df['Laenge'].str.replace(',', '.').astype(float)
    df['Breite'] = df['Breite'].str.replace(',', '.').astype(float)
    df = df[(df['Laenge'] >= -90) & (df['Laenge'] <= 90)]
    df = df[(df['Breite'] >= -90) & (df['Breite'] <= 90)]
    df = df.dropna()
    print(df.head(5))
    pattern_for_ifopt = r'^\S{2}:\d+:\d+(?:\d+)?$'
    df = df[df['IFOPT'].str.contains(pattern_for_ifopt)]
    print(df.head(5))
    return df


pd.options.display.max_colwidth = 100
pd.options.display.max_columns = 20
# dtype={'EVA_NR': int, 'DS100': str, 'IFOPT': str, 'NAME': str, 'Verkehr': str, 'Laenge': str, 'Breite': str, 'Betreiber_Name': str, 'Betreiber_Nr': int, 'Status': str}
df = pd.read_csv(DATASOURCE_URL, sep=";")
df = preprocess_data(df)
store_dataframe_in_db(df, TABLE_NAME)
