import io

import pandas as pd
import requests
import bz2
import rdflib
from rdfpandas.graph import to_dataframe

datasource1_url = "https://mobilithek.info/mdp-api/files/aux/573356838940979200/moin-2022-05-02.1-20220502.131229-1.ttl.bz2"
ds1_response = requests.get(datasource1_url, headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"})
ds1_decompressed = bz2.decompress(ds1_response.content)
ds1_content = io.BytesIO(ds1_decompressed)

ds1_graph = rdflib.Graph()
ds1_graph.parse(source=ds1_content, format='ttl')

ds1_df = to_dataframe(ds1_graph)
print(ds1_df.head())
