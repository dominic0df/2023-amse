import time

import pandas as pd
import requests
import os
import bz2
import re
import rdflib
from urllib.parse import unquote
import matplotlib.pyplot as plt
import pickle
import yaml
from xml.etree import ElementTree

from rdfpandas.graph import to_dataframe
from sqlalchemy import create_engine
from retrying import retry
from ratelimit import limits, RateLimitException

MOIN = "moin:"
UTF8 = "UTF-8"


# Note: the datasource1 file seems to be corrupted.
# It is not parsable with rdflib without this preprocessing steps
# As the file corrupted file is downloaded directly from the Internet, its corrupted content
# might change and the pipeline will fail
def preprocess_to_a_valid_parsable_ttl_file(ttl_file_content):
    # remove to many << and >>
    ttl_file_content = ttl_file_content.replace('<<', '<')
    ttl_file_content = ttl_file_content.replace('>>', '>')

    # remove blank lines
    pattern_for_blank_lines = re.compile(r'^\s*\n', re.MULTILINE)
    ttl_file_content = re.sub(pattern_for_blank_lines, '', ttl_file_content)

    # remove leading < and trailing > from urls
    lines = ttl_file_content.splitlines()
    line_nr = 0
    header = ""
    # do not remove leading < and trailing > from the header
    while lines[line_nr].startswith('@prefix'):
        header = header + "\n" + lines[line_nr]
        line_nr += 1
    body = "\n".join(lines[line_nr:])
    # actual removal
    pattern_to_extract_url = re.compile(r'<?(https?://[^>\s]+)>?', re.MULTILINE)
    ttl_file_content = re.sub(pattern_to_extract_url, lambda m: m.group(1), body)
    ttl_file_content = header + "\n" + ttl_file_content

    # add a leading < and trailing > to a line if the url is de.wikipedia.org
    # pattern_to_find_wiki_urls = re.compile(r'(https?://de\.wikipedia\.org\S*)')
    # ttl_file_content = re.sub(pattern_to_find_wiki_urls, r'<\g<0>>', ttl_file_content)

    # add a leading < and trailing > if the line starts with an url without leading < and trailing >
    pattern_for_leading_urls = re.compile(r'^(https?://\S+)', re.MULTILINE)
    ttl_file_content = re.sub(pattern_for_leading_urls, r'<\1>', ttl_file_content)

    # add a leading < to a line if it starts with wd:
    pattern_for_wd = re.compile(r'^(wd:\S.*)$', re.MULTILINE)
    ttl_file_content = re.sub(pattern_for_wd, r'<\g<0>', ttl_file_content)

    # add a leading < and trailing > to a line with moin:
    pattern_for_moin = re.compile(r'^(moin:\S.*)$', re.MULTILINE)
    ttl_file_content = re.sub(pattern_for_moin, r'<\g<0>>', ttl_file_content)

    # add a leading < and trailing > to an url if the line begins with schema:about url
    pattern_for_schema_about_url = re.compile(r'(schema:about\s+)(\S+)', re.MULTILINE)
    ttl_file_content = re.sub(pattern_for_schema_about_url, r'\1<\2>', ttl_file_content)

    # add a leading < and trailing > to urls if the line begins with moino:connectedTo
    # match the line starting with "moino:connectedTo"
    pattern_line_with_moino_connected_to = re.compile(r'(^\s*moino:connectedTo)(.+?)(?=\n|$)', re.MULTILINE)
    ttl_file_content = re.sub(pattern_line_with_moino_connected_to, escape_urls, ttl_file_content)

    # decode URLs
    # ttl_file_content = unquote(ttl_file_content)

    print("brought content of datasource1 file into parsable ttl content")
    return ttl_file_content


def escape_urls(match):
    return match.group(1) + re.sub(r'http[^\s]+', r'<\g<0>>', match.group(2))


@retry(stop_max_attempt_number=3, wait_fixed=600)
def download_and_decompress_ds1_file():
    datasource1_url = "https://mobilithek.info/mdp-api/files/aux/573356838940979200/moin-2022-05-02.1-20220502.131229-1.ttl.bz2"
    ds1_response = requests.get(datasource1_url, headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"})
    ds1_decompressed = bz2.decompress(ds1_response.content).decode(UTF8)
    print("data downloaded and decompressed")
    return ds1_decompressed


def parse_ttl_file_to_rdf_graph():
    # prefixes are still not loaded correctly - so the URIs are not correct
    graph = rdflib.Graph()
    moin = rdflib.Namespace("http://moin-project.org/data/")
    graph.bind("moin", moin)
    moino = rdflib.Namespace("http://moin-project.org/ontology/")
    graph.bind("moino", moino)
    schema = rdflib.Namespace("http://schema.org/")
    graph.bind("schema", schema)
    wd = rdflib.Namespace("http://www.wikidata.org/entity/")
    graph.bind("wd", wd)
    wdt = rdflib.Namespace("http://www.wikidata.org/prop/direct/")
    graph.bind("wdt", wdt)

    with open(os.getcwd() + "/dataset.ttl",
              'r', encoding=UTF8) as file:
            graph.parse(file, format='turtle')
    print("graph was parsed successfully")
    return graph


def extract_towns_from_graph(graph):
    subjects = [s for s in graph.subjects() if str(s).startswith(MOIN)]
    subjects_that_represent_towns = dict.fromkeys(subjects)
    print(subjects_that_represent_towns)
    print("number of listed towns in the dataset: ", len(subjects_that_represent_towns))
    towns_in_graph = []
    for line in subjects_that_represent_towns:
        for word in line.split():
            town = word.split(MOIN)
            if len(town) > 1 and town[1]:
                # print(town[1])
                towns_in_graph.append(town[1])

    # store towns separately for usage as input for the other dataset
    with open(os.getcwd() + '/towns.pkl', 'wb') as f:
        pickle.dump(towns_in_graph, f)
    return towns_in_graph


def store_dataframe_in_db(dataframe, table_description):
    engine = create_engine('sqlite:///train_connection_analysis.sqlite')
    dataframe.to_sql(table_description, con=engine, if_exists='replace')


# DB API Service Plan: 60 requests per minute, 24/7 availability without support
@retry(stop_max_attempt_number=3, wait_fixed=600, retry_on_result=lambda result: 500 <= result.status_code < 600)
@limits(calls=60, period=60)
def make_api_call(url, headers):
    payload = {}
    response = None
    try:
        response = requests.request("GET", url, headers=headers, data=payload)
        print("call to db api successful")
        return response
    except requests.exceptions.HTTPError as http_error:
        if response and response.status_code >= 500:
            print("call to db api unsuccessful - server error occurred: ", response)
            raise http_error
        else:
            print("call to db api unsuccessful - client error occurred: ", response)
            print("check your input for url ", url)
            raise http_error


def call_db_api(subdomain):
    ds2_url = "https://apis.deutschebahn.com/db-api-marketplace/apis/timetables/v1"
    url = ds2_url + subdomain
    print("request to db api with url: ", url)
    with open(os.getcwd() + '/auth.yml', 'r') as auth_file:
        auth_config = yaml.load(auth_file, Loader=yaml.FullLoader)
    db_client_id = auth_config['auth']['datasource2']['clientId']
    db_client_secret = auth_config['auth']['datasource2']['clientSecret']
    headers = {
        'Accept': 'application/xml',
        'DB-Client-Id': db_client_id,
        'DB-API-Key': db_client_secret,
    }

    while True:
        try:
            return make_api_call(url, headers)
        except RateLimitException:
            print("too many api calls - sleep for 60 seconds")
            time.sleep(60)


def extract_eva_numbers_from_stations_of_towns_that_are_also_part_of_the_graph(towns, xml_content):
    tree = ElementTree.fromstring(xml_content)
    stations_and_its_eva_numbers = {}
    names = []
    for station in tree.findall('station'):
        name = station.get('name')
        string_after_moin = name.split(MOIN)
        words_after_moin = string_after_moin[0].split()
        # experiment with len(words_after_moin) > 1, other parts of hbf will occur
        if len(words_after_moin) == 1 or (len(words_after_moin) == 2 and words_after_moin[1] == "Hbf"):
            if words_after_moin[0] in towns:
                # print("station found: ", name)
                stations_and_its_eva_numbers[name] = station.get('eva')
                names.append(words_after_moin[0])
    print("eva_numbers of stations found that towns are also part of the graph: ", len(stations_and_its_eva_numbers))
    not_in_list = list(set(towns) - set(names))
    """
    the stations that are not found have special names that can not be differentiated easily from stations we are not interested in.
    F.e. Ratingen Ost is not found as the station of Ratingen, searching for Ost would also result in other <town Ost> stations.
    The project could be extended to multiple stations of a town, but we will see. 
    """
    print("towns from graph that were not found as stations ", not_in_list)
    return stations_and_its_eva_numbers


# actual script

ds1_preprocessed = download_and_decompress_ds1_file()
ds1_preprocessed_ttl_content = preprocess_to_a_valid_parsable_ttl_file(ds1_preprocessed)
with open(os.getcwd() + "/dataset.ttl",
          "w", encoding=UTF8) as file:
    file.write(ds1_preprocessed_ttl_content)

print("ttl file extracted and ready for parsing")
# parse the ttl file to a pandas data frame
ds1_graph = parse_ttl_file_to_rdf_graph()

towns = extract_towns_from_graph(ds1_graph)

pd.options.display.max_colwidth = 100
pd.options.display.max_columns = 20
ds1_df = to_dataframe(ds1_graph)
# print("shape of ds1_df ", ds1_df.shape)
store_dataframe_in_db(ds1_df, 'connection_time_graph')
print("information of datasource1 loaded to database")
keys = ds1_df.keys()
print("keys: ", keys)
print(ds1_df.head(2))

# with open(os.getcwd() + '/towns.pkl', 'rb') as f:
#    towns = pickle.load(f)

print("number of listed towns in dataset 1: ", len(towns))

subdomain_stations_all = '/station/*'
db_api_stations_all_response = call_db_api(subdomain_stations_all)
towns_with_eva_numbers = extract_eva_numbers_from_stations_of_towns_that_are_also_part_of_the_graph(towns,
                                                                                                    db_api_stations_all_response.content)

xml_dfs = []
subdomain_timetable_changes = "/fchg/"
i = 0
for eva_number in towns_with_eva_numbers.values():
    subdomain_timetable_changes_for_eva_number = subdomain_timetable_changes + eva_number
    db_api_station_eva_number_response = call_db_api(subdomain_timetable_changes_for_eva_number)
    xml_df_of_response = pd.read_xml(db_api_station_eva_number_response.content, xpath='.//s/m')
    xml_dfs.append(xml_df_of_response)
    # print(xml_df_of_response.shape)

ds2_df = pd.concat(xml_dfs, keys=towns_with_eva_numbers.keys())
store_dataframe_in_db(ds2_df, 'timetable_for_stations')
print("information of datasource 2 loaded to database")
# print("shape of ds2_df ", ds2_df.shape)
# print(ds2_df.head(5))
