import time

import pandas as pd
import requests
import os
import bz2
import re
import rdflib
import pickle
import yaml
import urllib.parse
from xml.etree import ElementTree

from rdfpandas.graph import to_dataframe
from sqlalchemy import create_engine
from retrying import retry
from ratelimit import limits, RateLimitException

# ttl file constants
MOIN_PREFIX = "moin:"
MOINO_CONNECTED_TO = "moino:connectedTo"
MOIN_URL_PREFIX = "http://moin-project.org/data/"
FIRST_LINE_DEFAUL_PREFIX = "@prefix : <#> .\n"

# namespaces
MOIN_NAMESPACE = rdflib.Namespace("http://moin-project.org/data/")
MOINO = rdflib.Namespace("http://moin-project.org/ontology/")
SCHEMA = rdflib.Namespace("http://schema.org/")
WD = rdflib.Namespace("http://www.wikidata.org/entity/")
WDT = rdflib.Namespace("http://www.wikidata.org/prop/direct/")
RDF_SCHEMA = rdflib.Namespace("http://www.w3.org/2000/01/rdf-schema/")

# other constants
UTF8 = "UTF-8"
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))


def store_dataframe_in_db(dataframe, table_description):
    engine = create_engine('sqlite:///train_connection_analysis.sqlite')
    dataframe.to_sql(table_description, con=engine, if_exists='replace')


@retry(stop_max_attempt_number=3, wait_fixed=600)
def download_and_decompress_ds1_file():
    datasource1_url = "https://mobilithek.info/mdp-api/files/aux/573356838940979200/moin-2022-05-02.1-20220502.131229-1.ttl.bz2"
    ds1_response = requests.get(datasource1_url, headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"})
    ds1_decompressed = bz2.decompress(ds1_response.content).decode(UTF8)
    print("data downloaded and decompressed")
    return ds1_decompressed


# Note: the datasource1 file seems to be corrupted.
# It is not parsable with rdflib without this preprocessing steps
# As the file corrupted file is downloaded directly from the Internet, its corrupted content
# might change and the pipeline will fail

# The cause for isodate.isoerror.ISO8601Error: Unrecognised ISO 8601 time format: '... 5 Min.:00' warnings is
# that in the ttl file, some time formats are corrupted
def preprocess_to_a_valid_parsable_ttl_file(ttl_file_content):
    print("started preprocessing ttl file")

    # remove to many << and >>
    ttl_file_content = ttl_file_content.replace('<<', '')
    ttl_file_content = ttl_file_content.replace('>>', ';')

    # rdflib requires a default prefix
    # see https://stackoverflow.com/questions/17616463/error-in-serializing-notation3-file-into-rdfxml-format-in-python
    ttl_file_content = FIRST_LINE_DEFAUL_PREFIX + ttl_file_content

    # escape urls
    ttl_file_content = re.sub(r'(?<!<)(https?://\S+)(?!>)', r'<\1>', ttl_file_content)

    # relate origins and destinations with their associated trips in the turtle file
    lines = ttl_file_content.splitlines()
    moin_to_be_closed = False
    prev_line = ''
    for i, line in enumerate(lines):
        stripped_line = line.strip()
        if moin_to_be_closed and prev_line.strip() == '] .':
            print("ends with: ", lines[i - 1])
            lines[i - 1] = lines[i - 1].replace('] .', ']] .')
            moin_to_be_closed = False
        elif re.match("^" + MOIN_PREFIX + ".+ " + MOINO_CONNECTED_TO, stripped_line) or \
                re.match('^<' + MOIN_URL_PREFIX + ".+> " + MOINO_CONNECTED_TO, stripped_line):
            print("starts with ", line)
            moin_to_be_closed = True
            lines[i] = line.replace(MOINO_CONNECTED_TO, MOINO_CONNECTED_TO + " [ " + MOINO_CONNECTED_TO)
        elif i == len(lines) - 1:
            print("last line")
            lines[i] = line.replace('] .', ']] .')
        prev_line = line
    ttl_file_content = "\n".join(lines)

    # remove blank lines
    pattern_for_blank_lines = re.compile(r'^\s*\n', re.MULTILINE)
    ttl_file_content = re.sub(pattern_for_blank_lines, '', ttl_file_content)

    print("brought content of datasource1 file into parsable ttl content")
    return ttl_file_content


def parse_ttl_file_to_rdf_graph():
    # prefixes are still not loaded correctly - so the URIs are not correct
    graph = rdflib.Graph()
    graph.bind("moin", MOIN_NAMESPACE)
    graph.bind("moino", MOINO)
    graph.bind("schema", SCHEMA)
    graph.bind("wdt", WDT)
    graph.bind("wd", WD)

    with open(SCRIPT_DIR + "/dataset.ttl",
              'r', encoding=UTF8) as file:
        graph.parse(file, format='turtle')
    print("graph was parsed successfully")
    return graph


def extract_towns_from_graph(graph):
    subjects_that_represent_towns = set()
    for s, p, o in graph:
        if s.startswith(MOIN_NAMESPACE):
            town_name = s.replace(MOIN_NAMESPACE, "")
            town_name = urllib.parse.unquote(town_name)
            subjects_that_represent_towns.add(town_name)
    print(subjects_that_represent_towns)
    print("number of listed towns in the dataset: ", len(subjects_that_represent_towns))

    # store towns separately for usage as input for the other dataset
    with open(SCRIPT_DIR + '/towns.pkl', 'wb') as f:
        pickle.dump(subjects_that_represent_towns, f)
    return subjects_that_represent_towns


def rearrange_graph_to_origin_destination_trip_information_format(graph):
    # problem: the information of connectedTo and the information about hasTrip triples are not related
    # the only relation is the position in the ttl file, but this doesn´t help for the query
    # the query returns the cartesian product of moin:Bremerhaven moino:connectedTo ?connectedTo with all existing trips
    # as the triples moin:Bremerhaven moino:connectedTo ?connectedTo; and moin:Bremerhaven moino:hasTrip [...]
    # have no relation except moin:Bremerhaven
    query_origin_destination_trips = """
        SELECT ?source ?connectedTo ?duration ?transportType
        WHERE {
          ?source moino:connectedTo [ moino:connectedTo ?connectedTo;
                  moino:hasTrip [
                    moino:duration ?duration ;
                    moino:transportType ?transportType
                  ]] .
          }
        ORDER BY ?source ?connectedTo
        """

    origin_destination_trips = graph.query(query_origin_destination_trips)
    for row in origin_destination_trips:
        print(row)
    # print("len qres ", len(query_origin_destination_trips))
    return origin_destination_trips


def extract_transform_load_datasource1():
    ds1_preprocessed = download_and_decompress_ds1_file()
    ds1_preprocessed_ttl_content = preprocess_to_a_valid_parsable_ttl_file(ds1_preprocessed)
    with open(SCRIPT_DIR + "/dataset.ttl",
              "w", encoding=UTF8) as file:
        file.write(ds1_preprocessed_ttl_content)
    print("ttl file extracted and ready for parsing")
    # parse the ttl file to a pandas data frame
    ds1_graph = parse_ttl_file_to_rdf_graph()
    towns = extract_towns_from_graph(ds1_graph)
    ds1_graph = rearrange_graph_to_origin_destination_trip_information_format(ds1_graph)
    ds1_df = to_dataframe(ds1_graph)
    # print("shape of ds1_df ", ds1_df.shape)
    store_dataframe_in_db(ds1_df, 'connection_time_graph')
    print("information of datasource1 loaded to database")
    keys = ds1_df.keys()
    print("keys: ", keys)
    print(ds1_df.head(2))
    print("number of listed towns in dataset 1: ", len(towns))


# DB API Service Plan: 60 requests per minute, 24/7 availability without support
@retry(stop_max_attempt_number=3, wait_fixed=600, retry_on_result=lambda result: 500 <= result.status_code < 600)
@limits(calls=60, period=60)
def make_api_call(url, headers):
    payload = {}
    response = None
    try:
        response = requests.request("GET", url, headers=headers, data=payload)
        print("call to api successful")
        return response
    except requests.exceptions.HTTPError as http_error:
        if response and response.status_code >= 500:
            print("call to api unsuccessful - server error occurred: ", response)
            raise http_error
        else:
            print("call to api unsuccessful - client error occurred: ", response)
            print("check your input for url ", url)
            raise http_error


def call_db_api(subdomain):
    ds2_url = "https://apis.deutschebahn.com/db-api-marketplace/apis/timetables/v1"
    url = ds2_url + subdomain
    print("request to db api with url: ", url)
    with open(SCRIPT_DIR + '/auth.yml', 'r') as auth_file:
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
        string_after_moin = name.split(MOIN_PREFIX)
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


def extract_transform_load_datasource2():
    with open(os.getcwd() + '/towns.pkl', 'rb') as f:
        towns = pickle.load(f)
    subdomain_stations_all = '/station/*'
    db_api_stations_all_response = call_db_api(subdomain_stations_all)
    towns_with_eva_numbers = extract_eva_numbers_from_stations_of_towns_that_are_also_part_of_the_graph(towns,
                                                                                                        db_api_stations_all_response.content)
    xml_dfs = []
    subdomain_timetable_changes = "/fchg/"
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
    print(ds2_df.head(2))


@retry(stop_max_attempt_number=3, wait_fixed=600)
def download_ds3_file_and_load_to_df():
    datasource3_url = "https://opendata.rhein-kreis-neuss.de/api/v2/catalog/datasets/rhein-kreis-neuss-ladesaulen-in-deutschland/exports/json"
    ds3_df = pd.read_csv(datasource3_url)
    return ds3_df


# experimental
def extract_transform_load_datasource3():
    ds3_df = download_ds3_file_and_load_to_df()
    print(ds3_df.head())
    store_dataframe_in_db(ds3_df, 'e_car_charging_stations')


# actual script
pd.options.display.max_colwidth = 100
pd.options.display.max_columns = 20

extract_transform_load_datasource1()
extract_transform_load_datasource2()

# extract_transform_load_datasource3()
# datasource3_url = "https://opendata.rhein-kreis-neuss.de/api/v2/catalog/datasets/rhein-kreis-neuss-ladesaulen-in-deutschland/exports/json"
# ds3_df = pd.read_csv(datasource3_url)
# ds3_df.head(2)
