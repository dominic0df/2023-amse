import io
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

MOIN_PREFIX = "moin:"

# namespaces
MOIN = rdflib.Namespace("http://moin-project.org/data/")
MOINO = rdflib.Namespace("http://moin-project.org/ontology/")
SCHEMA = rdflib.Namespace("http://schema.org/")
WD = rdflib.Namespace("http://www.wikidata.org/entity/")
WDT = rdflib.Namespace("http://www.wikidata.org/prop/direct/")
RDF_SCHEMA = rdflib.Namespace("http://www.w3.org/2000/01/rdf-schema/")

UTF8 = "UTF-8"
FIRST_LINE_DEFAUL_PREFIX = "@prefix : <#> .\n"


# Note: the datasource1 file seems to be corrupted.
# It is not parsable with rdflib without this preprocessing steps
# As the file corrupted file is downloaded directly from the Internet, its corrupted content
# might change and the pipeline will fail
def preprocess_to_a_valid_parsable_ttl_file(ttl_file_content):
    print("started preprocessing ttl file")

    # remove to many << and >>
    ttl_file_content = ttl_file_content.replace('<<', '')
    ttl_file_content = ttl_file_content.replace('>>', ';')

    # rdflib requires a default prefix
    # see https://stackoverflow.com/questions/17616463/error-in-serializing-notation3-file-into-rdfxml-format-in-python
    ttl_file_content = FIRST_LINE_DEFAUL_PREFIX + ttl_file_content

    # pattern_for_moino_connected_to_escape = re.compile(r'(moin:[^ ;]+ moino:connectedTo)(.*?)(\.)', re.DOTALL)
    # ttl_file_content = pattern_for_moino_connected_to_escape.sub(r'\1 [ moino:connectedTo \2 ]\3', ttl_file_content)

    #pattern = re.compile(r'(moin:[^ ;]+ moino:connectedTo)(.*?)(\\]\.\n)', re.DOTALL)

    # Perform the substitutions using the compiled pattern
    #ttl_file_content = pattern.sub(r'\1 [ moino:connectedTo \2 ]] .\n', ttl_file_content)
    # time problem https://github.com/RDFLib/rdflib/issues/2148

    # substitute leading << and trailing >> by < and > from urls
    #lines = ttl_file_content.splitlines()
    #moin_to_be_closed = False
    #pattern_moino_connected_to = re.compile(r'(moino:connectedTo)\s+(moin:\w+)')
    #pattern_for_closing_bracket = re.compile(r'\](?=\.)?$')
    #for i in range(len(lines)):
    #    if moin_to_be_closed and lines[i].strip() == 0:
    #        lines[i - 1] = pattern_for_closing_bracket.sub(r']]', lines[i - 1])
     #       moin_to_be_closed = False
     #   elif lines[i].startswith(MOIN_PREFIX):
     #       moin_to_be_closed = True
     #       lines[i] = pattern_moino_connected_to.sub(r'\1 [ \1 \2', lines[i])
    #ttl_file_content = "\n".join(lines)

    # remove blank lines
    pattern_for_blank_lines = re.compile(r'^\s*\n', re.MULTILINE)
    ttl_file_content = re.sub(pattern_for_blank_lines, '', ttl_file_content)

    # actual_connected_to = ""
    # moino_has_trip = MOINO + "hasTrip"
    # offset = 0
    # for i in range(len(lines)):
    #    if lines[i].startswith(MOIN_PREFIX):
    #        words = lines[i].split()
    #        print(words)
    #        actual_connected_to = words[2]
    #    elif lines[i].startswith(moino_has_trip):
    #        connected_to = "moino:connectedTo " + actual_connected_to + " ;"
    #        lines.insert(i + offset + 1, connected_to)
    #       offset += 1
    # ttl_file_content = "\n".join(lines)
    # print(ttl_file_content)
    # print("offset ", offset)
    # line_nr = 0
    # header = ""
    # do not remove leading < and trailing > from the header
    # while lines[line_nr].startswith('@prefix'):
    #    header = header + "\n" + lines[line_nr]
    #    line_nr += 1
    # body = "\n".join(lines[line_nr:])
    # actual removal
    # pattern_to_extract_url = re.compile(r'<?(<https?://[^>\s]+>)>?', re.MULTILINE)
    # ttl_file_content = re.sub(pattern_to_extract_url, lambda m: m.group(1), body)
    # ttl_file_content = header + "\n" + ttl_file_content

    # ttl_file_content = ttl_file_content.replace('<<', '<')
    # ttl_file_content = ttl_file_content.replace('>>', '>')

    # add a leading < and trailing > to a line if the url is de.wikipedia.org
    # pattern_to_find_wiki_urls = re.compile(r'(https?://de\.wikipedia\.org\S*)')
    # ttl_file_content = re.sub(pattern_to_find_wiki_urls, r'<\g<0>>', ttl_file_content)

    # add a leading < and trailing > if the line starts with an url without leading < and trailing >
    # pattern_for_leading_urls = re.compile(r'^(https?://\S+)', re.MULTILINE)
    # ttl_file_content = re.sub(pattern_for_leading_urls, r'<\1>', ttl_file_content)

    # add a leading < to a line if it starts with wd:
    # pattern_for_wd = re.compile(r'^(wd:\S.*)$', re.MULTILINE)
    # ttl_file_content = re.sub(pattern_for_wd, r'<\g<0>', ttl_file_content)

    # add a leading < and trailing > to a line with moin:
    # pattern_for_moin = re.compile(r'^(moin:\S.*)$', re.MULTILINE)
    # ttl_file_content = re.sub(pattern_for_moin, r'<\g<0>>', ttl_file_content)

    # add a leading < and trailing > to an url if the line begins with schema:about url
    # pattern_for_schema_about_url = re.compile(r'(schema:about\s+)(\S+)', re.MULTILINE)
    # ttl_file_content = re.sub(pattern_for_schema_about_url, r'\1<\2>', ttl_file_content)

    # escape urls
    ttl_file_content = re.sub(r'(?<!<)(https?://\S+)(?!>)', r'<\1>', ttl_file_content)

    # add a leading < and trailing > to urls if the line begins with moino:connectedTo
    # match the line starting with "moino:connectedTo"
    # pattern_line_with_moino_connected_to = re.compile(r'(^\s*moino:connectedTo)(.+?)(?=\n|$)', re.MULTILINE)
    # ttl_file_content = re.sub(pattern_line_with_moino_connected_to, escape_urls, ttl_file_content)

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


@retry(stop_max_attempt_number=3, wait_fixed=600)
def download_ds3_file_and_load_to_df():
    datasource3_url = "https://opendata.rhein-kreis-neuss.de/api/v2/catalog/datasets/rhein-kreis-neuss-ladesaulen-in-deutschland/exports/json"
    ds3_df =  pd.read_csv(datasource3_url)
    return ds3_df


def parse_ttl_file_to_rdf_graph():
    # prefixes are still not loaded correctly - so the URIs are not correct
    graph = rdflib.Graph()
    graph.bind("moin", MOIN)
    graph.bind("moino", MOINO)
    graph.bind("schema", SCHEMA)
    graph.bind("wdt", WDT)
    graph.bind("wd", WD)

    with open(os.getcwd() + "/test.ttl",
              'r', encoding=UTF8) as file:
        graph.parse(file, format='turtle')
    print("graph was parsed successfully")
    return graph


def extract_towns_from_graph(graph):
    query = """
    SELECT ?connectedTo ?duration ?transportType
    WHERE {
      moin:Bremerhaven moino:connectedTo [
            moino:connectedTo ?connectedTo;
            moino:hasTrip [
                moino:duration ?duration ;
                moino:transportType ?transportType
            ]] .
    }
    ORDER BY ?source ?connectedTo
    """
    until_next = False

    qres = graph.query(query)
    for row in qres:
        print(row)
    print("len qres ", len(qres))
    # for s, p, o in graph.triples((MOIN.Bremerhaven, None, None)):
    # print(s, p, o)

    subjects = [s.rsplit('/', 1)[-1] for s in graph.namespaces()]
    subjects_that_represent_towns = dict.fromkeys(subjects)
    print(subjects_that_represent_towns)
    print("number of listed towns in the dataset: ", len(subjects_that_represent_towns))
    towns_in_graph = []
    for line in subjects_that_represent_towns:
        for word in line.split():
            town = word.split(MOIN_PREFIX)
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


def extract_transform_load_datasource1():
    ds1_preprocessed = download_and_decompress_ds1_file()
    ds1_preprocessed_ttl_content = preprocess_to_a_valid_parsable_ttl_file(ds1_preprocessed)
    with open(os.getcwd() + "/test.ttl",
              "w", encoding=UTF8) as file:
        file.write(ds1_preprocessed_ttl_content)
    print("ttl file extracted and ready for parsing")
    # parse the ttl file to a pandas data frame
    ds1_graph = parse_ttl_file_to_rdf_graph()
    towns = extract_towns_from_graph(ds1_graph)
    ds1_df = to_dataframe(ds1_graph)
    # print("shape of ds1_df ", ds1_df.shape)
    store_dataframe_in_db(ds1_df, 'connection_time_graph')
    print("information of datasource1 loaded to database")
    keys = ds1_df.keys()
    print("keys: ", keys)
    print(ds1_df.head(2))
    print("number of listed towns in dataset 1: ", len(towns))
    return towns


def extract_transform_load_datasource2(towns):
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
    # print(ds2_df.head(5))


def extract_transform_load_datasource3():
    ds3_df = download_ds3_file_and_load_to_df()
    print(ds3_df.head())
    store_dataframe_in_db(ds3_df, 'e_car_charging_stations')


# actual script
pd.options.display.max_colwidth = 100
pd.options.display.max_columns = 20
#towns = extract_transform_load_datasource1()
# with open(os.getcwd() + '/towns.pkl', 'rb') as f:
#    towns = pickle.load(f)
# extract_transform_load_datasource2(towns)
# extract_transform_load_datasource3()
#datasource3_url = "https://opendata.rhein-kreis-neuss.de/api/v2/catalog/datasets/rhein-kreis-neuss-ladesaulen-in-deutschland/exports/json"
#ds3_df = pd.read_csv(datasource3_url)
#ds3_df.head(2)

input_text = '''
moin:Bremerhaven moino:connectedTo moin:Marl ;
    moino:hasTrip  [ moino:duration       "PT322M"^^xsd:duration ;
                     moino:endTime        "15:50:00"^^xsd:time ;
                     moino:startTime      "10:28:00"^^xsd:time ;
                     moino:transportType  moino:train
                   ] ;
moino:hasTrip  [ moino:drivingDistance  288831 ].37934856815e0 ;
                         moino:duration         "PT9134.0S"^^xsd:duration ;
                         moino:route            "LINESTRING(8.586580000000001 53.551750000000006)"^^geo:wktLiteral ;
                         moino:transportType    moino:car
                       ]
    moino:hasTrip  [ moino:duration       "PT320M"^^xsd:duration ;
                     moino:endTime        "16:02:00"^^xsd:time ;
                     moino:startTime      "10:42:00"^^xsd:time ;
                     moino:transportType  moino:train
                   ] .
moin:Dortmund moino:connectedTo moin:Karlsruhe ;
    moino:hasTrip  [ moino:duration       "PT203M"^^xsd:duration ;
                     moino:endTime        "13:58:00"^^xsd:time ;
                     moino:startTime      "10:35:00"^^xsd:time ;
                     moino:transportType  moino:train
                   ] ;
    moino:hasTrip  [ moino:duration       "PT204M"^^xsd:duration ;
                     moino:endTime        "13:58:00"^^xsd:time ;
                     moino:startTime      "10:34:00"^^xsd:time ;
                     moino:transportType  moino:train
                   ] ;
    moino:hasTrip  [ moino:duration       "PT318M"^^xsd:duration ;
                     moino:endTime        "15:53:00"^^xsd:time ;
                     moino:startTime      "10:35:00"^^xsd:time ;
                     moino:transportType  moino:train
                   ] .
'''


# output_text = re.sub(r'(moin:Bremerhaven moino:connectedTo) (moin:Marl)', r'\1 [\2]', input_text)
# print(output_text)


# new_turtle_file = re.sub(r'\[.*?\]', add_brackets, input_text)
# print(new_turtle_file)
# new_turtle_file = re.sub(r'moin:Bremerhaven moino:connectedTo.*?(\.)', r'moin:Bremerhaven moino:connectedTo [\g<0>]', input_text, flags=re.DOTALL)
# new_turtle_file = re.sub(r'(moin:Bremerhaven moino:connectedTo)(.*?)(\.)', r'\1 [\2 ]\3', input_text, flags=re.DOTALL)
# new_turtle_file = re.sub(r'(moin:[^ ;]+ moino:connectedTo)(.*?)(\.)', r'\1 [ moino:connectedTo \2 ]\3', input_text,
#                         flags=re.DOTALL)
# print(new_turtle_file)

# Compile the regex pattern
pattern = re.compile(r'(moin:[^ ;]+ moino:connectedTo)(.*?)(] \.\n)', re.DOTALL)

# Perform the substitutions using the compiled pattern
new_turtle_file = pattern.sub(r'\1 [ moino:connectedTo \2 ]\3', input_text)
# print(new_turtle_file)

# turtle_file = "moin:Bremerhaven moino:connectedTo moin:Marl ; moino:hasTrip [ moino:duration \"PT322M\"^^xsd:duration ; moino:endTime \"15:50:00\"^^xsd:time ; moino:startTime \"10:28:00\"^^xsd:time ; moino:transportType moino:train ] ; moino:hasTrip [ moino:duration \"PT385M\"^^xsd:duration ; moino:endTime \"17:07:00\"^^xsd:time ; moino:startTime \"10:42:00\"^^xsd:time ; moino:transportType moino:train ] ; moino:hasTrip [ moino:duration \"PT320M\"^^xsd:duration ; moino:endTime \"16:02:00\"^^xsd:time ; moino:startTime \"10:42:00\"^^xsd:time ; moino:transportType moino:train ] ."

# new_turtle_file = re.sub(r"(moino\:connectedTo)", r"[ \1", turtle_file)
# new_turtle_file = re.sub(r"(\[.*\])", r"\1 ]", new_turtle_file)

# print(new_turtle_file)

# Multiline string
multiline_string = '''@prefix xsd:    <http://www.w3.org/2001/XMLSchema#> .

<< moin:Bremerhaven moino:connectedTo moin:Marl >>
        moino:hasTrip  [ moino:duration       "PT322M"^^xsd:duration ;
                         moino:endTime        "15:50:00"^^xsd:time ;
                         moino:startTime      "10:28:00"^^xsd:time ;
                         moino:transportType  moino:train
                       ] ;
        moino:hasTrip  [ moino:duration       "PT385M"^^xsd:duration ;
                         moino:endTime        "17:07:00"^^xsd:time ;
                         moino:startTime      "10:42:00"^^xsd:time ;
                         moino:transportType  moino:train
                       ] ;
        moino:hasTrip  [ moino:duration       "PT320M"^^xsd:duration ;
                         moino:endTime        "16:02:00"^^xsd:time ;
                         moino:startTime      "10:42:00"^^xsd:time ;
                         moino:transportType  moino:train
                       ] ;
        moino:hasTrip  [ moino:drivingDistance  288831.37934856815e0 ;
                         moino:duration         "PT9134.0S"^^xsd:duration ;
                         moino:route            "LINESTRING(8.586580000000001 53.551750000000006)"^^geo:wktLiteral ;
                         moino:transportType    moino:car
                       ] .

<< moin:Dortmund moino:connectedTo moin:Karlsruhe >>
'''

# substitute leading << and trailing >> by < and > from urls
lines = multiline_string.splitlines()
moin_to_be_closed = False
pattern_moino_connected_to = re.compile(r'(moino:connectedTo)\s+(moin:\w+)')
pattern_for_closing_bracket = re.compile(r'\](?=\.)?$')
for i in range(len(lines)):
    if moin_to_be_closed and lines[i].strip() == 0:
        lines[i - 1] = pattern_for_closing_bracket.sub(r']]', lines[i - 1])
        moin_to_be_closed = False
    elif lines[i].startswith(MOIN_PREFIX):
        moin_to_be_closed = True
        lines[i] = pattern_moino_connected_to.sub(r'\1 [ \1 \2', lines[i])
ttl_file_content = "\n".join(lines)

print(ttl_file_content)

def test_datasource2_pipeline():
    pass