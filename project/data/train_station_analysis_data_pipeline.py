import pandas as pd
import requests
import os
import bz2
import re
import rdflib
from rdfpandas.graph import to_dataframe


# Note: the file seems to be corrupted.
# It is not parsable with rdflib without this preprocessing steps
# As the file corrupted file is downloaded directly from the Internet, its corrupted content
# might change and the pipeline will fail
def preprocess_to_a_valid_parsable_ttl_file(ttl_file_content):
    # remove to many << and >>
    ttl_file_content = ttl_file_content.replace('<<', '<')
    ttl_file_content = ttl_file_content.replace('>>', '>')

    # remove blank lines
    ttl_file_content = re.sub(r'^\s*\n', '', ttl_file_content, flags=re.MULTILINE)

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
    pattern_to_extract_url = r'<?(http[s]?://[^>\s]+)>?'
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
    pattern_line_with_moino_connected_to = r'(^\s*moino:connectedTo)(.*?)(?=\n|$)'
    ttl_file_content = re.sub(pattern_line_with_moino_connected_to, escape_urls, ttl_file_content, flags=re.MULTILINE)

    print("brought content of file into parsable ttl content")
    return ttl_file_content


def escape_urls(match):
    return match.group(1) + re.sub(r'http[^\s]+', r'<\g<0>>', match.group(2))


datasource1_url = "https://mobilithek.info/mdp-api/files/aux/573356838940979200/moin-2022-05-02.1-20220502.131229-1.ttl.bz2"
ds1_response = requests.get(datasource1_url, headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"})
ds1_decompressed = bz2.decompress(ds1_response.content).decode('mbcs')
print("data downloaded and decompressed")

ds1_decompressed = preprocess_to_a_valid_parsable_ttl_file(ds1_decompressed)

with open(os.getcwd() + "/dataset.ttl",
          "w", encoding='ANSI') as file:
    file.write(ds1_decompressed)
print("ttl file extracted and ready for parsing")

# parse the ttl file to a pandas data frame
ds1_graph = rdflib.Graph()
ds1_graph.bind('moin', 'http://moin-project.org/data/')
with open(os.getcwd() + "/dataset.ttl",
          'r', encoding='ANSI') as f:
    ds1_graph.parse(f, format='turtle')

ds1_df = to_dataframe(ds1_graph)
print(ds1_df.head())
