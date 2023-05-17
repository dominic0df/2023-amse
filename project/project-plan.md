# Project Plan

## Summary

<!-- Describe your data science project in max. 5 sentences. -->
This project analyses the current state of mobility in Germany and attempts to identify regions where the expansion or improvement of existing rail connections is worth prioritising. For this purpose, connection times between different cities with different means of transport are analysed. Secondly, an attempt is made to identify bootlenecks at train stations via a Deutsche Bahn timetable API.

## Rationale

<!-- Outline the impact of the analysis, e.g. which pains it solves. -->
The project helps managers and citizens to identify regions where rail transport can still be improved or where the improvment should be prioritised. In addition, it also helps at this point in time in the choice of a place to live by highlighting cities that already have good rail connections.

## Datasources

<!-- Describe each datasources you plan to use in a section. Use the prefic "DatasourceX" where X is the id of the datasource. -->

### Datasource1: Multi-modal accessibility of major German cities
* Metadata URL: https://mobilithek.info/offers/573356838940979200
* Data URL: https://mobilithek.info/mdp-api/files/aux/573356838940979200/moin-2022-05-02.1-20220502.131229-1.ttl.bz2
* Data Type: RDF (Star) Graph, .ttl.bz2 - Archive

RDF (Star) knowledge graph with connection durations between the 100 most populous German cities in terms of car, train and flight.

### Datasource2: Deutsche Bahn Timetable API
* Metadata URL: https://developers.deutschebahn.com/db-api-marketplace/apis/product/timetables/api/26494#/Timetables_10213/overview
* Data URL: https://apis.deutschebahn.com/db-api-marketplace/apis/timetables/v1/
* Data Type: API - application/xml

API for passenger information for train stations operated by DB Station&Service AG. The API provides the data hourly. My idea is to call the API more or less hourly and then store not all but certain data (features I am interessted in) in a storage.

## Work Packages

<!-- List of work packages ordered sequentially, each pointing to an issue with more details. -->

1. Automated Data Pipeline [https://github.com/dominic0df/2023-amse/issues/1]
2. Improve data processing to a reasonable format for datasource1 [https://github.com/dominic0df/2023-amse/issues/6]
3. Automated Testing [https://github.com/dominic0df/2023-amse/issues/2]
4. Contineous Integration [https://github.com/dominic0df/2023-amse/issues/3]
5. Deployment with GitHub pages [https://github.com/dominic0df/2023-amse/issues/4]
6. Identify regions with good/bad train connections [https://github.com/dominic0df/2023-amse/issues/5]

[i1]: https://github.com/jvalue/2023-amse-template/issues/1
