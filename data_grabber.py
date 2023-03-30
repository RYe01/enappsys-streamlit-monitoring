import pandas as pd
import xml.etree.ElementTree as ET
import json
import os

from dotenv import dotenv_values
from urllib.request import urlopen

# .env file holds the pass key for imports
config = dotenv_values(".env")
pass_code = config["PASS"]

# Timezone and Resolution will be by default "CET" and "Quarter Hour", min&avg&max values won't be requested either
def create_import_url(entity, datatype, start, end, passcode=config["PASS"], timezone="CET", res="hourly", minavmax=False):
    return f'https://appqa.enappsys.com/jsonapi?entities={entity}&minavmax={minavmax}&pass={passcode}&res={res}&timezone={timezone}&type={datatype}&user=andras.rozs&start={start}&end={end}'


def get_entities_of(category, country_code, start, end):
    f = open(f'./chart_mappings_per_country/{country_code}-chart_mapping.json')
    mapping = json.load(f)
    
    charts = list(mapping.keys())
    
    if category == "demand":
        charts = [chart for chart in charts if f'{country_code}/elec/demand' in chart]
    elif category == "solar":
        charts = [chart for chart in charts if f'{country_code}/elec/renewables/solar' in chart]
    elif category == "wind":
        charts = [chart for chart in charts if f'{country_code}/elec/renewables/wind' in chart]
    # elif category == ""
    
    entities = {}
    
    for chart in charts:
        for k, v in mapping[chart].items():
            entities[k]= {"import": create_import_url(v['ENTITY'], v['DATATYPE'], start, end, res="qh"), "chart": chart}
        
    return entities 
        

def country_codes():
    return ['eu', 'al', 'at', 'ba', 'be', 'bg', 'ch', 'cz', 'de', 'dk', 'ee', 'es', 'fi', 'fr', 'gb', 'gr', 'hr', 'hu', 'isem', 'it', 'xk', 'lt', 'lv', 'me', 'mk', 'nl', 'no', 'pl', 'pt', 'ro', 'rs', 'se', 'si', 'sk']

def grab_mappings():
    # creating folder for chartmapping json files
    if (not os.path.exists("chart_mappings_per_country")):
        os.mkdir("chart_mappings_per_country")
        
    x = 0
    cc = country_codes()

    for country in cc:
        x+=1
        print(f'The chart mapping of {x}/{len(cc)} country has been created.')
        # grab chart mappings
        with urlopen(f'https://appqa.enappsys.com/chartdatatypeinfo?country={country}&pass={pass_code}&user=andras.rozs&format=xml') as f:
            # parse XML file
            tree = ET.parse(f)

            # get to the root of the XML
            root = tree.getroot()
            
            # extract the data from the XML and store it in a dictionary
            data = {}
            for chart in root.findall('chart'):
                chart_data = {}
                for series in chart.findall('series'):
                    chart_data[series.get('series_name')] = {"DATATYPE": series.get('data_type'), "ENTITY": series.get('entity')}
                data[chart.get('path')] = chart_data
            
        with open(f"./chart_mappings_per_country/{country}-chart_mapping.json", "w") as outfile:
            json.dump(data, outfile)


if __name__ == '__main__':
    # grab_mappings()
    for k,v in get_entities_of('demand', 'de', '202303300000', '202303310000').items():
        print(k, v)