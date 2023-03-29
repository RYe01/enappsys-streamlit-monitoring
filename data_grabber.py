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
def create_import_url(entity, passcode, datatype, start, end, timezone="CET", res="qh", minavmax=False):
    return f'https://appqa.enappsys.com/csvapi?entities={entity}&minavmax={minavmax}&pass={passcode}&res={res}&timezone={timezone}&type={datatype}&user=andras.rozs&start={start}&end={end}'

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
                    chart_data[series.get('series_name')] = {"DATAYPE": series.get('data_type'), "ENTITY": series.get('entity')}
                data[chart.get('path')] = chart_data
            
        with open(f"./chart_mappings_per_country/{country}-chart_mapping.json", "w") as outfile:
            json.dump(data, outfile)




if __name__ == '__main__':
    print('grabbing')
    grab_mappings()     