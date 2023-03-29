import pandas as pd
import xml.etree.ElementTree as ET
import json

from dotenv import dotenv_values
from urllib.request import urlopen

# .env file holds the pass key for imports
config = dotenv_values(".env")
pass_code = config["PASS"]

# Timezone and Resolution will be by default "CET" and "Quarter Hour", min&avg&max values won't be requested either
def create_import_url(entity, passcode, datatype, start, end, timezone="CET", res="qh", minavmax=False):
    return f'https://appqa.enappsys.com/csvapi?entities={entity}&minavmax={minavmax}&pass={passcode}&res={res}&timezone={timezone}&type={datatype}&user=andras.rozs&start={start}&end={end}'


# grab chart mappings
with urlopen(f'https://appqa.enappsys.com/chartdatatypeinfo?country=de&pass={pass_code}&user=andras.rozs&format=xml') as f:
    # parse XML file
    tree = ET.parse(f)

    # get to the root of the XML
    root = tree.getroot()
    
    # extract the data from the XML and store it in a dictionary
    data = {}
    for chart in root.findall('chart'):
        chart_data = {}
        for series in chart.findall('series'):
            chart_data[series.get('series_name')] = series.get('data_type')
        data[chart.get('path')] = chart_data
    
with open("sample.json", "w") as outfile:
    json.dump(data, outfile)
