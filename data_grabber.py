import pandas as pd
import streamlit as st
import xml.etree.ElementTree as ET
import json
import os
import time
import numpy as np

from dotenv import dotenv_values
from urllib.request import urlopen
from datetime import datetime, timedelta
from stqdm import stqdm

import db

# .env file holds the pass key for imports
config = dotenv_values(".env")
pass_code = config["PASS"]

# Timezone and Resolution will be by default "CET" and "Quarter Hour", min&avg&max values won't be requested either
def create_import_url(entity, datatype, start, end, passcode=config["PASS"], timezone="CET", res="hourly", minavmax=False):
    return f'https://appqa.enappsys.com/jsonapi?entities={entity}&minavmax={minavmax}&pass={passcode}&res={res}&timezone={timezone}&type={datatype}&user=andras.rozs&start={start}&end={end}'

# Convert DateTime to numbers for link
def convert_date(date_to_convert):
    dt_object = datetime.strptime(date_to_convert, "%Y-%m-%dT%H:%M:%S")
    converted = dt_object.strftime("%Y%m%d%H%M%S")
    return converted

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
        x = 0
        for k, v in mapping[chart].items():
            if k:
                entities[f'{k} - {chart}'] = {"import": create_import_url(v['ENTITY'], v['DATATYPE'], start, end, res="qh"), "chart": chart}
            else:
                x+=1
                entities[f'N/A no.{x} - {chart}'] = {"import": create_import_url(v['ENTITY'], v['DATATYPE'], start, end, res="qh"), "chart": chart}
    
    categories = {}
    for k, v in entities.items():
        if v['chart'] not in categories:
            categories[v['chart']] = {}
            categories[v['chart']][k] = v['import']
        else:
            categories[v['chart']][k] = v['import']
        
    return categories 
        

def country_codes():
    return ['eu', 'al', 'at', 'ba', 'be', 'bg', 'ch', 'cz', 'de', 'dk', 'ee', 'es', 'fi', 'fr', 'gb', 'gr', 'hr', 'hu', 'isem', 'it', 'xk', 'lt', 'lv', 'me', 'mk', 'nl', 'no', 'pl', 'pt', 'ro', 'rs', 'se', 'si', 'sk']

def check_cat():
    return ['demand', 'solar', 'wind']


def grab_mappings():
    # creating folder for chartmapping json files
    if (not os.path.exists("chart_mappings_per_country")):
        os.mkdir("chart_mappings_per_country")
        
    x = 0
    cc = country_codes()
    

    for _ in stqdm(range(len(cc))):
        x+=1
        time.sleep(0.5)
                
        
        # grab chart mappings
        with urlopen(f'https://appqa.enappsys.com/chartdatatypeinfo?country={cc[_]}&pass={pass_code}&user=andras.rozs&format=xml') as f:
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
            
        with open(f"./chart_mappings_per_country/{cc[_]}-chart_mapping.json", "w") as outfile:
            json.dump(data, outfile)

def complete(url):
    data = json.loads(urlopen(url).read())
    data = data['data']
    
    df = pd.json_normalize(data)
    print(df)
    return df

@st.cache_data
def completeness_table():
    tbl = pd.DataFrame(columns=country_codes()) 
    

    for cc in country_codes():
        tbl[cc] = ["", "", ""]
        
    tbl.index = check_cat()
    
    end_check = datetime.now() + timedelta(days=0.1)
    start_check = end_check - timedelta(days=1)
    start_check = start_check.replace(minute=0, second=0)
    
    country_errors = {}
    
    count = 0
    
    for cat in tbl.index:
        for cc in tbl.columns:
            
            count+=1
            print(f'{count}/{len(tbl.columns)*3}')
            
            link_list = []
            is_error = False
            
            en_list = get_entities_of(cat, cc, int(start_check.strftime("%Y%m%d%H%M%S")), int(end_check.strftime("%Y%m%d%H%M%S")))
            for key in en_list.keys():
                for val in en_list[key].values():
                    link_list.append(val)
                    
            if cc not in country_errors:
                country_errors[cc] = {}
            
            link_list = list(set(link_list))
            for link in link_list:
                try:
                    df = complete(link)
                except:
                    continue
                if 'forecast' in link.lower() or 'day_ahead' in link.lower() or 'da_price' in link.lower():
                    print(link.lower())
                    # print(df)
                    if 'value' in df.columns:
                        if(df.isnull().values.any()):
                            list_of_indexes = pd.isnull(df).any('value').nonzero()[0]
                            list_of_times = []
                            for index in list_of_indexes:
                                list_of_times.append(df.iloc[index]['dateTimeUTC'])
                                
                            country_errors[cc][link] = list_of_times
                            is_error = True     
                    else:
                        country_errors[cc][link] = "Not streaming anymore!"
                else:
                    
                    print(df)
                    if 'value' in df.columns:
                        df['dateTime'] = pd.to_datetime(df['dateTime'])
                        end = datetime.now() - timedelta(hours=2)
                        df = df[df['dateTime'] < end]
                        
                        if(df.isnull().values.any()):
                            print('hello')
                            list_of_indexes = pd.isnull(df).any('value').nonzero()[0]
                            list_of_times = []
                            for index in list_of_indexes:
                                list_of_times.append(df.iloc[index]['dateTimeUTC'])
                                
                            country_errors[cc][link] = list_of_times
                            is_error = True     
                    else:
                        country_errors[cc][link] = "Not streaming anymore!"
            
            if country_errors[cc]:
                if is_error:
                    tbl.at[cat, cc] = 'ERROR'
                else:
                    tbl.at[cat, cc] = 'NOT STREAMING'
            else:
                tbl.at[cat, cc] = 'OK'
    
    tbl_dict = tbl.to_dict()
    tbl_columns = tbl.columns
    
    print(tbl_dict)        
    
    return {'tbl_dict': tbl_dict, 'ce': country_errors, 'tbl_columns': tbl_columns}


if __name__ == '__main__':
    cplt = completeness_table()
    tbl_dict = cplt['tbl_dict']
    country_errors = cplt['ce']
    country_code_list = cplt['tbl_columns']
    
    # for cc in country_code_list:
    #     db.insert_country_completeness(cc, tbl_dict)
    db.update_country_completeness(tbl_dict)
    