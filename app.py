import streamlit as st
import pandas as pd
import plotly.express as px
import streamlit_toggle as tog
import json

from pathlib import Path
from datetime import datetime, timedelta

import functions
import data_grabber
import db


st.set_page_config(layout = "wide", page_icon = 'favicon.ico', page_title='EnAppSys Monitoring')

hide_streamlit_style = """
            <style>
            #MainMenu {visibility: hidden;}
            footer {visibility: hidden;}
            </style>
            """
st.markdown(hide_streamlit_style, unsafe_allow_html=True) 

st.header("EnAppSys Monitoring")

if st.button('Refresh Chart Mappings'):
    data_grabber.grab_mappings()
    

functions.space()
toggle_entities = tog.st_toggle_switch(label="Entities of a Chart", 
                    key="toggle_entities", 
                    default_value=False, 
                    label_after = True, 
                    inactive_color = '#D3D3D3', 
                    active_color="#11567f", 
                    track_color="#29B5E8"
                    )

nl_index = data_grabber.country_codes().index('nl')

if toggle_entities:

    country_code = st.selectbox('Select region:', data_grabber.country_codes(), key="country_code", index=nl_index)

    if country_code:
        f = open(f'./chart_mappings_per_country/{country_code}-chart_mapping.json')
        mapping = json.load(f)
        
        chart = st.selectbox('Select chart:', list(mapping.keys()), key="chart")
        
        st.write(mapping[chart])
    
functions.space()
toggle_charts = tog.st_toggle_switch(label="Chart Overview", 
                    key="toggle_charts", 
                    default_value=False, 
                    label_after = True, 
                    inactive_color = '#D3D3D3', 
                    active_color="#11567f", 
                    track_color="#29B5E8"
                    )

if (toggle_charts):
    country_code_charts = st.selectbox('Select region:', data_grabber.country_codes(), key="country_code_charts", index=nl_index)
    if country_code_charts:
        f = open(f'./chart_mappings_per_country/{country_code_charts}-chart_mapping.json')
        mapping = json.load(f)
        
        category = st.selectbox('Select category:', ['demand', 'solar', 'wind'], key="category")
        
        end_check = datetime.now() + timedelta(days=0.1)
        start_check = end_check - timedelta(days=1)

        st.write(data_grabber.get_entities_of(category, country_code_charts, int(start_check.strftime("%Y%m%d%H%M%S")), int(end_check.strftime("%Y%m%d%H%M%S"))))

functions.space()

st.subheader("Completeness")

run_completeness_check = st.button("Run Completeness Check")
    
tbl_data = db.fetch_completeness_table()

tbl = pd.DataFrame(columns=db.fetch_all_country_codes())

for data in tbl_data:
    for i in range(len(data)):
        if i == 0:
            tbl[data[i]] = ["", "", ""]
            cc = data[i]
        else:
            tbl.at[tbl.index[i-1], cc] = data[i]
            
tbl.index = db.fetch_all_categories()
    

st.dataframe(tbl.style.applymap(lambda x: "background-color: green; color: white;" if x == "OK" else ("background-color: orange; color: white;" if x == "NOT STREAMING" else "background-color: red; color: white;")))

if run_completeness_check:
    db.update_country_completeness(data_grabber.completeness_table()['tbl_dict'])
    # country_errors = data_grabber.completeness_table()['ce']
    # st.write(country_errors)




# ----- Dataset Specific ----- #
# functions.space()
# st.write('<p style="font-size:130%">Import Dataset</p>', unsafe_allow_html=True)

# file_format = st.radio('Select file format:', ('csv', 'excel'), key='file_format')
# dataset = st.file_uploader(label = '')

# st.sidebar.header('Select Dataset to Use Available Features')

# if dataset:
#     if file_format == 'csv':
#         df = pd.read_csv(dataset)
#     else:
#         df = pd.read_excel(dataset, engine="openpyxl")
    
#     st.subheader('Dataframe:')
#     n, m = df.shape
#     st.write(f'<p style="font-size:130%">Dataset contains {n} rows and {m} columns.</p>', unsafe_allow_html=True)   
#     st.dataframe(df)


#     all_vizuals = ['Info', 'NA Info', 'Descriptive Analysis', 'Target Analysis', 
#                 'Distribution of Numerical Columns', 'Count Plots of Categorical Columns', 
#                 'Box Plots', 'Outlier Analysis', 'Variance of Target with Categorical Columns']
#     functions.sidebar_space(3)         
#     vizuals = st.sidebar.multiselect("Choose which visualizations you want to see", all_vizuals)

#     if 'Info' in vizuals:
#         st.subheader('Info:')
#         c1, c2, c3 = st.columns([1, 2, 1])
#         c2.dataframe(functions.df_info(df))

#     if 'NA Info' in vizuals:
#         st.subheader('NA Value Information:')
#         if df.isnull().sum().sum() == 0:
#             st.write('There is not any NA value in your dataset.')
#         else:
#             c1, c2, c3 = st.columns([0.5, 2, 0.5])
#             c2.dataframe(functions.df_isnull(df), width=1500)
#             functions.space(2)
            

#     if 'Descriptive Analysis' in vizuals:
#         st.subheader('Descriptive Analysis:')
#         st.dataframe(df.describe())
        
#     if 'Target Analysis' in vizuals:
#         st.subheader("Select target column:")    
#         target_column = st.selectbox("", df.columns, index = len(df.columns) - 1)
    
#         st.subheader("Histogram of target column")
#         fig = px.histogram(df, x = target_column)
#         c1, c2, c3 = st.columns([0.5, 2, 0.5])
#         c2.plotly_chart(fig)


#     num_columns = df.select_dtypes(exclude = 'object').columns
#     cat_columns = df.select_dtypes(include = 'object').columns

#     if 'Distribution of Numerical Columns' in vizuals:

#         if len(num_columns) == 0:
#             st.write('There is no numerical columns in the data.')
#         else:
#             selected_num_cols = functions.sidebar_multiselect_container('Choose columns for Distribution plots:', num_columns, 'Distribution')
#             st.subheader('Distribution of numerical columns')
#             i = 0
#             while (i < len(selected_num_cols)):
#                 c1, c2 = st.columns(2)
#                 for j in [c1, c2]:

#                     if (i >= len(selected_num_cols)):
#                         break

#                     fig = px.histogram(df, x = selected_num_cols[i])
#                     j.plotly_chart(fig, use_container_width = True)
#                     i += 1

#     if 'Count Plots of Categorical Columns' in vizuals:

#         if len(cat_columns) == 0:
#             st.write('There is no categorical columns in the data.')
#         else:
#             selected_cat_cols = functions.sidebar_multiselect_container('Choose columns for Count plots:', cat_columns, 'Count')
#             st.subheader('Count plots of categorical columns')
#             i = 0
#             while (i < len(selected_cat_cols)):
#                 c1, c2 = st.columns(2)
#                 for j in [c1, c2]:

#                     if (i >= len(selected_cat_cols)):
#                         break

#                     fig = px.histogram(df, x = selected_cat_cols[i], color_discrete_sequence=['indianred'])
#                     j.plotly_chart(fig)
#                     i += 1

#     if 'Box Plots' in vizuals:
#         if len(num_columns) == 0:
#             st.write('There is no numerical columns in the data.')
#         else:
#             selected_num_cols = functions.sidebar_multiselect_container('Choose columns for Box plots:', num_columns, 'Box')
#             st.subheader('Box plots')
#             i = 0
#             while (i < len(selected_num_cols)):
#                 c1, c2 = st.columns(2)
#                 for j in [c1, c2]:
                    
#                     if (i >= len(selected_num_cols)):
#                         break
                    
#                     fig = px.box(df, y = selected_num_cols[i])
#                     j.plotly_chart(fig, use_container_width = True)
#                     i += 1

#     if 'Outlier Analysis' in vizuals:
#         st.subheader('Outlier Analysis')
#         c1, c2, c3 = st.columns([1, 2, 1])
#         c2.dataframe(functions.number_of_outliers(df))

#     if 'Variance of Target with Categorical Columns' in vizuals:
        
#         df_1 = df.dropna()
        
#         high_cardi_columns = []
#         normal_cardi_columns = []

#         for i in cat_columns:
#             if (df[i].nunique() > df.shape[0] / 10):
#                 high_cardi_columns.append(i)
#             else:
#                 normal_cardi_columns.append(i)


#         if len(normal_cardi_columns) == 0:
#             st.write('There is no categorical columns with normal cardinality in the data.')
#         else:
        
#             st.subheader('Variance of target variable with categorical columns')
#             model_type = st.radio('Select Problem Type:', ('Regression', 'Classification'), key = 'model_type')
#             selected_cat_cols = functions.sidebar_multiselect_container('Choose columns for Category Colored plots:', normal_cardi_columns, 'Category')
            
#             if 'Target Analysis' not in vizuals:   
#                 target_column = st.selectbox("Select target column:", df.columns, index = len(df.columns) - 1)
            
#             i = 0
#             while (i < len(selected_cat_cols)):
                
#                 if model_type == 'Regression':
#                     fig = px.box(df_1, y = target_column, color = selected_cat_cols[i])
#                 else:
#                     fig = px.histogram(df_1, color = selected_cat_cols[i], x = target_column)

#                 st.plotly_chart(fig, use_container_width = True)
#                 i += 1

#             if high_cardi_columns:
#                 if len(high_cardi_columns) == 1:
#                     st.subheader('The following column has high cardinality, that is why its boxplot was not plotted:')
#                 else:
#                     st.subheader('The following columns have high cardinality, that is why its boxplot was not plotted:')
#                 for i in high_cardi_columns:
#                     st.write(i)
                
#                 st.write('<p style="font-size:140%">Do you want to plot anyway?</p>', unsafe_allow_html=True)    
#                 answer = st.selectbox("", ('No', 'Yes'))

#                 if answer == 'Yes':
#                     for i in high_cardi_columns:
#                         fig = px.box(df_1, y = target_column, color = i)
#                         st.plotly_chart(fig, use_container_width = True)