# Author: Saeesh Mangwani
# Date: 24/04/2021

# Description: A script for scraping Pacfish Gauge and Temperature data and
# adding it to existing datatables.
# %% ==== Loading libraries ====
import os
import sys
from pathlib import Path
os.chdir(Path(__file__).parent.parent.parent)
sys.path.append(os.getcwd())
import requests
import pandas as pd
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from io import StringIO
from json import load
from sqlalchemy import create_engine
from scripts.update.update_help_funcs import format_station_data, get_urls_by_variable, check_success_status

# %% ==== Initializing user facing global variables ====

# Reading credentials from JSON
creds = load(open('options/credentials.json',))

# Reading filepaths from JSON
fpaths = load(open('options/filepaths.json',))

# Setting the default schema to 'pacfish' unless another was specified in the file
if 'schema' not in creds.keys():
    creds['schema'] = 'pacfish'

# Database connection
db = create_engine('postgresql+psycopg2://{}:{}@{}:{}/{}?options=-csearch_path%3D{}'.format(
    creds['user'],
    creds['password'],
    creds['host'],
    creds['port'],
    creds['dbname'],
    creds['schema'],
))
conn = db.raw_connection()
cursor = conn.cursor()

# Path to the reference data table storing station names and ids. Defaults to
# the data folder under the current working directory
path_to_ref_tab =  fpaths['station_data']

# Path to status report
path_to_report = fpaths['report']

# %% ==== Initializing script global variables ====
ref_tab = pd.read_csv(path_to_ref_tab)

# Getting station names correctly formatted
names = ref_tab["station_url_name"]

# %% Reading the data table to be updated --------

# Defining a dictionary of column data types (this will also be appied to newly
# downloaded data at a later stage)
dtype_dict = {
    'STATION_NUMBER': 'str',
    'STATION_NAME': 'str',
    'Date': 'datetime64',
    'Time': 'str',
    'Value': 'float64',
    'Parameter': 'str',
    'Code': 'str',
    'Comments': 'str',
}

# Querying only the last 30 days of data from the database
cursor.execute(
    """
    select * from pacfish.hourly 
    where "Date" >= '{}'
    """.format((datetime.today() - timedelta(days=30)).strftime('%Y-%m-%d'))
)
# Reading the result
curr_data = pd.DataFrame(cursor.fetchall(), columns=list(dtype_dict.keys()))
# Ensuring type consistency
curr_data = curr_data.astype(dtype_dict)

# %% ==== Preparing and validating data URLs ====

# A function that returns the station URL names for each station that has data
# associated with a certain variable
hyd_links = get_urls_by_variable('staff_gauge', ref_tab)
press_links = get_urls_by_variable('barometric_pressure', ref_tab)
temp_links = get_urls_by_variable('water_temperature', ref_tab)

# Storing these themselves in a dict to allow for appropriate naming during the cleaning stage
links = {
    'Hydrometric': hyd_links,
    'Pressure': press_links, 
    'Temperature': temp_links
    }

# %% Checking that the urls are valid --------
hyd_codes = {name: requests.get(
    link).status_code for name, link in hyd_links.items()}
print("Hydrometric links checked")
press_codes = {name: requests.get(
    link).status_code for name, link in hyd_links.items()}
print("Pressure links checked")
temp_codes = {name: requests.get(
    link).status_code for name, link in temp_links.items()}
print("Temperature links checked")

#%% Saving validity status --------

# Creating dictionaries storing the successful data scrape status associated
# with each link. For any links that failed the validity check, setting their
# sucess status to False
hyd_success, hyd_all_valid = check_success_status(hyd_codes)
print("Hydrometric links all valid:", hyd_all_valid)

press_success, press_all_valid = check_success_status(press_codes)
print("Pressure links all valid:", press_all_valid)

temp_success, temp_all_valid = check_success_status(temp_codes)
print("Temperature links all valid:", temp_all_valid)

# Creating a "success status" dictionary from these (will be updating with later
# error checking too)
success_status = {
    'Hydrometric': hyd_success, 
    'Pressure': press_success,
    'Temperature': temp_success
}

# Filtering the links dictionaries based on their validity status (i.e removing
# failed links if present)
links['Hydrometric'] = {name: link for name,
                        link in links['Hydrometric'].items() if hyd_success[name] == 'success'}
links['Pressure'] = {name: link for name,
                        link in links['Pressure'].items() if press_success[name] == 'success'}
links['Temperature'] = {name: link for name,
                        link in links['Temperature'].items() if temp_success[name] == 'success'}

# %% ==== Iterating over each valid link and appending it's data ====

# url_grp = list(links.keys())[0]
# url_name = list(links[url_grp].keys())[14]

# For each link group
for url_grp in links:
    print("Iterating", url_grp, "links")
    # Getting the links associated with that group
    curr_url_grp = links[url_grp]
    # For each url in this group of urls
    for url_name in curr_url_grp:
        try:
            # Getting the url from the dict using the key
            url = links[url_grp][url_name]
            # Opening page
            page = requests.get(url)
            # Parsing with beautiful soup
            soup = BeautifulSoup(page.content, 'html.parser')
            # Using beautiful soup to find the data table element by class and id
            stat_table = soup.find(attrs={'class': 'CenteredGrid'})
            # Converting it to a pandas dataframe
            df = pd.read_html(str(stat_table))[0]

            # Formatting the dataframe to GW specifications by calling the function
            # defined above
            df = format_station_data(df, url_grp, url_name, ref_tab)
            # Ensuring types are consistently set
            df = df.astype(dtype_dict)

            # Doing an anti-join with existing set of recent data, to ensure
            # overlaps are removed. To do so, first a left join with an 'indicator'
            # is needed
            left_joined = df.merge(curr_data, how='left', indicator=True)
            # Keeping only those df rows where the merge indicator says "left_only"
            left_joined = left_joined[left_joined._merge == "left_only"]
            df = left_joined.drop(columns="_merge")

            # Rearranging columns to match specification
            df = df[['STATION_NUMBER', 'STATION_NAME', 'Date', 'Time',
                    'Value', 'Parameter', 'Code', 'Comments']]

            # Initialize an empty string buffer
            sio = StringIO()    
            # Appending these rows to the complete csv
            df.to_csv(sio, sep = ',', header=False, index=False, columns = list(dtype_dict.keys()))
            sio.seek(0)
            # Appending to database from from buffer
            cursor.copy_from(sio, "hourly", sep = ',')
            conn.commit()
            # Status update
            print("Successfully completed data pull for Station: ",
                  url_name, ", Data type:", url_grp)
        except Exception as e:
            # Printing a message in case of an error
            print(url_grp, "data scrape failed for station:", url_name)
            print("Error:", str(e))
            # Saving the error message in the success dictionary
            success_status[url_grp][url_name] = "Error: " + str(e)

# %% ==== Writing a status txt file giving details of this run ====

# Opening a report file
with open(path_to_report, "w") as f:
    # Printing a header and description
    print('===== Pacfish Hydrometric Data Scraper =====', file=f)
    print('', file=f)
    print('This file gives a summary of the most recent pacfish data update', file = f)
    print('This run was a database UPDATE - run via the script "pacfish_update_7-day.py"', file=f)
    print('', file=f)
    # Date of scraping attempt
    print('Most recent scrape Date: ', str(datetime.now()), file=f)
    # Whether all links were valid
    print('All hydrometric links valid:', hyd_all_valid, file=f)
    print('All pressure links valid:', press_all_valid, file=f)
    print('All temperature links valid:', temp_all_valid, file=f)
    print('', file=f)
    # Station-wise status for Hydrometric data (formatted as a dataframe for easy reading)
    print('Hydrometric data station completion status:', file=f)
    print(pd.DataFrame.from_dict(success_status['Hydrometric'], orient='index').rename(
        columns={0: "Status"}), file=f)
    print('', file=f)
    # Station-wise status for pressure data
    print('Pressure data station completion status:', file=f)
    print(pd.DataFrame.from_dict(success_status['Pressure'], orient='index').rename(
        columns={0: "Status"}), file=f)
    print('', file=f)
    # Station-wise status for temperature data
    print('Temperature data station completion status:', file=f)
    print(pd.DataFrame.from_dict(success_status['Temperature'], orient='index').rename(
        columns={0: "Status"}), file=f)
# %%
