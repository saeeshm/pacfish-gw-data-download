# Author: Saeesh Mangwani
# Date: 16/05/2022

# Description: Downloading the historical archive of Pacfish data and adding to the postgres schema
# %% ==== Loading libraries ====
import os
import sys
from pathlib import Path
os.chdir(Path(__file__).parent.parent.parent)
sys.path.append(os.getcwd())
import requests
import pandas as pd
from datetime import datetime
from selenium import webdriver
from bs4 import BeautifulSoup
from io import StringIO
from sqlalchemy import create_engine
from json import load

# %% ==== Initalizing global variables ====

# Reading credentials from JSON
creds = load(open('options/credentials.json',))

# Reading filepaths from JSON
fpaths = load(open('options/filepaths.json',))

# Path to the reference data table storing station names and ids. Defaults to
# the data folder under the current working directory
path_to_ref_tab = fpaths['station_data']

# Path to status report
path_to_report = fpaths['report']

# Path to the geckodriver that runs firefox in automative mode
gecko_path = fpaths['geckodriver']

# Defining a dictionary of column data types (this will be appied to newly
# downloaded data)
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

#%% Starting database connection
# Database connection
db = create_engine('postgresql+psycopg2://{}:{}@{}:{}/{}?options=-csearch_path%3D{}'.format(
    creds['user'],
    creds['password'],
    creds['host'],
    creds['port'],
    creds['dbname'],
    creds['schema']
))
conn = db.raw_connection()
cursor = conn.cursor()

# %% ==== Reading the reference table for station names and IDs ====
ref_tab = pd.read_csv(path_to_ref_tab)

# Removing stations that don't exist
# ref_tab = ref_tab[~ref_tab.status.str.match('INACTIVE')]

# Getting station names correctly formatted
names = ref_tab["station_url_name"]

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
    link).status_code for name, link in press_links.items()}
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

# %% ==== Automating downloads with Selenium ====

# Opening firefox driver
browser = webdriver.Firefox(executable_path=gecko_path)
# url_grp = list(links.keys())[0]
# url_name = list(links[url_grp].keys())[0]

# Iterating over each url and group
for url_grp in links:
    print("Iterating", url_grp, "links")
    # Getting the links associated with that group
    curr_url_grp = links[url_grp]
    # For each url in this group of urls
    for url_name in curr_url_grp:
        try:
            # Setting arbitrary start date - except for Leiner, which has a weird glitching preventing the auto-setting feature of pacfish from working
            if url_name == 'Leiner':
                start_date = '10/09/2018 11:01'
            else:
                start_date = '01/01/2000 00:00'
            start_date = datetime.strptime(start_date, '%m/%d/%Y %H:%M').strftime('%b %d, %Y %H:%M')
                
            # Navigating to url
            browser.get(links[url_grp][url_name])

            # Setting the date value in this date-picker to be the date we're
            # interested in (keep trying until it works or exceeded max tries)
            iters = 0
            curr_elem_val = ''
            while(curr_elem_val != start_date):
                # Refreshing the browswer
                browser.refresh()
                # Getting the 'from' date element
                elem = browser.find_element_by_id(
                    'ContentPlaceHolder1_DateTimePicker')
                # Trying to set the value tag to the start date for the data
                browser.execute_script(
                    'arguments[0].setAttribute("value", "%s")' % start_date, elem)
                # Getting the current element value following the set attempt
                curr_elem_val = elem.get_attribute('value')
                # Incrementing max iterations by 1
                iters += 1
                # If iterations exceed 5, breaking the loop with an error
                if(iters == 6):
                    print("Maximum iterations reached without success")
                    break
            else:
                print("Date successfully set after", iters, "iterations")

            # Clicking button to get tabular data for these dates
            # First click fails, because the start date is too old. But the website populates the 
            # field with the correct start data for this station
            browser.find_element_by_id('ContentPlaceHolder1_Button1').click()
            # Second click succeeds, because the start date is correct
            browser.find_element_by_id('ContentPlaceHolder1_Button1').click()
            # Parsing the page's source html with BeautifulSoup 
            soup = BeautifulSoup(browser.page_source, 'html.parser')
            # Getting data table element by class (using beautiful soup)
            stat_table = soup.find(attrs={'class': 'CenteredGrid'})

            # Converting it to a pandas dataframe
            df = pd.read_html(str(stat_table))[0]

            # Formatting the dataframe to GW specifications by calling the function
            # defined above
            df = format_station_data(df, url_grp, url_name, ref_tab)
            # Ensuring types are consistently set
            df = df.astype(dtype_dict)

            # Rearranging columns to match specification
            df = df[['STATION_NUMBER', 'STATION_NAME', 'Date', 'Time',
                    'Value', 'Parameter', 'Code', 'Comments']]
            
            # If this is the first table in the first group list of names, overwriting the table and setting columns
            if url_grp == list(links.keys())[0] and url_name == names[0]:
                df.head(0).to_sql('hourly', db, schema = creds['schema'], if_exists='replace', index = False)

            # Initialize an empty string buffer
            sio = StringIO()
            # Writing the data to a csv buffer
            df.to_csv(sio, sep=',', header=False, index=False,
                      columns=list(dtype_dict.keys()))
            sio.seek(0)
            # Appending to database from from buffer
            cursor.copy_from(sio, "hourly", sep=',')
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

# Closing selenium server
browser.close()
cursor.close()

# %% ==== Writing a status txt file giving details of this run ====

# Opening a report file
with open(path_to_report, "w") as f:
    # Printing a header and description
    print('===== Pacfish Hydrometric Data Scraper =====', file=f)
    print('', file=f)
    print('This file gives a summary of the most recent pacfish data update', file = f)
    print('This run was a database RESET - run via the script "pacfish_download_archive.py"', file=f)
    print('', file=f)
    # Date of scraping attempt
    print('Last scrape Date: ', str(datetime.now()), file=f)
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
