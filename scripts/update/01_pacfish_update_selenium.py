# Author: Saeesh Mangwani
# Date: 14/05/2021

# Description: A script for scraping Pacfish Gauge and Temperature data and
# adding it to existing datatables.
# %% ==== Loading libraries ====
import requests
import pandas as pd
from datetime import datetime, timedelta
from selenium import webdriver
from bs4 import BeautifulSoup
from io import StringIO
from optparse import OptionParser
from sqlalchemy import create_engine
from json import load
from update_help_funcs import format_station_data, get_urls_by_variable, check_success_status

#%% Initializing option parsing
parser = OptionParser()
parser.add_option("-d", "--days", dest="days", action="store", default=30,
                  help="The number of days before today for which data need to be downloaded")
parser.add_option("-g", "--gecko", dest="geckopath", action="store", default='E:/saeeshProjects/_webdrivers/geckodriver/geckodriver',
                  help="Path to geckodriver, which opens an automated firefox browser")
(options, args) = parser.parse_args()

# %% ==== Initalizing global variables ====

# Reading credentials from file
creds = load(open('../../credentials.json',))

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
    creds['schema']
))
conn = db.raw_connection()
cursor = conn.cursor()

# The path to the directory where the the update report from each run should be
# stored. Defaults to the working directory
path_to_report = '../../update_report.txt'
# Path to the reference data table storing station names and ids. Defaults to
# the data folder under the current working directory
path_to_ref_tab = '../../data/pacfish_station_data.csv'
# How many days worth of data is required
time_diff = timedelta(days=options.days)
# Path to the geckodriver that runs firefox in automative mode
gecko_path = options.geckopath

# %% ==== Reading the reference table for station names and IDs ====
ref_tab = pd.read_csv(path_to_ref_tab)

# Removing stations that don't exist
ref_tab = ref_tab[~ref_tab.status.str.match('INACTIVE')]

# Getting station names correctly formatted
names = ref_tab["station_url_name"]

# %% Reading the master pacfish table --------

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
    """.format((datetime.today() - time_diff).strftime('%Y-%m-%d'))
)
# Reading the result
curr_data = pd.DataFrame(cursor.fetchall(), columns=list(dtype_dict.keys()))
# Ensuring type consistenct
curr_data = curr_data.astype(dtype_dict)

# %% ==== Preparing and validating data URLs ====

# A function that returns the station URL names for each station that has data
# associated with a certain variable
hyd_links = get_urls_by_variable('staff_gauge', ref_tab)
press_links = get_urls_by_variable('pressure', ref_tab)
temp_links = get_urls_by_variable('temp', ref_tab)

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

# %% ==== Automating downloads with Selenium ====

# Opening firefox driver
browser = webdriver.Firefox(executable_path = gecko_path)
# url_grp = list(links.keys())[0]
# url_name = list(links[url_grp].keys())[14]

# Getting the correctly formatted date from when we want data
start_date = (datetime.today() - time_diff).strftime('%b %d, %Y 00:00')

# Iterating over each url and group
for url_grp in links:
    print("Iterating", url_grp, "links")
    # Getting the links associated with that group
    curr_url_grp = links[url_grp]
    # For each url in this group of urls
    for url_name in curr_url_grp:
        try:
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
            # Writing the data to a csv buffer
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
    print('This run was a database UPDATE - run via the script "pacfish_update_selenium.py"', file=f)
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