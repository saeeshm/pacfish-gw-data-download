# Author: Saeesh Mangwani
# Date: 16/05/2022

# Description: Updating the station metadata file to account for changes

# %% ==== Loading libraries ====
import os
from pathlib import Path
os.chdir(Path(__file__).parent.parent.parent)
import pandas as pd
from datetime import datetime
from selenium import webdriver
from bs4 import BeautifulSoup
from json import load
from sqlalchemy import create_engine
import re
import subprocess

# %% ==== Initalizing global variables ====

# Reading credentials from JSON
creds = load(open('options/credentials.json',))

# Reading filepaths from JSON
fpaths = load(open('options/filepaths.json',))

# Path to the reference data table storing station names and ids. Defaults to
# the data folder under the current working directory
path_to_ref_tab = fpaths['station_data']

# Path to the geckodriver that runs firefox in automative mode
gecko_path = fpaths['geckodriver']

# %% ==== Setting up database connection ====
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

# %% ==== Initializing Selenium browser ====

# Opening firefox driver
browser = webdriver.Firefox(executable_path=gecko_path)

# Navigating to the main pacfish page
baseurl = 'http://www.pacfish.ca/wcviweather/'
browser.get(baseurl)

# %% ==== Getting basic station info ====

# Reading page source
soup = BeautifulSoup(browser.page_source, 'html.parser')

# Getting list of stations sidebar element
stat_list = soup.find(attrs={'id': 'sidebar'}) \
    .find(attrs={'class': ['c2','alignleft']}) \
    .find(attrs={'id': 'example'}) \
    .findAll('li', recursive=False)

# Filtering the list to remove non-station related list items
stat_list = [stat \
    for stat in stat_list \
    if stat.find('p', class_='sf-with-ul') is not None \
        and stat.find('p', class_='sf-with-ul').text != 'Admin'
]

# %% ==== Station names and available data URLs ====
outlist = list()
# Extracting data from station lists
for i in range(0, len(stat_list)):
    # Getting station data
    stat = stat_list[i]
    # Station name
    statname = stat.find('p', class_='sf-with-ul').text
    print(statname)
    # Child list of params and urls
    ul = stat.find('ul', recursive=False)
    # Available parameters
    params = [li.text for li in ul.findAll('a')]
    # urls 
    urls = [li['href'] for li in ul.findAll('a')]
    # Dictionary of urls by parameter, selecting only relevant ones
    outdict = {
        param: (baseurl+url) \
        for param, url in zip(params, urls) \
        if param in ['Water Temperature', 'Barometric Pressure', 'Staff Gauge', 'Voltage', 'Site Info']
    }
    # Adding the station id to the output dictionary
    outdict['station_name'] = statname
    # Creating a nested dictionary
    outlist.append(outdict)

dat = pd.DataFrame(outlist)

# Removing stations where all data URLs are NA
dat.dropna(axis=0, how='all', subset=['Staff Gauge', 'Water Temperature', 'Barometric Pressure'], inplace=True)

# Getting url station names from URLs
staturlnames = [re.search('(?<=20Pages\/)(\w+)', levelurl).group(1) \
    for levelurl in list(dat['Site Info'])]
dat['station_url_name'] = staturlnames

# Creating station ids from station names - removing water body identifiers and whitespace
statids = [re.sub('(RIVER|LAKE|CREEK|\s)', '',statname.upper()) \
    for statname in list(dat['station_name'])]
    
# Cleaning "upper" and "lower" tags
def str_clean_ul(str):
    if re.search('(\w+)(\s?)(UPPER|LOWER)', str) is None:
        return str
    matchobj = re.match('(\w+)(\s?)(UPPER|LOWER)', str)
    outname = matchobj.group(3)[0:1] + '_' + matchobj.group(1)
    return outname
statids = [str_clean_ul(id) for id in statids]
# Adding a P_ prefix
statids = ['P_' + id for id in statids]
# Adding to dataframe
dat['station_id'] = statids

# %% ==== Data start and end dates ====
start_list = list()
end_list = list()

for i in range(0, dat.shape[0]):
    # Getting data url
    url = dat.iloc[i]['Staff Gauge']
    # Navigating to url
    browser.get(url)
    # Getting the 'from' date element
    elem = browser.find_element_by_id('ContentPlaceHolder1_DateTimePicker')
    # Setting the value tag to an arbitrary very old start date
    browser.execute_script('arguments[0].setAttribute("value", "Jan 01, 1950 00:00")', elem)
    # Clicking to get data - it fails because the index is out of range, but the website populates the range with the correct min and max data ranges
    browser.find_element_by_id('ContentPlaceHolder1_Button1').click()

    # Getting the start date
    elem = browser.find_element_by_id('ContentPlaceHolder1_DateTimePicker')
    start_date = elem.get_attribute('value')
    try:
        start_date = datetime.strptime(start_date, '%b %d, %Y %I:%M %p').strftime('%Y/%m/%d %H:%M')
    except:
        start_date = datetime.strptime(start_date, '%b %d, %Y %H:%M').strftime('%Y/%m/%d %H:%M')
    start_list.append(start_date)

    # End date
    elem = browser.find_element_by_id('ContentPlaceHolder1_DateTimePicker2')
    end_date = elem.get_attribute('value')
    try:
        end_date = datetime.strptime(end_date, '%b %d, %Y %I:%M %p').strftime('%Y/%m/%d %H:%M')
    except ValueError:
        end_date = datetime.strptime(end_date, '%b %d, %Y %H:%M').strftime('%Y/%m/%d %H:%M')
    end_list.append(end_date)

# Adding date columns to dataframe
dat['start_date'] = start_list
dat['end_date'] = end_list

# %% ==== Station coordinates ====

# From the main station html, getting the script containing the Javascript with station coordinates
scr = soup.find(attrs={'id':'main'}).find('script', attrs={'type': 'text/javascript'})

# Extracting the station coordinates list
matchstr = re.search("var locations = (\[(.|\n)*\]);", scr.string).group(1)

# Parsing object string to a dataframe of coordinates by station ID
def parse_coords(matchstr):
    # Removing container brackets
    matchstr = matchstr.strip('][')
    # Removing comment lines and newlines
    matchstr = re.sub('\n|\/','',matchstr)
    # Splitting to lists of objs
    coordlist = matchstr.split('],')
    # Removing brackets again
    coordlist = [item.strip().strip('][') for item in coordlist]
    # Splitting strings into lists of items
    coordlist = [re.sub('\'', '', item).split(', ') for item in coordlist]
    # Removing empty list items and the URL column (we already have these)
    coordlist = [itemlist[0:3] for itemlist in coordlist if len(itemlist) == 4]
    # Creating dataframe
    df = pd.DataFrame(coordlist, columns=['station_id', 'lat', 'long'])
    # Formatting station names to station IDs for matching
    def stat_name_to_id(name):
        name = name.upper()
        name = re.sub('(RIVER|LAKE|CREEK)', '', name)
        name = name.strip()
        name = re.sub('U\s', 'U_', name)
        name = re.sub('L\s', 'L_', name)
        name = re.sub('\s', '', name)
        name = 'P_' + name
        return(name)
    df['station_id'] = [
        stat_name_to_id(name)for name in list(df['station_id'])
    ]
    df = df.astype({'station_id':'str', 'lat': 'float', 'long': 'float'})
    return df
# Parsing coodinates
coords = parse_coords(matchstr)

# Joining coordinates to station table
dat = dat.join(coords.set_index('station_id'), on = 'station_id', how = 'left')

# %% ==== Cleaning and formatting metadata table ====

# Converting URL cols to boolean values indicating whether this datatype is available or not
dat['Staff Gauge'] = dat['Staff Gauge'].notna()
dat['Water Temperature'] = dat['Water Temperature'].notna()
dat['Barometric Pressure'] = dat['Barometric Pressure'].notna()
dat['Voltage'] = dat['Voltage'].notna()

# Cleaning names
dat.columns = [re.sub('\s', '_', name.lower()) for name in dat.columns]

# Rearranging order
dat = dat[['station_id', 'station_name', 'station_url_name', 'start_date', 'end_date', 'water_temperature', 'staff_gauge', 'voltage', 'barometric_pressure', 'lat', 'long','site_info']]

# %% ==== Checking for new stations since last update ====

# Querying current station table
try:
    cursor.execute("select * from pacfish.station_metadata")
    curr_stats = pd.DataFrame(cursor.fetchall(), columns=dat.columns)
    # Comparing current station names with new names to get new stations
    new_stats = set(dat.station_id).difference(set(curr_stats.station_id))
    # Replacing station metadata file with the new file
    dat.to_sql('station_metadata', db, schema = creds['schema'], if_exists='replace', index = False, index_label=dat.columns)
except:
    print('No pre-existing station data. Adding the new metadata file directly.')
    dat.to_sql('station_metadata', db, schema = creds['schema'], if_exists='replace', index = False, index_label=dat.columns)
    new_stats = set(dat.station_id)


# %% ==== Calling an archive reset for all new stations ====
if len(new_stats) > 0:
    for statid in new_stats:
        print('Getting the full timeseries for new station: ' + statid)
        subprocess.run(args = ('python scripts/reset/03_pacfish_reset_by_station.py -s "' + statid +'"'), shell=True)

# %% Writing metadata file to disk
dat.to_csv(path_to_ref_tab, index=False, na_rep='NA')

# Closing browser
browser.close()
# %%
