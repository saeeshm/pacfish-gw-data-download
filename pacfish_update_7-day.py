# Author: Saeesh Mangwani
# Date: 24/04/2021

# Description: A script for scraping Pacfish Gauge and Temperature data and
# adding it to existing datatables.
# %% ==== Loading libraries ====
import requests
import pandas as pd
from datetime import datetime, timedelta
from bs4 import BeautifulSoup

# %% ==== Initializing user facing global variables ====

# The path to the directory where the the update report from each run should be
# stored. Defaults to the working directory
path_to_report = 'update_report.txt'
# Path to the reference data table storing station names and ids. Defaults to
# the data folder under the current working directory
path_to_ref_tab = 'data/Pacfish Monitoring stations 2021.xlsx'
# Path to the station csv that needs to be update with new station data.
# Defaults to the data folder under the current working directory
path_to_dat_tab = 'data/pacfish_stations.csv'

# %% ==== Initializing script global variables ====

# Reading the reference table for station names and ids --------
ref_tab = pd.read_excel(path_to_ref_tab)[["STATION_NAME", "STATION_NUMBER"]]

# Removing stations that don't exist
ref_tab = ref_tab[~ref_tab.STATION_NAME.str.match('Burman|Shaw')]

# Getting station names correctly formatted
names = ref_tab["STATION_NAME"]
names = names.str.replace("Creek|River|Lake|Upper|\s",
                          "", regex=True).str.strip()
names = names.tolist()

# Replacing some URL names to account for inconsistent formatting on the site
names[9] = 'BlackCreek'
names[16] = 'FrenchCreek'

# Adding formatted names as a column back to the ref_table to allow for indexing
# station names and ids at a later stage
ref_tab['inames'] = names

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

# Reading data, and ensuring that the Date column is parsed as a pandas timestamp
dat_tab = pd.read_csv(
    path_to_dat_tab,
    parse_dates=['Date'],
    converters={'Code': lambda x: '' if str(x) == 'nan' else x,
                'Comments': lambda x: '' if str(x) == 'nan' else x}
)
# Creating a timestamp of 30 days prior to today
filter_tstamp = (datetime.today() - timedelta(days=14))
# Creating a filter mask to only pick data for the last 30 days (really imp for
# efficiency since we only need to refer to recent data to ensure a lack of
# overlap)
filter_mask = dat_tab['Date'] > filter_tstamp
# Applying the filter mask to select the df
dat_tab = dat_tab[filter_mask]
# Removing the filter objects as they're quite large and no longer necessary
del([filter_tstamp, filter_mask])
# Ensuring appropriate types in the remaining table
dat_tab.astype(dtype_dict)

# %% Helper function for formatting data to GW specifications --------


def formatStationDat(df, url_grp, url_name, ref_tab):

    # Creating a list of variable names based on the url group currently being
    # iterated over - if it is gage, using hydrometric names
    if url_grp == "Hydrometric":
        # If there are only 2 dims, using only 2 names otherwise 3
        col_names = ['Time', 'Water Level', 'Sensor Depth'] if df.shape[1] == 3 else [
            'Time', 'Water Level']
    # If it is Temp, using temperature names
    else:
        col_names = ['Time', 'Air Temperature', 'Water Temperature'] if df.shape[1] == 3 else [
            'Time', 'Water Temperature']

    # Renaming columns
    df.columns = col_names

    # Adding an empty column for comment
    df['Comments'] = ''

    # Checking if either of the data columns have "estimated" tags by seeing if
    # they were parsed as strings or not
    if df.shape[1] == 4:
        # If both data columns are present, checking both
        col1_str = pd.api.types.is_string_dtype(df.iloc[:, 1])
        col2_str = pd.api.types.is_string_dtype(df.iloc[:, 2])
    else:
        # Otherwise checking only the first 1 and setting the second string
        # check to false by default
        col1_str = pd.api.types.is_string_dtype(df.iloc[:, 1])
        col2_str = False

    # Checking the type of the data variable. If it is a string, "Estimated" tags
    # are present which need to be corrected for
    if col1_str or col2_str:
        # Checking where the "Estimated" tags are present (if the column is not
        # a string, then there are certainly no such tags present in which case
        # just returning a False series)
        code_index1 = df.iloc[:, 1].str.contains(
            pat="\*\*Estimated", regex=True) if col1_str else (df.iloc[:, 1]*0).astype('bool')
        # Doing the same for the second column
        code_index2 = df.iloc[:, 2].str.contains(
            pat="\*\*Estimated", regex=True) if col2_str else (df.iloc[:, 1]*0).astype('bool')
        # Creating a combined index through an 'or' operation
        code_index = code_index1 | code_index2

        # Using the index, assigning a value of 21 where there are estimates otherwise
        # leaving it empty, and assigning this to the code variable
        df['Code'] = [21 if i else '' for i in code_index]
        # Converting variables containing data to numeric by removing any
        # "Estimated" tags (assuming the column is a string. Again, if it isn't
        # just returning the original column)
        df.iloc[:, 1] = df.iloc[:, 1].str.replace(
            pat="\*\*Estimated",
            repl='',
            regex=True).astype(float) if col1_str else df.iloc[:, 1]
        # For variable 2, if it exists
        if df.shape[1] == 5:
            df.iloc[:, 2] = df.iloc[:, 2].str.replace(
                pat="\*\*Estimated",
                repl='',
                regex=True).astype(float) if col2_str else df.iloc[:, 2]
    # Otherwise leaving the code column empty
    else:
        df['Code'] = ''

    # Splitting the Time Column into dates and times
    df['Date'] = pd.to_datetime(df['Time']).dt.date
    df['Time'] = pd.to_datetime(df['Time']).dt.time

    # If there are 6 columns by now:
    if df.shape[1] == 6:
        # Separating out the second stream of data
        df2 = df.loc[:, df.columns != df.columns[1]]
        # Removing it from the original df
        df = df.loc[:, df.columns != df.columns[2]]
        # Adding a column named Parameter based on the column name
        df['Parameter'] = df.columns[1]
        df2['Parameter'] = df2.columns[1]
        # Renaming both data columns to "Value"
        df.rename(columns={df.columns[1]: 'Value'}, inplace=True)
        df2.rename(columns={df2.columns[1]: 'Value'}, inplace=True)
        # Appending them by row and saving this as the original df
        df = df.append(df2, ignore_index=True)
    # If there are not 6 rows there was only 1 stream of data
    else:
        # So we only need to add a parameter column
        df['Parameter'] = df.columns[1]
        # And change the data column name to Value
        df.rename(columns={df.columns[1]: 'Value'}, inplace=True)

    # Finally, adding columns for station name and station ID by referencing the reference table
    df['STATION_NAME'] = ref_tab.loc[ref_tab['inames'] == url_name].iloc[0, 0]
    df['STATION_NUMBER'] = ref_tab.loc[ref_tab['inames'] == url_name].iloc[0, 1]

    # Returning the cleaned df
    return(df)


# %% ==== Preparing and validating data URLs ====

# Creating a list of urls from the list of names --------
hyd_links = [("http://www.pacfish.ca/wcviweather/Content%20Pages/" +
              name + "/WaterLevel.aspx") for name in names]
# Turning it into a dictionary to allow for indexing by name later
hyd_links = dict(zip(names, hyd_links))

# The same for the temperature links
temp_links = [("http://www.pacfish.ca/wcviweather/Content%20Pages/" +
              name + "/Temperature.aspx") for name in names]
temp_links = dict(zip(names, temp_links))

# Storing these themselves in a dict to allow for appropriate naming during the cleaning stage
links = {'Hydrometric': hyd_links, 'Temperature': temp_links}

# Checking that the urls are valid --------
hyd_codes = {name: requests.get(
    link).status_code for name, link in hyd_links.items()}
print("Hydrometric links checked")
temp_codes = {name: requests.get(
    link).status_code for name, link in temp_links.items()}
print("Temperature links checked")

# Creating dictionaries storing the successful data scrape status associated
# with each link. For any links that failed the validity check, setting their
# sucess status to False
hyd_success = {name: "success" if code ==
               200 else "Error: link invalid" for name, code in hyd_codes.items()}
# Were all links valid?
hyd_all_valid = all(
    [valid_check == "success" for valid_check in hyd_success.values()]
)
# Printing status
print("Hydrometric links all valid:", hyd_all_valid)

# Same for temperature links
temp_success = {name: "success" if code ==
                200 else "Error: link invalid" for name, code in temp_codes.items()}
temp_all_valid = all(
    [valid_check == "success" for valid_check in temp_success.values()]
)
print("Temperature links all valid: ", temp_all_valid)

# Saving them both in a single dict to make indexing easier later
success_status = {'Hydrometric': hyd_success, 'Temperature': temp_success}

# Removing failed links from iteration, if present --------

# Filtering the links dictionaries based on their validity status
links['Hydrometric'] = {name: link for name,
                        link in links['Hydrometric'].items() if hyd_success[name] == 'success'}
links['Temperature'] = {name: link for name,
                        link in links['Temperature'].items() if temp_success[name] == 'success'}

# %% ==== Iterating over each valid link and appending it's data ====

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
            df = formatStationDat(df, url_grp, url_name, ref_tab)
            # Ensuring types are consistently set
            df = df.astype(dtype_dict)

            # Doing an anti-join with existing set of recent data, to ensure
            # overlaps are removed. To do so, first a left join with an 'indicator'
            # is needed
            left_joined = df.merge(dat_tab, how='left', indicator=True)
            # Keeping only those df rows where the merge indicator says "left_only"
            left_joined = left_joined[left_joined._merge == "left_only"]
            df = left_joined.drop(columns="_merge")

            # Rearranging columns to match specification
            df = df[['STATION_NUMBER', 'STATION_NAME', 'Date', 'Time',
                    'Value', 'Parameter', 'Code', 'Comments']]

            # Appending these rows to the complete csv
            df.to_csv('data/pacfish_stations.csv',
                      mode='a', header=False, index=False)

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
    print('===== Pacfish Hydrometric and Temperature Data Scraper =====', file=f)
    print('', file=f)
    print('This file gives a summary of the most recent pacfish data update, run via the script "pacfish_scraping.py"', file=f)
    print('', file=f)
    # Date of scraping attempt
    print('Last scrape Date: ', str(datetime.now()), file=f)
    # Whether all links were valid
    print('All hydrometric links valid:', hyd_all_valid, file=f)
    print('All temperature links valid:', temp_all_valid, file=f)
    print('', file=f)
    # Station-wise status for Hydrometric data (formatted as a dataframe for easy reading)
    print('Hydrometric data station completion status:', file=f)
    print(pd.DataFrame.from_dict(success_status['Hydrometric'], orient='index').rename(
        columns={0: "Status"}), file=f)
    print('', file=f)
    # Station-wise status for temperature data
    print('Temperature data station completion status:', file=f)
    print(pd.DataFrame.from_dict(success_status['Temperature'], orient='index').rename(
        columns={0: "Status"}), file=f)
