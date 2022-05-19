# Author: Saeesh Mangwani
# Date: 18/05/2022

# Description: Helper functions for database reset and initialization

import pandas as pd

def formatColNames(dat, url_grp):
    """
    Private function that formats column names for each dataframe by data type
    """
    if url_grp == "Hydrometric":
        col_names = (
            ['Time', 'Water Level', 'Sensor Depth'] 
            if dat.shape[1] == 3 
            else ['Time', 'Water Level']
        )
    elif(url_grp == 'Pressure'):
        col_names = ['Time', 'Pressure']
    else:
        col_names = (
            ['Time', 'Air Temperature', 'Water Temperature'] 
            if dat.shape[1] == 3 
            else ['Time', 'Water Temperature']
        )
    return col_names
    
def castDataColsToNumeric(dat, colnames):
    """
    Private function that formats numeric data columns to numeric by handling "estimated" tags
    """
    # Checking number of columns
    ncols = len(colnames)
    # If ncols is 3, there are 2 columns of data that need to be checked
    if ncols == 3:
        # Checking whether each column is of type string
        col1_is_str = pd.api.types.is_string_dtype(dat.iloc[:, 1])
        col2_is_str = pd.api.types.is_string_dtype(dat.iloc[:, 2])
        # If both contain "estimated" tags
        if col1_is_str and col2_is_str:
            print('Cleaning estimated tags from both columns')
            # Finding row locations for "estimated" data in each column
            estimated_index1 = dat.iloc[:,1].str.contains(pat="\*\*Estimated", regex=True)
            estimated_index2 = dat.iloc[:,2].str.contains(pat="\*\*Estimated", regex=True)
            estimated_index = estimated_index1 | estimated_index2
            # Assigning an estimated code (21) in a 'Code' column
            dat['Code'] = [21 if i else '' for i in estimated_index]
            # Removing estimated tags and converting to numeric
            dat.iloc[:,1] = dat.iloc[:,1].str.replace(pat="\*\*Estimated",repl='',regex=True).astype(float)
            dat.iloc[:,2] = dat.iloc[:,2].str.replace(pat="\*\*Estimated",repl='',regex=True).astype(float)
        # If only the first column is a string, replacing estimated tags in just that column
        elif col1_is_str:
            print('Cleaning estimated tags from column 1')
            estimated_index = dat.iloc[:,1].str.contains(pat="\*\*Estimated", regex=True)
            dat['Code'] = [21 if i else '' for i in estimated_index]
            dat.iloc[:,1] = dat.iloc[:,1].str.replace(pat="\*\*Estimated",repl='',regex=True).astype(float)
        # Same if only the second column is a string
        elif col2_is_str:
            print('Cleaning estimated tags from column 2')
            estimated_index = dat.iloc[:,2].str.contains(pat="\*\*Estimated", regex=True)
            dat['Code'] = [21 if i else '' for i in estimated_index]
            dat.iloc[:,2] = dat.iloc[:,2].str.replace(pat="\*\*Estimated",repl='',regex=True).astype(float)
        # If neither columns are string, creating an empty code column and ensuring data cols are numeric
        else:
            print('No cleaning of estimated data required')
            dat.iloc[:,1].astype(float)
            dat.iloc[:,2].astype(float)
            dat['Code'] = ''
    # If there is only 1 stream of data, checking only that column
    else:
        col1_is_str = pd.api.types.is_string_dtype(dat.iloc[:, 1])
        if col1_is_str:
            print('Cleaning estimated tags from column 1')
            # Checking where the "Estimated" tags are present in any of the data columns
            estimated_index = dat.iloc[:,1].str.contains(pat="\*\*Estimated", regex=True)
            # Using the index, assigning a value of 21 where there are estimates otherwise
            # leaving it empty, and assigning this to the code variable
            dat['Code'] = [21 if i else '' for i in estimated_index]
            dat.iloc[:,1] = dat.iloc[:,1].str.replace(pat="\*\*Estimated",repl='',regex=True).astype(float)
        else:
            print('No cleaning of estimated data required')
            dat.iloc[:,1].astype(float)
            dat['Code'] = ''
    # Returning the cleaned table with a new Code column
    return dat
    
def format_station_data(df, url_grp, url_name, ref_tab):
    """
    Formatting downloaded station data to GW format
    """
    # Creating a list of variable names based on the url group currently being
    # iterated over - if it is gage, using hydrometric names

    # Renaming columns
    df.columns = formatColNames(df, url_grp)
    
    # Formatting data types to numeric - inplace
    castDataColsToNumeric(df, df.columns)
    
    # Adding an empty column for comment
    df['Comments'] = ''

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
    df['STATION_NAME'] = ref_tab.loc[ref_tab['station_url_name'] == url_name].iloc[0, 0]
    df['STATION_NUMBER'] = ref_tab.loc[ref_tab['station_url_name'] == url_name].iloc[0, 1]

    # Returning the cleaned df
    return(df)

def get_urls_by_variable(var, ref_tab):
    """
    Getting urls for all stations that have data from the selected variable type
    """
    # Checking that variables is valid
    vartypes = ['staff_gauge', 'temp', 'pressure']
    errMessage = "var must be one of 'staff_gauge', 'temp', or 'pressure'"
    assert (var in vartypes), errMessage
    
    # Dictionary of datatype names
    queryVarNames = dict(zip(vartypes, ['WaterLevel', 'Temperature', 'Pressure']))

    # Filtering stations where this data type is available
    stat_names = ref_tab.loc[ref_tab[var]].station_url_name
    
    # Creating urls for this station
    data_links = [
        ("http://www.pacfish.ca/wcviweather/Content%20Pages/" +
         name + "/" + queryVarNames[var] + ".aspx") for name in stat_names]
    
    # Returning the links
    return dict(zip(stat_names, data_links))

def check_success_status(url_dict, check_all_valid = True):
    """
    Checking URL success status and returning a dictionary that contains 
    success status by station
    """
    success_dict = {
        name: "success" 
        if code == 200 
        else "Error: link invalid" 
        for name, code in url_dict.items()
    }
    
    if check_all_valid:
        allValid = all(
            [valid_check == "success" for valid_check in success_dict.values()]
        )
        return success_dict, allValid
    else:
        return success_dict
    
    