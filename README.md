# Pacfish Hydrometric Data Download
A collection of scripts that download all well data from the [Pacfish Hydrometric Station Network](http://www.pacfish.ca/wcviweather/Default.aspx), collecting them into a PostgreSQL database. The program is designed to be running periodically to regularly update the database as new data becomes available.

## Limitations
The program is currently only set up to download available data based on a specified number of days starting from the present. Since each station began recording on a separate data, this script cannot be used to download the entire historical record for each station. This means that to initialize the database, users will manually need to download csv files for all stations and combine them into a single 'hourly' datatable. This program can then regularly update the database with new data.

## Structure
The program contains 3 primary files, for the following uses:

### pacfish_update_7-day.py
This script downloads the hydrometric record for the past 1 week for each station in the pacfish network. All data features available at the station are downloaded (i.e water level, water temperature and air temperature). It checks whether any downloaded data are already present in the existing database, filtering these out to ensure that only new data are appended to the database. It requires an existing PostgreSQL database to update.

### pacfish_update_selenium.py
This script allows the user to specify the number of days prior to today for which data should be downloaded. Since specifying the date range requires interacting with webpage elements, the scripts uses a selenium server to automate data download. A working installation of the [Firefox browser](https://www.mozilla.org/en-US/firefox/new/) as well a [geckodriver executable](https://github.com/mozilla/geckodriver/releases) is required to run this script. The number of days for which data are requested, as well as the path to the geckdriver executable must be passed using command line arguments:
```
cd /path/to/workingDir
python pacfish_update_selenium.py --days 30 --gecko "/path/to/geckodriver/geckodriver"
```
The script checks whether any downloaded data are already present in the existing database, filtering these out to ensure that only new data are appended to the database. It requires an existing PostgreSQL database to update.

### pacfish_create_ancil_dbases.R
The preceding two scripts update a data-table named `hourly` within the specified schema to contain all downloaded hourly data. This script generates two additional tables: `daily` contains the average records by day for each station. `hourly_recent` contains only the hourly data for the preceding 1 year. Both tables are generated within the same schema.

## Usage notes
A working installation of PostgreSQL is required for using this script. The database should contain a schema titled `pacfish` within which data will be added. A file titled `credentials.json` must be placed in the home directory, which contains the parameters for connecting to the Postgres database. This script can be structured as follows:
```
{
  "user": "<USERNAME>",
  "host": "<HOST IP: can be 'localhost'>",
  "port": "<PORT: default is usually 5432>",
  "dbname": "<DATABASE NAME>",
  "password": "<USER PASSWORD>"
}
```
The `credentials.json` file may optionally contain a parameter `"schema": "<SCHEMA NAME>"` which specifies the database schema. This parameter is required in case the preferred schema is named something other than `pacfish`.

The scripts can be called from the command prompt/terminal to run in the background. Usually, either of the python scripts are run first, followed by the R script, which creates the secondary data tables from the downloaded primary data. The easiest method is to put calls to both scripts in a single `.bat` or `.sh` file:
```
cd /path/to/workingDir

python pacfish_update_selenium.py

Rscript create_ancil_data.R --vanilla
```

For anyone wanting to avoid working with firefox or selenium, you can use the `pacfish_update_7-day.py` script to update the database weekly, which does not require a selenium server.
