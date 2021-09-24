# Author: Saeesh Mangwani
# Date: 2021-06-02

# Description: A script that generates the ancilliary data files from the
# hourly observation well data (scraped by the pacfish_update_*.py script)

# ==== Loading libraries ====
library(DBI)
library(RPostgreSQL)
library(lubridate)

# ==== Reading data ====

# Opening database connection
conn <- dbConnect("PostgreSQL", 
                  host = 'localhost', dbname = 'gws', 
                  user = 'saeesh', password = 'admin')

# ==== Daily mean dataset ====

# Dropping the table if it exists
dbExecute(conn, 'drop table if exists pacfish.daily')

# Create the daily mean table from the temp table
dbExecute(conn, 'create table pacfish.daily as (
        	select "STATION_NUMBER", max("STATION_NAME") as "STATION_NAME",
            "Date", avg("Value") as "Value", count("Date") as "numObservations",
            "Parameter"
          from pacfish.hourly
          group by "STATION_NUMBER", "Date", "Parameter"
          )
          ')

# ==== Past 1-year dataset ====

# Dropping table if it exists
dbExecute(conn, 'drop table if exists pacfish.hourly_recent')

# Specifying the timestamp for 1-year ago
date_filter <- ymd((Sys.Date() - 366))

# Creating a past 1 year dataset from the temptable
dbExecute(conn, 
          paste0('create table pacfish.hourly_recent as (', 
          'select *
          from pacfish.hourly where "Date" >= ',
          "'", date_filter, "'",
          ')'))