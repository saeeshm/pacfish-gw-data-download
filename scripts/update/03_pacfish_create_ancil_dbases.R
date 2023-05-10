# Author: Saeesh Mangwani
# Date: 2021-06-02

# Description: A script that generates the ancilliary data files from the
# hourly observation well data (scraped by the pacfish_update_*.py script)

# ==== Loading libraries ====
library(DBI)
library(RPostgres)
library(lubridate)
library(rjson)

# ==== Reading data ====

# Credentials files
creds <- fromJSON(file = 'options/credentials.json')

# Setting default schema unless pre-specified
if (is.null(creds$schema)) creds$schema <- 'pacfish'

# Opening database connection
conn <- dbConnect(RPostgres::Postgres(),
                  host = creds$host, dbname = creds$dbname,
                  user = creds$user, password = creds$password)

# ==== Daily mean dataset ====

# Dropping the table if it exists
dbExecute(conn, paste0('drop table if exists ', creds$schema, '.daily'))

# Create the daily mean table from the temp table
dbExecute(conn, paste0('create table ', creds$schema, '.daily as (
        	select "STATION_NUMBER", max("STATION_NAME") as "STATION_NAME",
            "Date", avg("Value") as "Value", count("Date") as "numObservations",
            "Parameter"
          from ', creds$schema, '.hourly
          group by "STATION_NUMBER", "Date", "Parameter"
          )
          '))

# ==== Past 1-year dataset ====

# Dropping table if it exists
dbExecute(conn, paste0('drop table if exists ', creds$schema, '.hourly_recent'))

# Specifying the timestamp for 1-year ago
date_filter <- ymd((Sys.Date() - 366))

# Creating a past 1 year dataset from the temptable
dbExecute(conn, 
          paste0('create table ', creds$schema, '.hourly_recent as (', 
          'select *
          from ', creds$schema, '.hourly where "Date" >= ',
          "'", date_filter, "'",
          ')'))

# ==== Disconnecting ====
dbDisconnect(conn)
