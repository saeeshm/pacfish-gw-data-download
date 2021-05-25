# Author: Saeesh Mangwani
# Date: 04/04/2021

# Description: A script for formatting Pacfish raw downloaded data into GW's format

# ==== Loading libraries ====
library(tidyverse)
library(tsibble)
library(readxl)
library(openxlsx)
library(lubridate)

# ==== Defining global variables ====

# Path to file where the data is to be appended (can be existing or new)
path_to_db <- paste0("data/pacfish_stations.csv")

# Path to folder containing raw data
path_to_raw <- "data/Pacfish_RAW_Data"

# Which variables are being cleaned
vars <- setNames(c("Water Level", "Temperature"), c("level", "temp"))

# ==== Reading data ====

# A function that gets and names the file paths to the hydrometric and
# temperature data
getNamedPaths <- function(var){
  # Removing any spaces from the var passed
  var <- str_remove_all(str_squish(var), "\\s")
  # Creating the regex pattern used for parsing the file names
  rpat <- paste0("(",var, "HydrometData|", var, "From).*\\.xlsx$")
  
  # Getting complete filepaths for the data
  fnames <- list.files(path_to_raw, pattern = rpat, full.names = T)
  # Getting just the filenames for the same data
  fnames_short <- list.files(path_to_raw, pattern = rpat)
  
  # Setting names to this vector by editing filenames to only keep the station
  # identifier, in the same format as the station names vector above
  names(fnames) <- fnames_short %>% 
    # Removing non-station stuff using the same regex string as above
    str_remove(rpat) %>%
    # Turning to lowercase, all spaces removed to ease indexing
    tolower() %>% 
    str_squish() %>% 
    str_remove_all("\\s")
  # Reording them so that they are ordered alphabetically by their names
  fnames <- fnames[factor(names(fnames))]
  # Returning
  return(fnames)
}

# Using the function to read file paths for hydrometric and temperature data
fnames <- map(vars, getNamedPaths)

# Reading each file into a (named) list of tables
ftables <- map_depth(fnames, 2, ~{
  print(.x)
  read_xlsx(.x, col_types = c("guess"))
  })

# Ensuring variables across all tables are named the same, and that time
# variables are formatted as character for now, to prevent parsing issues
# later
ftables <- map_depth(ftables, 2, ~{
  # Renaming
  # If the 3rd column is present
  if(!is.na(names(.x)[3])) {
    # Checking whether the dataframe is for temperature or level and naming
    # variables accordingly
    names(.x) <- ifelse(str_detect(names(.x)[2], "Temp"), 
                        list(c("Time", "Air Temperature", "Water Temperature")),
                        list(c("Time", "Water Level", "Sensor Depth")))[[1]]
  # If the 3rd column is missing, only naming for 2 columns
  }else{
    # Checking whether the dataframe is for temperature or level and naming
    # variables accordingly
    names(.x) <- ifelse(str_detect(names(.x)[2], "Temp"), 
                        list(c("Time", "Water Temperature")),
                        list(c("Time", "Water Level")))[[1]]
  }
  
  # Ensuring the time variable is character
  .x$Time <- as.character(.x$Time)
  # Reparsing it to a date based on 2 format specifications
  .x$Time <- parse_date_time(.x$Time, orders = c("ymd HMS", "mdy HMS p"))
  # Removing times that failed to parse (these are rows that don't contain
  # actual data just a clarifying message)
  return(.x %>% filter(!is.na(Time)))
  return(.x)
})

# Reading the reference dataset to get the actual station names and station
# numbers
name_ref <- read_xlsx("data/Pacfish Monitoring stations 2021.xlsx")
# Removing rows that aren't present and only keeping relevant cols
name_ref <- name_ref %>% 
  select(STATION_NAME, STATION_NUMBER) %>% 
  arrange(STATION_NAME) %>%
  filter(!str_detect(tolower(STATION_NAME), "burman|shaw"))

# Created a named vector of actual station names using the shortened
# station names shown here
stat_names <- setNames(name_ref$STATION_NAME, names(fnames$level))
# Creating a vector of station numbers using the reference dataset
stat_nums <- setNames(name_ref$STATION_NUMBER, names(fnames$level))

# ==== Formatting data to specification ====

# Iterating over all the tables and formatting them according to the
# specification. This iteration additionally combines all of them into a
# single dataframe
gage_df <- map_dfr(ftables, function(x){
  imap_dfr(x, ~{
    # First splitting the dataframe to 2 parts to get the 2 parameters processed
    # separately
    df1 <- .x[,c(1,2)]
    # Getting the name of the parameter associated with that dataframe
    df1_param <- names(df1)[2]
    # Renaming the 2 dfs to make further processing easier
    names(df1) <- c("Time", "Value")
    
    # If df2 is valid (i.e  a second stream of data is in fact present, doing
    # the same for this)
    if(length(.x) > 2){
      df2 <- .x[,c(1,3)]
      df2_param <- names(df2)[2]
      names(df2) <- c("Time", "Value")
    }
    
    # Reformatting the dataframes
    df1 <- df1 %>% 
      # Adding a variable called Code that flags whether the data are estimated or
      # measured
      mutate(Code = ifelse(str_detect(Value, "Estimated"), 21, NA_integer_)) %>% 
      # Removing the "**Estimated" flag and converting to numeric
      mutate(Value = str_remove(Value, "\\*\\*Estimated")) %>% 
      mutate(Value = as.numeric(Value)) %>% 
      # Separating the timestamp into Date and Time columns
      mutate(Date = date(Time)) %>% 
      mutate(Time = hms::as_hms(Time)) %>% 
      # Adding a "Parameter" Column that stores what this parameter is
      mutate(Parameter = df1_param) %>% 
      # Adding a station number column that contains the number associated with
      # this station
      mutate(STATION_NUMBER = stat_nums[.y]) %>% 
      # Adding a station name column that contains the full station name by
      # indexing the name vector created above
      mutate(STATION_NAME = stat_names[.y]) %>%
      # Creating an empty comments column
      mutate(Comments = NA_character_) %>% 
      select(STATION_NUMBER, STATION_NAME, Date, Time, Value,
             Parameter, Code, Comments)
    
    if (exists('df2', inherits = F)) {
      df2 <- df2 %>% 
        # Adding a variable called Code that flags whether the data are estimated or
        # measured
        mutate(Code = ifelse(str_detect(Value, "Estimated"), 21, NA_integer_)) %>% 
        # Removing the "**Estimated" flag and converting to numeric
        mutate(Value = str_remove(Value, "\\*\\*Estimated")) %>% 
        mutate(Value = as.numeric(Value)) %>% 
        # Separating the timestamp into Date and Time columns
        mutate(Date = date(Time)) %>% 
        mutate(Time = hms::as_hms(Time)) %>% 
        # Adding a "Parameter" Column that stores what this parameter is
        mutate(Parameter = df2_param) %>% 
        # Adding a station number column that contains the number associated with
        # this station
        mutate(STATION_NUMBER = stat_nums[.y]) %>% 
        # Adding a station name column that contains the full station name by
        # indexing the name vector created above
        mutate(STATION_NAME = stat_names[.y]) %>%
        # Creating an empty comments column
        mutate(Comments = NA_character_) %>% 
        # Selecting relevant variables in the right order
        select(STATION_NUMBER, STATION_NAME, Date, Time, Value,
               Parameter, Code, Comments)
      
      # Joining them by row to a single DF
      out_df <- bind_rows(df1, df2)
    # If there is no df1, out_df is set to only df1
    }else{
      out_df <- df1
    }
    # Returning the output df
    return(out_df)
  })
})

# Removing all other objects except the final df
rm(list = ls()[!ls() %in% c("out_format", "path_to_db", "gage_df")])
gc()

# Correcting the type on the Code column
gage_df$Code <- as.integer(gage_df$Code)

# Saving
write_csv(gage_df, path_to_db, na = '', append = F)
  

