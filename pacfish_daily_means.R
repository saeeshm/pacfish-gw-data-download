# !usr/bin/env Rscript

# Author: Saeesh Mangwani
# Date: 2021-05-14

# Description: A script that takes the pacfish database and creates a separate
# daily mean database from it

# ==== Loading libraries ====
library(tidyverse)

# ==== Initializing global variables ====

# Path to input data
in_path <- "data/pacfish_stations.csv"

# Path to file where the data is to be appended (can be existing or new)
path_to_db <- paste0("data/pacfish_daily_mean.csv")

# ==== Creating a daily mean file from the data ====

# Reading data
pac_stations <- read_csv(in_path, col_types = "ccDtdcic")

# Helper function that selects the non-na value among a set of values if
# present, otherwise returns NA
get_unique <- function(x){
  na.omit(unique(x))[1]
}

# Creating daily means
daily_mean <- pac_stations %>% 
  # Grouping by stations and Date
  group_by(STATION_NUMBER, Parameter, Date) %>% 
  # Summarizing values
  summarise("STATION_NAME" = get_unique(STATION_NAME),
            "Value" = mean(Value), 
            "Code" = get_unique(Code),
            "Comments" = get_unique(Comments)) %>% 
  ungroup() %>% 
  # Where the code is 21, adding a comment that some date for this date are
  # estimated
  mutate(Comments = ifelse(Code == 21, 
                            "Some measurements on this date were estimated (i.e not direct observations)", 
                            Comments)) %>% 
  select(STATION_NUMBER, STATION_NAME, Date, Value, Parameter, Code, Comments)

# Reading any existing mean data and anti-joining to only get unique rows
if (file.exists(path_to_db)) {
  # Existing mean data
  curr_mean <- read_csv(path_to_db,  col_types = "ccDdcic")
  # Anti-joining to remove any overlapping values
  daily_mean <- anti_join(daily_mean, curr_mean, by = c("STATION_NUMBER", "Date"))
  # If there are no unique rows left, printing a status statement
  if (nrow(daily_mean) == 0) {
    print("All data already present in the table! Process terminated.")
  }else{
    write_csv(daily_mean, path_to_db, append = T)
  }
# If no existing data are present, just writing the file
}else{
  write_csv(daily_mean, path_to_db, append = F)
}