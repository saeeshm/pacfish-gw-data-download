# Author: Saeesh Mangwani
# Date: 2021-05-17

# Description: Cleaning the pacfish data archive to keep only the most recent
# 2 files

# ==== Loading libraries ====
library(tidyverse)
library(lubridate)

# Getting available files and naming them by their datestamps
fnames <- list.files('data/archive', pattern = ".csv$") %>% 
  setNames(str_extract(., "\\d{4}-\\d{2}-\\d{2}"))
# Getting the update reports as well
rnames <- list.files('data/archive', pattern = ".txt$") %>% 
  setNames(str_extract(., "\\d{4}-\\d{2}-\\d{2}"))

# Sorting names and selecting only the most recent 2
dates <- names(fnames) %>% 
  ymd() %>% 
  sort() %>% 
  tail(2)

# Indexing the list to select only those files dated before these 2
fnames <- fnames[!(ymd(names(fnames)) %in% dates)]
rnames <- rnames[!(ymd(names(rnames)) %in% dates)]

# Removing these files (if there are any to remove)
if (length(fnames) > 0) file.remove(paste0('data/archive/', fnames))
if (length(rnames) > 0) file.remove(paste0('data/archive/', rnames))


