#!/bin/sh

# Activating the pacfish environment
conda activate gwenv

# Navigating to initialization directory
cd ~/itme/code/GWProjects/databases/pacfish-hydrometric

# Updating station metadata (if new stations have been added, this scripts downloads the full archive for them as well)
python scripts/reset/01_pacfish_update_station_data.py

# Running the selenium update script, which takes an argument for how many days of data to download. Defaults to 31 (1 month)
python scripts/update/01_pacfish_update_selenium.py -d 31

# Creating downstream databases
Rscript scripts/update/03_pacfish_create_ancil_dbases.R

