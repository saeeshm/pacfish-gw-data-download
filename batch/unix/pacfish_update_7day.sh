#!/bin/sh

# Activating the pacfish environment
conda activate gwenv

# Navigating to initialization directory
cd ~/itme/code/GWProjects/databases/pacfish-hydrometric

# Updating station metadata (if new stations have been added, this scripts downloads the full archive for them as well)
python scripts/reset/01_pacfish_update_station_data.py

# Running the 7-day data update script
python scripts/update/01_pacfish_update_7-day.py

# Creating downstream databases
Rscript scripts/update/03_pacfish_create_ancil_dbases.R