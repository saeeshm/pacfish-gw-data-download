:: Calling the activation script to run conda
call C:\Users\OWNER\miniconda3\Scripts\activate.bat

:: Activating the pacfish environment
call conda activate gwenv

:: Navigating to initialization directory
cd /d E:\saeeshProjects\databases\pacfish-hydrometric

:: Resetting or initialise the Postgres container
python scripts\reset\00_pacfish_init_postgres_container.py

:: Updating the station dataset, which in turns calls the station-reset script on every station that is new (which in the case of a reset, is all stations)
python scripts\reset\01_pacfish_update_station_data.py

:: Creating downstream databases
Rscript scripts\update\03_pacfish_create_ancil_dbases.R