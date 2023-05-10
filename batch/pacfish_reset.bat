:: Calling the activation script to run conda
call C:\Users\OWNER\miniconda3\Scripts\activate.bat

:: Activating the pacfish environment
call conda activate gwenv

:: Navigating to initialization directory
cd /d E:\saeeshProjects\databases\pacfish-hydrometric

:: Running the update script
python scripts\reset\01_pacfish_init_postgres_container.py

:: Downloading full archive to initialize the database
python scripts\initialize\02_pacfish_download_archive.py


:: Creating downstream databases
Rscript scripts\update\03_pacfish_create_ancil_dbases.R