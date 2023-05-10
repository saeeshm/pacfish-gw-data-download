:: Calling the activation script to run conda
call C:\Users\OWNER\miniconda3\Scripts\activate.bat

:: Activating the GW environment
call conda activate gwenv

:: Navigating to initialization directory
cd /d E:\saeeshProjects\databases\pacfish-hydrometric\

:: Running the update script
python scripts\update\01_pacfish_update_selenium.py -d 30

:: Creating downstream databases
Rscript scripts\update\03_pacfish_create_ancil_dbases.R