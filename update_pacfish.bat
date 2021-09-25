:: Calling the activation script to run conda
call C:\Users\OWNER\miniconda3\Scripts\activate.bat

:: Activating the pacfish environment
call conda activate gwenv

:: BC Observation wells
E:
cd E:\saeeshProjects\pacfish_scraping

:: Running the update script
call python pacfish_update_7-day.py

:: Running the ancilliary data creation script (mean daily and 1 yr)
Rscript pacfish_create_ancil_dbases.R --vanilla