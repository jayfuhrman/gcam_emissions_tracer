WHAT DOES THE CODE DO?

The R code will run the queries in "queries.xml" on a designated GCAM database. Then, it will trace primary fuels through the energy system and use that result to distribute indirect emissions. There is also an added function that prints out the landuse changes from one landuse type to another for each region. 

--------------------------------------------------------------------------

HOW DO I RUN THE CODE?

Open "main.R". You should not need to use "functions.R", where all of the calculations are performed. Assuming all packages used in the code are installed properly, you should only need to set the variables at the start of the file. All variables should be between single or double quotes. Then, in R Studio you can press Ctrl+Shift+Enter to run the entire file (to run only one line, press Ctrl+Enter). It should output two csv files.

Variables to select:

- FOLDER_LOCATION is the path to the location of the folder (GCAMEmissionsTracer) that contains the .R files, queries.xml, and the input and output folders. 
- DATABASE_FOLDER is the path to the folder two above the database. rgcam seems to prefer this (ie "/pic/project/horowitz/CWF/")
- DATABASE_LOCATION is the path to the folder that contains the database you will be querying from, not including the "database_basexdb" folder or something similar (ie "db/")
- DATABASE_NAME is the name of the database folder, usually "database_basexdb"
- SCENARIO_NAME is the name of the scenario in the database, not including the date and time stamp. If you enter "ALL" here, then the code will run on all scenarios in the designated database.
- QUERY_RESULTS_LOCATION is the path to the file where the query results will be stored. This prevents having to query the same database twice. I recommend always keeping this at "output/CHOOSE_A_NAME.dat". You need to change this every time you run the code on a new database.
- EMISSIONS_OUTPUT is the file path where the emissions csv output prints to. This can be anywhere you'd like, but must end with ".csv".
- LANDUSE_CHANGE_OUTPUT is the file path where the landuse change csv output prints to. This can be anywhere you'd like, but must end with ".csv".
- WIDE_FORMAT is set to TRUE. If you'd like long format, enter FALSE (no quotes)

--------------------------------------------------------------------------

WHAT IF I DON'T HAVE ALL THE NECESSARY PACKAGES INSTALLED?

Uncomment line 19 (install.packages(c("tibble", "dplyr", "tidyr", "stringr", "readr", "rgcam"))) and run it by pressing Ctrl+Enter with your cursor on line 19. If you are asked to install packages from source, you can type "Yes" into the console. If everything installs properly, you can comment out this line again by putting a "#" before the line. 

