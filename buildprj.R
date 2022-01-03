FOLDER_LOCATION <- '/qfs/people/fuhr472/wrk/GCAM_EmissionsTracer/'
#FOLDER_LOCATION <- 'C:/Users/fuhr472/OneDrive - PNNL/Documents/GitHub/cwf_emissions_tracer/'

DATABASE_LOCATION <- FOLDER_LOCATION

DATABASE_FOLDER <- 'db'

DATABASE_NAME <- 'tech_ensemble_db'

SCENARIO_NAME <- 'ALL' # Use 'ALL' to indicate query all scenarios in a db

#QUERY_RESULTS_LOCATION <- 'output/test.dat'

library(rgcam)
library(dplyr)
options(dplyr.summarise.inform = FALSE)
library(tidyr)
library(stringr)
library(readr)


#scenario_list <- c('SSP1_1p5','SSP1_2','SSP1_REF','SSP2_1p5','SSP2_2','SSP2_REF','SSP3_1p5','SSP3_2','SSP3_REF','SSP4_1p5','SSP4_2','SSP4_REF','SSP5_1p5','SSP5_2','SSP5_REF')
#scenario_list <- c('GCAM_CWF_V2.6_BIO2','GCAM_CWF_V2.6_BIO3')



print("Querying database...")
conn <- rgcam::localDBConn(DATABASE_FOLDER, DATABASE_NAME,maxMemory = '16g')
for (scenario in rgcam::listScenariosInDB(conn)$name){
  QUERY_RESULTS_LOCATION <- paste0('output/',scenario,'.dat')
  print(QUERY_RESULTS_LOCATION)
  prj <- rgcam::addScenario(conn, paste0(FOLDER_LOCATION, QUERY_RESULTS_LOCATION), scenario,
                            paste0(FOLDER_LOCATION, 'queries.xml'))
}
