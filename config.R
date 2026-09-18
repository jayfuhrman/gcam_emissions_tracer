
#local PC
FOLDER_LOCATION <- 'C:/Users/fuhr472/Documents/stash/emissions-tracer/'

#pic
#FOLDER_LOCATION <- '/qfs/people/fuhr472/wrk/gcam_emissions_tracer/'

RGCAM <- TRUE # True if using rgcam, false if using query file

GHG_EMISSIONS_OUTPUT <- 'output/all_ghg_emissions-GCAM_CurrentPolicy.csv'
NON_GHG_EMISSIONS_OUTPUT <- 'output/non_ghg_emissions-GCAM_CurrentPolicy.csv'
SEQUESTRATION_OUTPUT <- 'output/sequestration-GCAM_CurrentPolicy.csv'
LANDUSE_CHANGE_OUTPUT <- 'output/landuse_change-GCAM_CurrentPolicy.csv'

QUERY_FILE <- 'queries.xml'

WIDE_FORMAT <- TRUE

##### DEBUG TOGGLES FOR A SINGLE REGION AND YEAR #####

DEBUG <- FALSE
DEBUG_SCENARIO <- 'CurPol'
DEBUG_REGION <- c('China')
DEBUG_YEAR <- c(2025)


# The packages below are needed for the calculations
# You can uncomment and run the following line if you need to install them:
# install.packages(c("tibble", "dplyr", "tidyr", "stringr", "readr", "rgcam"))
library(rgcam)
library(dplyr)
options(dplyr.summarise.inform = FALSE)
library(tidyr)
library(stringr)
library(readr)

setwd(FOLDER_LOCATION)


# SET THIS VARIABLES IF USING QUERY CSV OUTPUT
if(!RGCAM){
  QUERY_FILE <- "output/queryout-emisstracer.csv"
}
options(scipen = 999)
# SET THESE VARIABLES IF USING RGCAM
if(RGCAM){
  DATABASE_LOCATION <- FOLDER_LOCATION
  
  DATABASE_FOLDER <- 'db'
  
  DATABASE_NAME <- 'database_basexdb'
  
  SCENARIO_NAME <- 'ALL' # Use 'ALL' to indicate query all scenarios in a db
  
  QUERY_RESULTS_LOCATION <- 'output/database_basexdbactivity.dat'
}