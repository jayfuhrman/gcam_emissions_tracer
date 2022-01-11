#local PC
FOLDER_LOCATION <- 'C:/Users/fuhr472/Documents/Github/gcam_emissions_tracer/'

#pic
#FOLDER_LOCATION <- '/qfs/people/fuhr472/wrk/gcam_emissions_tracer/'

RGCAM <- TRUE # True if using rgcam, false if using query file

EMISSIONS_OUTPUT <- 'output/emissions-GCAM_CWF.csv'

LANDUSE_CHANGE_OUTPUT <- 'output/landuse_change-GCAM_CWF.csv'

WIDE_FORMAT <- TRUE

# SET THIS VARIABLES IF USING QUERY CSV OUTPUT
if(!RGCAM){ 
  QUERY_FILE <- "output/queryout-emisstracer.csv"
}

# SET THESE VARIABLES IF USING RGCAM
if(RGCAM){
  DATABASE_LOCATION <- FOLDER_LOCATION
  
  DATABASE_FOLDER <- 'db'
  
  DATABASE_NAME <- 'database_basexdb'
  
  SCENARIO_NAME <- 'ALL' # Use 'ALL' to indicate query all scenarios in a db
  
  QUERY_RESULTS_LOCATION <- 'output/emissions_CWF.dat'
}

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

source("functions.R")

###################  Getting Query Output ###################
if(RGCAM){
  setwd(DATABASE_LOCATION)
  if (file.exists(paste0(FOLDER_LOCATION, QUERY_RESULTS_LOCATION))){
    print("Database already queried. Opening data file...")
    prj <- rgcam::loadProject(paste0(FOLDER_LOCATION, QUERY_RESULTS_LOCATION))
    print("Data file opened.")
  } else {
    print("Querying database...")
    conn <- rgcam::localDBConn(DATABASE_FOLDER, DATABASE_NAME,maxMemory = '16g')
    if(SCENARIO_NAME == "ALL"){
      for (scenario in rgcam::listScenariosInDB(conn)$name){
        prj <- rgcam::addScenario(conn, paste0(FOLDER_LOCATION, QUERY_RESULTS_LOCATION), scenario,
                                  paste0(FOLDER_LOCATION, 'queries.xml'))
      }
      
    } else {
      prj <- rgcam::addScenario(conn, paste0(FOLDER_LOCATION, QUERY_RESULTS_LOCATION), SCENARIO_NAME,
                                paste0(FOLDER_LOCATION, 'queries.xml'))
    }
    
    print("Database queried.")
  }
  setwd(FOLDER_LOCATION)
}

if(!RGCAM){
  prj <- query_splitter(QUERY_FILE)
  print("Query file processed.")

}
###################  Fuel Tracing ###################
fuel_tracing <- fuel_distributor(prj)

###################  CO2 Sequestration ###################
# Mapping from subsector to primary/direct
primary_map <- read_csv("input/sequestration_primary_map.csv")

sequestration <- co2_sequestration_distributor(prj, fuel_tracing, primary_map, WIDE_FORMAT)
#readr::write_csv(sequestration, str_replace(EMISSIONS_OUTPUT, ".csv", "_sequestration.csv"))

###################  Emission Inputs ###################
#
# Load all inputs
#
nonCO2 <- rgcam::getQuery(prj, "nonCO2 emissions by subsector")

resource_nonCO2 <- rgcam::getQuery(prj, "nonCO2 emissions by resource production") %>%
  rename(sector = resource, subsector = subresource)

CO2 <- rgcam::getQuery(prj, "CO2 emissions by sector (no bio)") %>%
  rename(sector = `primary fuel`)

input <- rgcam::getQuery(prj, "inputs by subsector") %>%
  group_by(Units, scenario, region, sector, input, year) %>%
  summarise(value = sum(value)) %>%
  ungroup()

LUC <-  rgcam::getQuery(prj, "LUC emissions by LUT") %>%
  tidyr::separate(landleaf, into = c("sector", "details"), "_", extra = "merge") %>%
  group_by(Units, scenario, region, sector, year) %>%
  summarise(value = sum(value)) %>%
  ungroup() %>%
  mutate(Units = "MTC", ghg = "LUC CO2") %>%
  filter(year %in% distinct(CO2, year)$year)

GWP <- readr::read_csv("input/GWP_AR5.csv")

sector_label <- readr::read_csv("input/sector_label.csv")

land_aggregation <- readr::read_csv("input/aggregated_land.csv")

###################  Emission Calculation ###################


all_emissions <- emissions(CO2, nonCO2, LUC, fuel_tracing, GWP, sector_label, land_aggregation, WIDE_FORMAT)
all_emissions <- bind_rows(all_emissions,sequestration)

readr::write_csv(all_emissions, EMISSIONS_OUTPUT)

###################  Land Transfers ###################
land_change <- land_change_tracker(prj, land_aggregation, WIDE_FORMAT)
readr::write_csv(land_change, LANDUSE_CHANGE_OUTPUT)

cat(paste("------------------------------------------",
          "FILE COMPLETED.",
          "------------------------------------------", sep="\n"))

