FOLDER_LOCATION <- 'C:/Users/fuhr472/OneDrive - PNNL/Documents/Postdoc/Climateworks CDR/Files/GCAM_EmissionsTracer/'

DATABASE_LOCATION <- FOLDER_LOCATION

DATABASE_FOLDER <- 'db'

DATABASE_NAME <- 'database_basexdb'

SCENARIO_NAME <- 'ALL' # Use 'ALL' to indicate query all scenarios in a db

QUERY_RESULTS_LOCATION <- 'output/emissions_CWF.dat'

EMISSIONS_OUTPUT <- 'output/emissions-GCAM_CWF.csv'

LANDUSE_CHANGE_OUTPUT <- 'output/landuse_change-GCAM_CWF.csv'

WIDE_FORMAT <- TRUE

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
setwd(DATABASE_LOCATION)
if (file.exists(paste0(FOLDER_LOCATION, QUERY_RESULTS_LOCATION))){
  print("Database already queried. Opening data file...")
  prj <- rgcam::loadProject(paste0(FOLDER_LOCATION, QUERY_RESULTS_LOCATION))
  print("Data file opened.")
} else {
  print("Querying database...")
  conn <- rgcam::localDBConn(DATABASE_FOLDER, DATABASE_NAME)
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

###################  Fuel Tracing ###################
fuel_tracing <- fuel_distributor(prj)

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
readr::write_csv(all_emissions, EMISSIONS_OUTPUT)

###################  Land Transfers ###################
land_change <- land_change_tracker(prj, land_aggregation, WIDE_FORMAT)
readr::write_csv(land_change, LANDUSE_CHANGE_OUTPUT)

cat(paste("------------------------------------------",
          "FILE COMPLETED.",
          "------------------------------------------", sep="\n"))


