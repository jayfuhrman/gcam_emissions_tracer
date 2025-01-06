#local PC
FOLDER_LOCATION <- 'C:/GCAM_CWF_Model/gcam-emissions-tracer_gcam7/'

#pic
#FOLDER_LOCATION <- '/qfs/people/fuhr472/wrk/gcam_emissions_tracer/'

RGCAM <- TRUE # True if using rgcam, false if using query file

GHG_EMISSIONS_OUTPUT <- 'output/all_ghg_emissions-GCAM_cwf_central.csv'
NON_GHG_EMISSIONS_OUTPUT <- 'output/non_ghg_emissions-GCAM_cwf_central.csv'
SEQUESTRATION_OUTPUT <- 'output/sequestration-GCAM_cwf_central.csv'

LANDUSE_CHANGE_OUTPUT <- 'output/landuse_change-GCAM_HFTO.csv'

WIDE_FORMAT <- TRUE

##### DEBUG TOGGLES FOR A SINGLE REGION AND YEAR #####

DEBUG <- TRUE
DEBUG_SCENARIO <- 'cwf_central'
DEBUG_REGION <- 'USA'
DEBUG_YEAR <- 2060

# SET THIS VARIABLES IF USING QUERY CSV OUTPUT
if(!RGCAM){
  QUERY_FILE <- "output/queryout-emisstracer.csv"
}
options(scipen = 999)
# SET THESE VARIABLES IF USING RGCAM
if(RGCAM){
  DATABASE_LOCATION <- FOLDER_LOCATION
  
  DATABASE_FOLDER <- 'db'
  
  DATABASE_NAME <- 'cwf_central'
  
  SCENARIO_NAME <- 'ALL' # Use 'ALL' to indicate query all scenarios in a db
  
  QUERY_RESULTS_LOCATION <- 'output/test_run_cwf_central.dat' #temporary
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

source("functions/emissions_tracer.R")

###################  Getting Query Output ###################
if(RGCAM){
  setwd(DATABASE_LOCATION)
  if (file.exists(paste0(FOLDER_LOCATION, QUERY_RESULTS_LOCATION))){
    print("Database already queried. Opening data file...")
    prj <- rgcam::loadProject(paste0(FOLDER_LOCATION, QUERY_RESULTS_LOCATION))
    print("Data file opened.")
  } else {
    print("Querying database...")
    conn <- rgcam::localDBConn(DATABASE_FOLDER, DATABASE_NAME, maxMemory = '16g')
    if(SCENARIO_NAME == "ALL"){
      for (scenario in rgcam::listScenariosInDB(conn)$name){
        prj <- rgcam::addScenario(conn, paste0(FOLDER_LOCATION, QUERY_RESULTS_LOCATION), scenario,
                                  paste0(FOLDER_LOCATION, 'queries_g7.xml'))
      }

    } else {
      prj <- rgcam::addScenario(conn, paste0(FOLDER_LOCATION, QUERY_RESULTS_LOCATION), SCENARIO_NAME,
                                paste0(FOLDER_LOCATION, 'queries_g7.xml'))
    }

    print("Database queried.")
  }
  setwd(FOLDER_LOCATION)
}

# prj_all <- prj
# prj <- prj_all

if(!RGCAM){
  prj <- query_splitter(QUERY_FILE)
  print("Query file processed.")

}

if (DEBUG == TRUE){
  prj <- dropQueries(prj,'CO2 prices')
  
  prj <- dropScenarios(prj,c(DEBUG_SCENARIO), invert=TRUE)

  prj[[DEBUG_SCENARIO]] <- prj[[DEBUG_SCENARIO]] %>%
    lapply(dplyr::filter, region == DEBUG_REGION, year == DEBUG_YEAR)
}


###################  Fuel Tracing ###################
fuel_tracing <- energy_water_distributor(prj)

# fuel_tracing_all <- fuel_tracing
# fuel_tracing <- fuel_tracing_all

###################  CO2 Sequestration ###################
# Mapping from subsector to primary/direct
primary_map <- read_csv("input/sequestration_primary_map.csv") #%>%
#  filter(subsector != 'natural gas')

sequestration <- co2_sequestration_distributor(prj, fuel_tracing %>% filter(primary %in% c('crude oil','coal','natural gas','total biomass')), primary_map, WIDE_FORMAT)

###################  Emission Inputs ###################
#
# Load all inputs
#
nonCO2 <- 
  rgcam::getQuery(prj, "nonCO2 emissions by subsector (excluding resource production)")

resource_nonCO2 <- 
  rgcam::getQuery(prj, "nonCO2 emissions by resource production") %>%
  rename(sector = resource, subsector = subresource)

CO2 <- 
  rgcam::getQuery(prj, "CO2 emissions by sector (no bio) (excluding resource production)")

# CO2_tech <-
#   rgcam::getQuery(prj, "CO2 emissions by tech (excluding resource production)")


CO2_bio <- rgcam::getQuery(prj, "CO2 emissions by tech (excluding resource production)")

resource_CO2 <- 
  rgcam::getQuery(prj, "CO2 emissions by resource production ") %>%
  rename(sector = resource, subsector = subresource)

input_raw <- rgcam::getQuery(prj, "inputs by tech") 

input <- rgcam::getQuery(prj, "inputs by tech") %>%
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

all_emissions <- emissions(CO2, CO2_bio, resource_CO2, nonCO2, LUC, 
                           fuel_tracing, input_raw,
                           GWP, sector_label, land_aggregation, WIDE_FORMAT)

# all_emissions <- bind_rows(all_emissions, sequestration)

all_GHG_emission <- all_emissions %>% filter(Units != "Tg")
non_GHG_emission <- all_emissions %>% filter(Units == "Tg")

readr::write_csv(all_GHG_emission, GHG_EMISSIONS_OUTPUT)
readr::write_csv(non_GHG_emission, NON_GHG_EMISSIONS_OUTPUT)
readr::write_csv(sequestration, SEQUESTRATION_OUTPUT)

# sum((all_GHG_emission %>% filter(ghg == "CO2", `2015` <= 0))$`2015`)
# sequestration %>% filter(ghg == "Captured CO2")

###################  Land Transfers ###################
land_change <- land_change_tracker(prj, land_aggregation, WIDE_FORMAT)
readr::write_csv(land_change, LANDUSE_CHANGE_OUTPUT)

cat(paste("------------------------------------------",
          "FILE COMPLETED.",
          "------------------------------------------", sep="\n"))


################## Abatement Costs #####################
library("gcamdata")
#source("functions/abatement_cost_calculator.R")

#all_emissions_cost <- all_emissions %>%
#  pivot_longer('2005':'2100',names_to='year') %>%
#  mutate(year = as.double(year))

#abatement_cost <- abatement_cost_calculator(prj,all_emissions_cost,count_upstream_emiss = TRUE)

#abatement_cost <- abatement_cost_calculator(prj,all_emissions,count_upstream_emiss = FALSE)



######### Full Upstream Energy Water #########

#energy_water_tracing <- energy_water_distributor(prj)