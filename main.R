source('config.R')
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
    lapply(dplyr::filter, region %in% DEBUG_REGION, year %in% DEBUG_YEAR)
}

GWP <- readr::read_csv("input/GWP_AR5.csv")
sector_label <- readr::read_csv("input/sector_label.csv")
land_aggregation <- readr::read_csv("input/aggregated_land.csv")
###################  Fuel Tracing ###################
fuel_tracing <- energy_water_distributor(prj)


###################  CO2 Sequestration ###################
# Mapping from subsector to primary/direct
primary_map <- read_csv("input/sequestration_primary_map.csv") #%>%
#  filter(subsector != 'natural gas')

transport_rewrite <- read_csv('input/transport_rewrite.csv')

sequestration <- co2_sequestration_distributor(prj, fuel_tracing %>% filter(primary %in% c('crude oil','coal','natural gas','total biomass')), primary_map, WIDE_FORMAT) %>%
  left_join(transport_rewrite, by = c('enduse' = 'sector')) %>%
  mutate(enduse = if_else(!is.na(renamed),renamed,enduse)) %>%
  select(-renamed) %>%
  left_join(transport_rewrite, by = c('transformation' = 'sector')) %>%
  mutate(transformation = if_else(!is.na(renamed),renamed,transformation)) %>%
  select(-renamed) %>%
  group_by(scenario,region,direct,transformation,enduse,ghg,phase,CWF_Sector,Units) %>%
  summarize(across(where(is.numeric),\(x) sum(x, na.rm = TRUE))) %>%
  ungroup()
  

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



###################  Emission Calculation ###################

all_emissions <- emissions(CO2, CO2_bio, resource_CO2, nonCO2, LUC,
                           fuel_tracing, input_raw,
                           GWP, sector_label, land_aggregation, WIDE_FORMAT) %>%
  left_join(transport_rewrite, by = c('enduse' = 'sector')) %>%
  mutate(enduse = if_else(!is.na(renamed),renamed,enduse)) %>%
  select(-renamed) %>%
  left_join(transport_rewrite, by = c('transformation' = 'sector')) %>%
  mutate(transformation = if_else(!is.na(renamed),renamed,transformation)) %>%
  select(-renamed)

# all_emissions <- bind_rows(all_emissions, sequestration)

all_GHG_emission <- all_emissions %>% filter(Units != "Tg") %>%
  group_by(scenario,region,direct,transformation,enduse,ghg,phase,CWF_Sector,Units) %>%
  summarize(across(where(is.numeric),\(x) sum(x, na.rm = TRUE))) %>%
  ungroup()

non_GHG_emission <- all_emissions %>% filter(Units == "Tg") %>%
  group_by(scenario,region,direct,transformation,enduse,ghg,phase,CWF_Sector,Units) %>%
  summarize(across(where(is.numeric),\(x) sum(x, na.rm = TRUE))) %>%
  ungroup()

readr::write_csv(all_GHG_emission, GHG_EMISSIONS_OUTPUT)
readr::write_csv(non_GHG_emission, NON_GHG_EMISSIONS_OUTPUT)
readr::write_csv(sequestration, SEQUESTRATION_OUTPUT)

# sum((all_GHG_emission %>% filter(ghg == "CO2", `2015` <= 0))$`2015`)
# sequestration %>% filter(ghg == "Captured CO2")

###################  Land Transfers ###################
land_change <- land_change_tracker(prj, land_aggregation, WIDE_FORMAT)
readr::write_csv(land_change, LANDUSE_CHANGE_OUTPUT)



source("functions/activity_tracer.R")
elec_capacity_new <- getElecGenCapacity(prj,NEW_VINTAGE = TRUE)
elec_capacity_operational <- getElecGenCapacity(prj,NEW_VINTAGE = FALSE)
fleet_size_sales <- getTrnFleetSize(prj,NEW_SALES = TRUE)
fleet_size_stock <- getTrnFleetSize(prj,NEW_SALES = FALSE)


ACTIVITY_OUTPUT <- paste0('output/','CurPol','Activity.csv')
activity <- bind_rows(elec_capacity_new,
                      elec_capacity_operational,
                      fleet_size_sales,
                      fleet_size_stock) %>%
  select(scenario, region, Variable,
         as.character(c(1990, 2005, 2010, 2015, 2021, seq(2025, 2100, by = 5))))

readr::write_csv(activity,ACTIVITY_OUTPUT)




cat(paste("------------------------------------------",
          "FILE COMPLETED.",
          "------------------------------------------", sep="\n"))


################## Abatement Costs #####################
#library("gcamdata")
#source("functions/abatement_cost_calculator.R")

#all_emissions_cost <- all_emissions %>%
#  pivot_longer('2005':'2100',names_to='year') %>%
#  mutate(year = as.double(year))

#abatement_cost <- abatement_cost_calculator(prj,all_emissions_cost,count_upstream_emiss = TRUE)

#abatement_cost <- abatement_cost_calculator(prj,all_emissions,count_upstream_emiss = FALSE)



######### Full Upstream Energy Water #########

#energy_water_tracing <- energy_water_distributor(prj)
