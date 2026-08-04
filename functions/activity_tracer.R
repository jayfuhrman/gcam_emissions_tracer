## Installing and loading packages

# install.packages('devtools')
# devtools::install_github('JGCRI/rgcam')
# install.packages('readxl')
# install.packages("writexl")


library("devtools")
library("rgcam")
#library("gcamdata")
library("tibble")

library('tidyr')
library('dplyr')
library("magrittr")
library("ggplot2")
library("stringr")
library("readxl")
library("writexl")

scenario_folder <- list.dirs(
  path = "input_data/GCAM_outputs",
  full.names = FALSE,
  recursive = FALSE
)


cwf_csv_import_all_fun <- function(data_type, scenario_list){

  data <- tibble()

  for (i in scenario_list){
    data_export <- read.csv(paste("input_data/GCAM_outputs", i, data_type, sep = "/"), skip = 1) %>%
      # somehow read in csv file add a column of NA to the last column, we need to remove it.
      select(-ncol(.)) %>%
      {setNames(., sub("^X", "", names(.)))} %>%
      as_tibble() %>%
      gather_years() %>%
      separate(scenario, into = c("scenario", "time"), sep = ",") %>%
      mutate(scenario = i) %>%
      mutate(scenario = substring(scenario, 7))

    data <- bind_rows(data, data_export)

  }

  data

}

# 1.  electricity capacity ----

getElecGenCapacity <- function(prj,NEW_VINTAGE) {

  ## input the capacity factor
  cf_data <- getQuery(prj,'elec capacity factor')
  
  # GCAM does not model hydro power endogenously (use exogenous data for hydro results), so there is no hydro capacity factor in the model. We just collect hydro power capacity factor from NREL ATB.
  hydro_make_up_cf_data <-
    cf_data %>%
    filter(technology == "biomass (conv CCS) (dry cooling)") %>%
    mutate(sector = "electricity",
           subsector = "hydro",
           technology = "hydro",
           value = 0.44)
  
  final_cf_data <- cf_data %>% rbind(hydro_make_up_cf_data) %>%
    select(-Units)
  
  ## input electricity generation by technology (new vintage)

  if (NEW_VINTAGE == TRUE){
      elec_gen_tech_new_raw <- getQuery(prj,'elec gen by gen tech and cooling tech (new)')
      VARIABLE = "Capacity Additions|Electricity|"
      DESCRIPTION = "Total annual new installation of electricity generation capacity from "
  } else if (NEW_VINTAGE == FALSE) {
      elec_gen_tech_new_raw <- getQuery(prj,'elec gen by gen tech')
      VARIABLE = "Capacity|Electricity|"
      DESCRIPTION = "Installed (available) capacity to generate electricity from "
  } else {
    stop("NEW_VINTAGE must be TRUE or FALSE")
  }
  
  elec_by_tech_new_vintage <-
    elec_gen_tech_new_raw %>%
    filter(Units == "EJ") %>%
    # In GCAM, hydro power is based on exogenous input and rooftop PV does not account for vintage (meaning the generation result in this query is based on total capacity, instead of new capacity).
    # Here, we just calculate the a factor (0.135 for rooftop pv, and 0.025 for hydro) that represent the percent of total generation being the generation from new capacity (based on average
    # average generation addition in each model period).
    mutate(new_vin_updated = ifelse(subsector %in% c("rooftop_pv", "rooftop_pv_mineral"), value*0.135,
                                    ifelse(subsector == "hydro", value*0.025, value))) %>%
    select(Units, scenario, region, subsector, technology, year, new_vin_updated)
  
  
  # calculate the new capacity for generation technologies
  elec_new_capacity <-
    elec_by_tech_new_vintage %>%
    left_join(final_cf_data %>% mutate(year = if_else(year == 2020, 2021, year)), by = c("scenario","region", "technology", "year")) %>%
    select(Units, scenario, region,
           subsector = subsector.x, technology, year, new_vin_EJ = new_vin_updated, cf_value = value) %>%
    unique() %>%
    mutate(new_capacity_TW = (new_vin_EJ * 277.78)/(8760 * cf_value),
           Units = "TW",
           new_capacity_TW = if_else(is.na(new_capacity_TW),0,new_capacity_TW)) %>%
    select(-new_vin_EJ, -cf_value) %>%
    filter(!technology %in% c('PV','PV_storage','wind','wind_offshore','wind_storage')) 
  
  
  elec_capacity_global <- elec_new_capacity %>%
    group_by(scenario,year,subsector,Units) %>%
    summarize(value = sum(new_capacity_TW)) %>%
    ungroup() %>%
    mutate(region = "Global")
  
  
  elec_capacity <- pivot_wider(elec_new_capacity %>%
                                 mutate(value = new_capacity_TW) %>%
                                 bind_rows(elec_capacity_global) %>%
                                 mutate(value = if_else(year == 2025 & NEW_VINTAGE == TRUE, value / 4, 
                                                        if_else(year > 2025 & NEW_VINTAGE == TRUE, value / 5, value))) %>%
                                 filter(year>=2025)%>%
                                 group_by(scenario,region,year,subsector,Units) %>%
                                 summarize(value = sum(value)) %>%
                                 ungroup(), names_from = "year",values_fill = 0) %>%
    mutate(subsector = if_else(str_detect(subsector,'_pv|pv_'),'solar',subsector),
           subsector = if_else(str_detect(subsector,'wind'),'wind',subsector),
           Variable = paste0(VARIABLE,subsector),
           Description = paste0(DESCRIPTION,subsector)) %>%
    select(-subsector) %>%
    group_by(scenario,region,Variable,Description) %>%
    summarise(across(where(is.numeric), sum, na.rm = TRUE)) %>%
    ungroup()
  
  
  return(elec_capacity)


}



getTrnFleetSize <- function(prj,NEW_SALES){
  
  trn_annual_travel_data <-
    readr::read_csv("input/trn_annual_travel_data.csv") %>%
    as_tibble() %>%
    # select(-ncol(.)) %>%
    {setNames(., sub("^X", "", names(.)))} %>%
    pivot_longer(`1975`:`2100`,names_to = "year") %>%
    mutate(year = as.numeric(year))
  
  trn_load_factor_data <- getQuery(prj,'transport load factors')
  
  if (NEW_SALES == TRUE){
  ## input the vehicle service output by technology (new vintage)
    trans_ser_by_tech_new_raw <- getQuery(prj,'transport service output by tech (new)')
    VARIABLE = "Sales|Transportation|"
    DESCRIPTION = "Annual Sales of "
  } else if (NEW_SALES == FALSE) {
    trans_ser_by_tech_new_raw <- getQuery(prj,'transport service output by tech')
    VARIABLE = "Stock|Transportation|"
    DESCRIPTION = "Stock of "
  } else {
    stop("NEW_SALES must be TRUE or FALSE")
  }
  
  tran_ser_tech_fleet_size_new <-
    trans_ser_by_tech_new_raw %>%
    select(Units, scenario, region, sector, subsector, technology, year, value) %>%
    ## remove all the pass through technologies to avoid double counting
    ## get rid of "walk"
    filter(!grepl("_pass", technology),
           technology != "Walk") %>%
    left_join(trn_annual_travel_data, by = c("sector" = "supplysector", "subsector", "technology", "year")) %>%
    select(scenario, region, sector, subsector, technology, year,
           service_Units = Units, service_value = value.x,
           annual_travel_units = unit, annual_travel_value = value.y) %>%
    left_join(trn_load_factor_data %>% rename(loadFactor = value),
              by = c("scenario","region", "sector", "subsector", "technology", "year")) %>%
    ## Cycle does not have load factor, we add load factor for cycle = 1 manually
    mutate(loadFactor = if_else(technology == "Cycle", 1, loadFactor),
           stock = service_value/(annual_travel_value * loadFactor),
           Units = "Million units") %>%
    select(scenario, region, sector, subsector, technology, year, Units, value = stock)
  
  
  trn_fleet_size_global <- tran_ser_tech_fleet_size_new %>%
    group_by(scenario,sector,subsector,technology,year,Units) %>%
    summarize(value = sum(value)) %>%
    ungroup() %>%
    mutate(region = "Global")
  
  
  trn_fleet_size <- tran_ser_tech_fleet_size_new %>%
    bind_rows(trn_fleet_size_global) %>%
    filter(year != 2020) %>%
    mutate(value = if_else(year == 2025 & NEW_SALES == TRUE, value / 4, 
                           if_else(year > 2025 & NEW_SALES == TRUE, value / 5, value))) %>% #get average annual sales between timesteps
    pivot_wider(names_from = "year",values_fill = 0) %>%
    mutate(technology = if_else(str_detect(technology,"bev"),"BEV",technology),
           subsector = case_when(str_detect(subsector, "fret_road_ht") ~ "Heavy truck", TRUE ~ subsector),
           subsector = case_when(str_detect(subsector, "fret_road_mt") ~ "Medium truck", TRUE ~ subsector),
           subsector = case_when(str_detect(subsector, "fret_road_lt") ~ "Light truck", TRUE ~ subsector),
           subsector = case_when(subsector== "car_bev" ~ "Car", TRUE ~ subsector),
           subsector = case_when(subsector== "largecar_bev" ~ "Large Car and Truck", TRUE ~ subsector),
           subsector = case_when(subsector== "minicar_bev" ~ "Mini Car", TRUE ~ subsector),
           subsector = case_when(subsector== "bus_bev" ~ "Bus", TRUE ~ subsector),
           subsector = case_when(str_detect(subsector, "Car") ~ "Car", TRUE ~ subsector),
           subsector = case_when(str_detect(subsector, "truck") ~ "Truck", TRUE ~ subsector),
           subsector = case_when(str_detect(subsector, "HSR") ~ "Passenger Rail", TRUE ~ subsector),
           Variable = paste0(VARIABLE,subsector,"|",technology),
           Description = paste0(DESCRIPTION,subsector," with ",technology," drive type")) %>%
    select(-sector,-subsector,-technology)
  
  return(trn_fleet_size)
  
  
}







