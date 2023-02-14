library(rgcam)
library(dplyr)
options(dplyr.summarise.inform = FALSE)
library(tidyr)
library(stringr)
library(readr)
library(ggplot2)

end_use = 'iron and steel'
fuel_cycle_mappings <- read_csv('input/fuel_cycle_mappings.csv')

emissions_fuel_cycle <- function(all_emissions,end_use){

  all_emiss <- all_emissions %>%
    pivot_longer(`2005`:`2050`,names_to='year') %>%
    mutate(year = as.numeric(year)) %>%
    filter(!is.na(value),
           enduse == end_use,
           scenario %in% c('DOE-zero','GCAM-ref'))
  
  nonCO2 <- getQuery(prj,'nonCO2 emissions by tech') %>%
    mutate(sector = if_else(str_detect(sector,'trn_'),subsector,sector)) %>%
    filter(sector == end_use) %>%
    rename(enduse = sector) %>%
    group_by(scenario,region,year,enduse,ghg) %>%
    mutate(emiss_frac = value / sum(value)) %>%
    ungroup() %>%
    select(-value,-Units,-subsector)
  
  CO2_enduse <- nonCO2 %>%
    filter(ghg == 'CO2') %>%
    mutate(transformation = if_else(str_detect(technology,'Liquids'),'refining',
                                    if_else(technology %in% c('NG','gas'),'gas processing',enduse))) %>%
    group_by(scenario,region,year,enduse,transformation,ghg) %>%
    mutate(emiss_frac = emiss_frac / sum(emiss_frac))
  
  nonCO2_enduse <- nonCO2 %>%
    filter(ghg != 'CO2') %>%
    mutate(transformation = enduse)
  
  enduse_emiss_disag <- bind_rows(CO2_enduse,nonCO2_enduse) %>%
    left_join(all_emiss %>% filter(phase == 'enduse'),by = c('scenario','region','year','enduse','transformation','ghg')) %>%
    filter(year >= 2005) %>%
    mutate(value = value * emiss_frac) %>%
    select(scenario,region,enduse,technology,ghg,year,transformation,direct,Units,phase,CWF_Sector,value)
  #disaggregated enduse emissions
  
  inputs_by_tech <- getQuery(prj,'inputs by tech') %>%
    mutate(sector = if_else(str_detect(sector,'trn_'),subsector,sector)) %>%
    rename(enduse = sector) %>%
    filter(enduse == end_use) %>%
    mutate(transformation = if_else(str_detect(input,'elect_'),'electricity',
                                    if_else(str_detect(input,'H2'),'H2 production',
                                            if_else(str_detect(input,'gas'),'gas processing',
                                                    if_else(str_detect(input,'refined liquids'),'refining',enduse))))) %>%
    group_by(scenario,region,year,enduse,transformation) %>%
    mutate(input_frac = value / sum(value)) %>%
    ungroup()
  
  upstream_emiss <- all_emiss %>%
    filter(phase != 'enduse')
  
  upstream_emiss_disag <- inputs_by_tech %>%
    select(-value,-Units) %>%
    left_join(upstream_emiss, by = c('scenario','region','year','enduse','transformation')) %>%
    filter(year >= 2005) %>%
    mutate(value = value * input_frac) %>%
    select(scenario,region,enduse,technology,ghg,year,transformation,direct,Units,phase,CWF_Sector,value)
  
  disag_emiss_by_tech <- bind_rows(upstream_emiss_disag,enduse_emiss_disag)
  

  
  output <- rgcam::getQuery(prj, "outputs by tech") %>%
    mutate(sector = if_else(str_detect(sector,'trn_'),subsector,sector)) %>% 
    # The emissions tracer tool does not currently differentiate between liquids and hybrid liquids technologies 
    # so we group them together to avoid allocating all emissions to a small number of tkm / pkm and double counting
    # implicitly this reflects enhanced efficiency adoption over time for liquid fueled vehicles
    filter(sector == end_use,
           year >= 2005) %>%
    rename(enduse = sector) %>%
    mutate(year = as.numeric(year))

  fuel_cycle_emiss <- disag_emiss_by_tech %>%
    right_join(output, by = c('scenario','region','year','enduse','technology')) %>%
    filter(!is.na(value.x)) %>%
    mutate(value.x = value.x * 10^12,
           Units.x = if_else(Units.x == 'MTCO2e','grams CO2e','grams'),
           value.y = if_else(Units.y %in% c('million pass-km','million ton-km'), value.y * 10^6, value.y * 10^12), #convert million km to km; else convert EJ to MJ; million tons to grams
           Units.y = if_else(Units.y == 'million pass-km', 'pass-km',
                             if_else(Units.y == 'million ton-km', 'ton-km',
                                     if_else(Units.y == 'EJ', 'MJ',
                                             if_else(str_detect(Units.y,'Mt'),'gram', NA_character_)))),
           value = value.x / value.y)

  
  
}