library(rgcam)
library(dplyr)
options(dplyr.summarise.inform = FALSE)
library(tidyr)
library(stringr)
library(readr)
library(ggplot2)

end_use = 'iron and steel'
#fuel_cycle_mappings <- read_csv('input/fuel_cycle_mappings.csv')

emissions_fuel_cycle <- function(all_emissions,end_use){

  all_emiss <- all_emissions %>%
    pivot_longer(`2005`:`2050`,names_to='year') %>%
    mutate(year = as.numeric(year),
           transformation = if_else(direct == 'natural gas' & transformation == enduse, 'gas processing',transformation)) %>%
    filter(!is.na(value),
           enduse == end_use,
           scenario %in% c('DOE-zero','GCAM-ref')) %>%
    select(scenario,region,direct,transformation,enduse,ghg,Units,phase,CWF_Sector,year,value) %>%
    group_by(scenario,region,direct,transformation,enduse,ghg,Units,phase,CWF_Sector,year) %>%
    summarize(value = sum(value)) %>%
    ungroup()
  
  nonCO2 <- getQuery(prj,'nonCO2 emissions by tech') %>%
    mutate(sector = if_else(str_detect(sector,'trn_'),subsector,sector)) %>%
    filter(sector == end_use) %>%
    rename(enduse = sector) 
  
  nonCO2_enduse <- nonCO2 %>%
    filter(ghg != 'CO2') %>%
    mutate(transformation = enduse) %>%
    group_by(scenario,region,year,enduse,ghg) %>%
    mutate(emiss_frac = value / sum(value)) %>%
    ungroup() %>%
    select(-value,-Units,-subsector)
  
  inputs_by_tech <- getQuery(prj,'inputs by tech') %>%
    mutate(sector = if_else(str_detect(sector,'trn_'),subsector,sector)) %>%
    rename(enduse = sector) %>%
    filter(enduse == end_use)
  
  ccoef_mapping <- read_csv('input/ccoef_mapping.csv') %>%
    mutate(PrimaryFuelCO2Coef = if_else(fuel == 'biomass',0,PrimaryFuelCO2Coef)) %>%
    rename(input = PrimaryFuelCO2Coef.name)
  
  CO2_seq <- getQuery(prj,'CO2 sequestration by tech') %>%
    mutate(sector = if_else(str_detect(sector,'trn_'),subsector,sector)) %>%
    filter(sector == end_use) %>%
    rename(enduse = sector) 
  
  CO2_enduse_by_tech <- inputs_by_tech %>%
    left_join(ccoef_mapping, by = c('input')) %>%
    filter(!is.na(PrimaryFuelCO2Coef),
           !(str_detect(input,'biomass'))) %>%
    mutate(transformation = if_else(fuel == 'natural gas','gas processing',
                                    if_else(fuel == 'refining',fuel,enduse)),
           c_input = value * PrimaryFuelCO2Coef,
           ghg = 'CO2') %>%
    select(-value) %>%
    group_by(scenario,region,year,enduse,subsector,technology,ghg) %>%
    mutate(tech_c_input = sum(c_input),
           frac_tech_c_input = c_input / tech_c_input) %>%
    ungroup() 
  
  
  CO2_enduse <- CO2_enduse_by_tech %>%
    left_join(CO2_seq %>% 
                select(-Units) %>%
                rename(seq = value), by = c('scenario','region','year','enduse','subsector','technology')) %>%
    mutate(seq = if_else(is.na(seq),0,seq),
           seq = seq * frac_tech_c_input,
           emiss = c_input - seq) %>%
    group_by(scenario,region,year,enduse,transformation) %>%
    mutate(emiss_frac = emiss / sum(emiss)) %>%
    ungroup() %>%
    select(scenario,region,year,enduse,technology,ghg,emiss_frac,transformation)
  

  
  enduse_emiss_disag <- bind_rows(CO2_enduse,nonCO2_enduse) %>%
    left_join(all_emiss %>% filter(phase == 'enduse'),by = c('scenario','region','year','enduse','transformation','ghg')) %>%
    filter(year >= 2005) %>%
    mutate(value = value * emiss_frac) %>%
    select(scenario,region,enduse,technology,ghg,year,transformation,direct,Units,phase,CWF_Sector,value)
  #disaggregated enduse emissions
    
  inputs_by_tech <- inputs_by_tech %>%
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
    filter(!is.na(value.x),
           technology != 'Hybrid Liquids') %>%
    mutate(value.x = value.x * 10^12,
           Units.x = if_else(Units.x == 'MTCO2e','grams CO2e','grams'),
           value.y = if_else(Units.y %in% c('million pass-km','million ton-km'), value.y * 10^6, value.y * 10^12), #convert million km to km; else convert EJ to MJ; million tons to grams
           Units.y = if_else(Units.y == 'million pass-km', 'pass-km',
                             if_else(Units.y == 'million ton-km', 'ton-km',
                                     if_else(Units.y == 'EJ', 'MJ',
                                             if_else(str_detect(Units.y,'Mt'),'gram', NA_character_)))),
           value = value.x / value.y) %>%
    group_by(scenario,region,year,enduse,technology,ghg,transformation,Units.x,Units.y,phase,CWF_Sector) %>%
    summarize(value = sum(value)) %>%
    ungroup()

  
  
}