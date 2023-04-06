library(rgcam)
library(dplyr)
options(dplyr.summarise.inform = FALSE)
library(tidyr)
library(stringr)
library(readr)
library(ggplot2)

#end_use = 'cement'

emissions_fuel_cycle <- function(all_emissions,end_use){
  
  if (end_use == 'process heat cement'){
    
    
    all_emiss <- all_emissions %>%
      pivot_longer(`2005`:`2050`,names_to='year') %>%
      mutate(year = as.numeric(year),
             transformation = if_else(direct == 'natural gas' & transformation == enduse, 'gas processing',transformation)) %>%
      filter(!is.na(value),
             enduse == 'cement') %>%
      select(scenario,region,direct,transformation,enduse,ghg,Units,phase,CWF_Sector,year,value) %>%
      group_by(scenario,region,direct,transformation,enduse,ghg,Units,phase,CWF_Sector,year) %>%
      summarize(value = sum(value)) %>%
      ungroup()
    
    all_emiss <- all_emiss %>%
      filter(transformation != 'calcination') %>%
      mutate(enduse = end_use)
    
  } else{

  
  all_emiss <- all_emissions %>%
    pivot_longer(`2005`:`2050`,names_to='year') %>%
    mutate(year = as.numeric(year),
           transformation = if_else(direct == 'natural gas' & transformation == enduse, 'gas processing',transformation)) %>%
    filter(!is.na(value),
           enduse == end_use) %>%
    select(scenario,region,direct,transformation,enduse,ghg,Units,phase,CWF_Sector,year,value) %>%
    group_by(scenario,region,direct,transformation,enduse,ghg,Units,phase,CWF_Sector,year) %>%
    summarize(value = sum(value)) %>%
    ungroup()
  }
  
  inputs_by_tech <- getQuery(prj,'inputs by tech') %>%
    mutate(sector = if_else(str_detect(sector,'trn_'),subsector,sector)) %>%
    rename(enduse = sector) %>%
    filter(enduse == end_use | enduse == paste0('process heat ', end_use)) 
    
  #mutate(enduse = end_use) %>%
    #group_by(Units,scenario,region,enduse,subsector,technology,input,year) %>%
    #summarize(value = sum(value)) %>%
    #ungroup()
  
  nonCO2 <- getQuery(prj,'nonCO2 emissions by tech') %>%
    mutate(sector = if_else(str_detect(sector,'trn_'),subsector,sector)) %>%
    filter(sector == end_use | sector == paste0('process heat ', end_use)) %>%
    #mutate(sector = end_use) %>%
    rename(enduse = sector) 
  
  nonCO2_enduse <- nonCO2 %>%
    filter(ghg != 'CO2') %>%
    mutate(transformation = enduse) %>%
    group_by(scenario,region,year,enduse,ghg) %>%
    mutate(emiss_frac = value / sum(value)) %>%
    ungroup() %>%
    select(-value,-Units,-subsector) %>%
    mutate(transformation = if_else(transformation == paste0('process heat ', end_use) & !(ghg %in% c('CH4', 'N2O')), end_use, 
                                    if_else(transformation == paste0('process heat ', end_use) & (ghg %in% c('CH4', 'N2O')), 'process heat',transformation)),
           transformation = if_else(transformation == paste0('process heat cement') & ghg %in% c('CH4', 'N2O'), 'process heat',transformation),
           enduse = end_use)
  
  ccoef_mapping <- read_csv('../input/ccoef_mapping.csv') %>%
    mutate(PrimaryFuelCO2Coef = if_else(fuel == 'biomass',0,PrimaryFuelCO2Coef)) %>%
    rename(input = PrimaryFuelCO2Coef.name)
  
  CO2_seq <- getQuery(prj,'CO2 sequestration by tech') %>%
    mutate(sector = if_else(str_detect(sector,'trn_'),subsector,sector)) %>%
    filter(sector == end_use | sector == paste0('process heat ', end_use)) %>%
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
    select(scenario,region,year,enduse,technology,ghg,emiss_frac,transformation) %>%
    mutate(transformation = if_else(str_detect(enduse,'process heat'), 'process heat', transformation),
           transformation = if_else(str_detect(transformation,'cement'), 'calcination', transformation),
           enduse = end_use)
  

  
  enduse_emiss_disag <- bind_rows(CO2_enduse,nonCO2_enduse) %>%
    left_join(all_emiss %>% 
                filter(phase == 'enduse'),by = c('scenario','region','year','enduse','transformation','ghg')) %>%
    filter(year >= 2005) %>%
    mutate(value = value * emiss_frac) %>%
    select(scenario,region,enduse,technology,ghg,year,transformation,direct,Units,phase,CWF_Sector,value)
  #disaggregated enduse emissions
    
  inputs_by_tech_upstream <- inputs_by_tech %>%
    filter(!str_detect(input,'process heat'),
           !str_detect(input,'limestone')) %>%
    mutate(transformation = if_else(str_detect(input,'elect_'),'electricity',
                                    if_else(str_detect(input,'H2'),'H2 production',
                                            if_else(str_detect(input,'gas'),'gas processing',
                                                    if_else(str_detect(input,'refined liquids'),'refining',enduse)))),
           transformation = if_else(str_detect(enduse,'process heat') & input %in% c('delivered biomass','delivered coal'), end_use, transformation)) %>%#,
    group_by(scenario,region,year,enduse,transformation) %>%
    mutate(input_frac = value / sum(value)) %>%
    ungroup()
  
  inputs_by_tech_upstream <- inputs_by_tech_upstream %>%
    bind_rows(inputs_by_tech_upstream %>% 
                filter(transformation == 'process heat') %>% 
                mutate(transformation = end_use)) %>%
    mutate(enduse = if_else(enduse == 'cement', 'process heat cement',enduse))
  
  upstream_emiss <- all_emiss %>%
    filter(phase != 'enduse') %>%
    mutate(enduse = if_else(enduse == 'cement', 'process heat cement',enduse),
           transformation = if_else(transformation == 'cement' & end_use == 'process heat cement', 'process heat cement', transformation))
    
  
  upstream_emiss_disag <- inputs_by_tech_upstream %>%
    select(-value,-Units) %>%
    left_join(upstream_emiss, by = c('scenario','region','year','enduse','transformation')) %>%
    filter(year >= 2005) %>%
    mutate(value = value * input_frac,
           enduse = if_else(enduse == 'process heat cement' & end_use == 'cement', 'cement', enduse)) %>%
    select(scenario,region,enduse,technology,ghg,year,transformation,direct,Units,phase,CWF_Sector,value)
  
  disag_emiss_by_tech <- bind_rows(upstream_emiss_disag,enduse_emiss_disag)
  
  process_heat_input_for_disag <- getQuery(prj,'inputs by tech') %>%
    filter(input == paste0('process heat ',end_use)) %>%
    group_by(scenario,region,year) %>%
    mutate(input_frac = value / sum(value)) %>%
    ungroup() %>%
    select(scenario,region,year,technology,input_frac)
  
  process_heat_disag <- disag_emiss_by_tech %>%
    filter(enduse %in% c('cement') & !str_detect(technology,'cement')) %>%
    select(-technology) %>%
    left_join(process_heat_input_for_disag, by = c('scenario','region','year')) %>%
    mutate(value = value * input_frac) %>%
    select(-input_frac)
  
  disag_emiss_by_tech <- disag_emiss_by_tech %>%
    filter(!(enduse %in% c('cement') & !str_detect(technology,'cement'))) %>%
    bind_rows(process_heat_disag)

  
  output <- rgcam::getQuery(prj, "outputs by tech") %>%
    mutate(sector = if_else(str_detect(sector,'trn_'),subsector,sector)) %>% 
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
    mutate(value = sum(value)) %>%
    ungroup()
  
  return(fuel_cycle_emiss)
  
}


energy_water_fuel_cycle_impacts <- function(end_use,energy_water_tracing){
  
  en_water_tracing <- energy_water_tracing %>%
    filter(enduse == end_use) %>%
    mutate(input = if_else(transformation == enduse,primary,transformation),
           input = if_else(input == 'biophysical water consumption', 'total biomass', input),
           input = if_else(input %in% c('water consumption','water withdrawals','seawater'),'upstream water coal biomass',input))
  
  outputs_by_tech <- rgcam::getQuery(prj, "outputs by tech") %>%
    mutate(sector = if_else(str_detect(sector,'trn_'),subsector,sector)) %>% 
    filter(sector == end_use,
           year >= 2005) %>%
    rename(enduse = sector) %>%
    mutate(year = as.numeric(year))
  
  inputs_by_tech <- rgcam::getQuery(prj, "inputs by tech") %>%
    mutate(sector = if_else(str_detect(sector,'trn_'),subsector,sector),
           input = if_else(str_detect(input,'elect_'),'electricity',
                           if_else(str_detect(input,'refined liquids'),'refining',input)),
           input = if_else(str_detect(input,'coal'),'coal',
                           if_else(str_detect(input,'gas'),'gas processing',
                                   if_else(str_detect(input,'biomass'),'total biomass',input)))) %>% 
    filter(sector == end_use,
           year >= 2005) %>%
    rename(enduse = sector) %>%
    mutate(year = as.numeric(year)) %>%
    group_by(Units,scenario,region,year,enduse,input) %>%
    mutate(frac_of_input_in_tech = value / sum(value)) %>%
    ungroup()
  
  if (end_use %in% c('cement')){
    
    inputs_by_tech <- rgcam::getQuery(prj, "inputs by tech") %>%
      mutate(sector = if_else(str_detect(sector,'trn_'),subsector,sector),
             input = if_else(str_detect(input,'elect_'),'electricity',
                             if_else(str_detect(input,'refined liquids'),'refining',input)),
             input = if_else(str_detect(input,'coal'),'coal',
                             if_else(str_detect(input,'gas'),'gas processing',
                                     if_else(str_detect(input,'biomass'),'total biomass',input)))) %>% 
      filter(sector %in% c(end_use,paste0('process heat ',end_use)),
             year >= 2005) %>%
      rename(enduse = sector) %>%
      mutate(year = as.numeric(year)) %>%
      mutate(enduse = end_use)
    
    process_heat_input_for_disag <- getQuery(prj,'inputs by tech') %>%
      filter(input == paste0('process heat ',end_use)) %>%
      group_by(scenario,region,year) %>%
      mutate(input_frac = value / sum(value)) %>%
      ungroup() %>%
      select(scenario,region,year,technology,input_frac)
    
    process_heat_disag <- inputs_by_tech %>%
      filter(enduse %in% c('cement') & !str_detect(technology,'cement')) %>%
      select(-technology) %>%
      left_join(process_heat_input_for_disag, by = c('scenario','region','year')) %>%
      mutate(value = value * input_frac) %>%
      select(-input_frac)
    
    inputs_by_tech <- inputs_by_tech %>%
      filter(!(enduse %in% c('cement') & !str_detect(technology,'cement')),
             input != paste0('process heat ', enduse)) %>%
      bind_rows(process_heat_disag) %>%
      mutate(subsector = 'cement') %>%
      group_by(Units,scenario,region,year,enduse,input) %>%
      mutate(frac_of_input_in_tech = value / sum(value)) %>%
      ungroup() 
    
  }
  

  
  coal_bio_for_water_disag <- inputs_by_tech %>%
    select(-frac_of_input_in_tech) %>%
    filter(input %in% c('coal','total biomass')) %>%
    group_by(scenario,region,year,enduse,input) %>%
    summarize(value = sum(value)) %>%
    ungroup() %>%
    group_by(scenario,region,year,enduse) %>%
    mutate(en_input_frac = value / sum(value)) %>%
    ungroup()
  
  en_water_tracing_no_upstream_water <- en_water_tracing %>%
    filter(input != 'upstream water coal biomass')
  
  upstream_water <- en_water_tracing %>%
    filter(input == 'upstream water coal biomass') 
  
  upstream_water_disag <- upstream_water %>%
    select(-input) %>%
    left_join(coal_bio_for_water_disag %>% select(-value), by = c('scenario','region','year','enduse')) %>%
    mutate(value = value * en_input_frac) %>%
    select(-en_input_frac)
  
  energy_water_for_disag <- bind_rows(upstream_water_disag,en_water_tracing_no_upstream_water) %>%
    filter(year >= 2005,
           input %in% inputs_by_tech$input)
  
  disag_en_water <- energy_water_for_disag %>%
    left_join(inputs_by_tech %>% select(-value,-Units),by = c('scenario','region','year','enduse','input')) %>%
    mutate(value = value * frac_of_input_in_tech) %>%
    select(-frac_of_input_in_tech,-input)
  
  
  en_water_fuel_cycle_impacts <- disag_en_water %>%
    left_join(outputs_by_tech, by = c('scenario','region','year','enduse','technology','subsector')) %>%
    mutate(value.x = value.x * 10^9,
           Units.x = if_else(Units.x == 'km^3','m^3',
                             if_else(Units.x == 'EJ','GJ',Units.x)),
           value.y = if_else(Units.y %in% c('million pass-km','million ton-km','Mt'), value.y * 10^6, value.y), #convert million km to km; else convert EJ to MJ; million tons to tonnes
           Units.y = if_else(Units.y == 'million pass-km', 'pass-km',
                             if_else(Units.y == 'million ton-km', 'ton-km',
                                     if_else(Units.y == 'Mt','tonne',Units.y))),
           value = value.x / value.y,
           Units = paste0(Units.x,' per ',Units.y))
  
  return(en_water_fuel_cycle_impacts)
  
}


