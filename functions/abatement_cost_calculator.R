

get_upstream_emiss_intensity <- function(prj,all_emissions,regions,multiple_incumbent_subsectors,incumbent_techs,outputs){  
  ccoef <- read_csv('input/ccoef_mapping.csv')
  
  
  upstream_emiss <- all_emissions %>%
    filter(ghg == 'CO2',
           region %in% regions,
           transformation %in% c('electricity','H2 wholesale dispensing','H2 central production','refining')) %>%
    group_by(scenario,region,year,transformation) %>%
    summarize(emiss = sum(value) * 12/44) %>%
    ungroup() %>%
    mutate(Units = 'MTC')
  
  
  upstream_outputs <- rgcam::getQuery(prj, "outputs by tech") %>%
    filter(region %in% regions,
           sector %in% upstream_emiss$transformation) %>%
    group_by(scenario,region,year,sector) %>%
    summarize(value = sum(value)) %>%
    ungroup() %>%
    rename(transformation = sector,
           output = value)
  
  upstream_energy_emiss_intensity <- upstream_emiss %>%
    left_join(upstream_outputs, by = c("scenario","region","year","transformation")) %>%
    mutate(upstream_emiss_intensity = emiss/output,
           Units = 'MTC per EJ') %>%
    select(-emiss,-output)
  
  
  inputs_with_upstream_emiss_energy_intensity <- rgcam::getQuery(prj, "inputs by tech") %>%
    mutate(sector = if_else(str_detect(sector,'trn_'),subsector,sector),
           sector = if_else(sector %in% multiple_incumbent_subsectors$sector,
                            paste0(subsector,' ',sector),sector)) %>%
    mutate(input = if_else(str_detect(input,'elect_'),'electricity',input),
           input = if_else(input %in% c('H2 retail dispensing'),'H2 wholesale dispensing',input),
           input = if_else(input %in% c('H2 industrial'),'H2 central production',input),
           input = if_else(input %in% c('refined liquids enduse','refined liquids industrial'),'refining',input),
           input = if_else(input %in% c('wholesale gas','delivered gas'),'gas processing',input)) %>%
    filter(sector %in% incumbent_techs$sector,
           region %in% regions,
           input %in% upstream_energy_emiss_intensity$transformation) %>%
    rename(input_value = value,
           input_Units = Units) %>%
    left_join(outputs %>% select(-subsector),
              by = c("scenario","region","year","sector","technology")) %>%
    mutate(transformation_en_intensity = input_value / output_value,
           en_intensity_Units = paste0(input_Units,' per ',output_Units)) %>%
    select(scenario,region,year,sector,subsector,technology,input,transformation_en_intensity,en_intensity_Units,output_Units,input_Units) 
  
  
  emiss_intensity_upstream <- inputs_with_upstream_emiss_energy_intensity %>%  
    rename(transformation = input) %>%
    left_join(upstream_energy_emiss_intensity  %>%
                rename(emiss_intensity_Units = Units), by = c("scenario","region","year","transformation")) %>%
    mutate(emiss_intensity = transformation_en_intensity * upstream_emiss_intensity,
           phase = 'upstream',
           emiss_intensity_Units = paste0('MTC per ',output_Units)) %>%
    select(scenario,region,year,sector,technology,emiss_intensity,emiss_intensity_Units,phase)
  
  return(emiss_intensity_upstream)
}


abatement_cost_calculator <- function(prj,all_emissions,count_upstream_emiss){
  
  
  incumbent_techs <- read_csv('input/incumbent_techs.csv') %>%
    mutate(sector = if_else(str_detect(sector,'trn_'),subsector,sector)) %>%
    group_by(sector) %>%
    mutate(num_incumbent_subsectors = length(subsector)) %>%
    ungroup() 
    
  multiple_incumbent_subsectors <- incumbent_techs %>%
    filter(num_incumbent_subsectors > 1)
    
    
  incumbent_techs <- incumbent_techs %>%  
    mutate(sector = if_else(num_incumbent_subsectors > 1, paste0(subsector,' ',sector),sector)) 
  
  regions = c('USA')
  
  emiss <- rgcam::getQuery(prj, "nonCO2 emissions by tech") %>%
    mutate(sector = if_else(str_detect(sector,'trn_'),subsector,sector),
           sector = if_else(sector %in% multiple_incumbent_subsectors$sector,
                            paste0(subsector,' ',sector),sector)) %>%
    filter(#ghg == 'CO2',
           sector %in% incumbent_techs$sector,
           region %in% regions) %>%
    rename(emiss = value,
           emiss_Units = Units) 
  
  
  outputs <- rgcam::getQuery(prj, "outputs by tech") %>%
    mutate(sector = if_else(str_detect(sector,'trn_'),subsector,sector),
           sector = if_else(sector %in% multiple_incumbent_subsectors$sector,
                            paste0(subsector,' ',sector),sector)) %>%
    filter(sector %in% incumbent_techs$sector,
           region %in% regions) %>%
    rename(output_value = value,
           output_Units = Units) 
  
  costs <- rgcam::getQuery(prj, "costs by tech") %>% 
    mutate(sector = if_else(str_detect(sector,'trn_'),subsector,sector),
           sector = if_else(sector %in% multiple_incumbent_subsectors$sector,
                            paste0(subsector,' ',sector),sector)) %>%
    filter(sector %in% incumbent_techs$sector,
           region %in% regions) %>%
    rename(cost = value,
           cost_Units = Units) 
  
  zero_emiss_techs <- outputs %>% #get techs that have non-zero service output, but zero emissions
    anti_join(emiss, by = c("scenario","region","year","sector","subsector","technology")) %>%
    mutate(emiss_intensity = 0)
  

  tailpipe_emiss_intensity <- emiss %>%
    left_join(outputs, by = c('scenario','region','year','sector','subsector','technology')) %>%
    mutate(emiss_intensity = emiss / output_value) %>%
    bind_rows(zero_emiss_techs) %>%
    select(scenario,region,year,sector,technology,emiss_intensity,output_Units) %>%
    mutate(emiss_intensity_Units=paste0('MTC per ',output_Units),
           phase = 'enduse')
  
  

  if (count_upstream_emiss == TRUE){
  emiss_intensity_upstream <- get_upstream_emiss_intensity(prj,all_emissions,regions,multiple_incumbent_subsectors,incumbent_techs,outputs)
    
    
  emiss_intensity <- tailpipe_emiss_intensity %>%
    bind_rows(emiss_intensity_upstream) %>%
    group_by(scenario,region,year,sector,technology,emiss_intensity_Units,phase) %>%
    fill(output_Units, .direction = 'down') %>%
    ungroup() 
  
  emiss_intensity %>%
    group_by(scenario,region,year,sector,technology,emiss_intensity_Units,output_Units,phase) %>%
    summarize(emiss_intensity = sum(emiss_intensity)) %>%
    ungroup() -> emiss_intensity
  
  #write_csv(emiss_intensity %>% 
  #            group_by(scenario,region,year,sector,technology,emiss_intensity_Units,phase) %>%
  #            summarize(emiss_intensity = sum(emiss_intensity)) %>%
  #            ungroup() %>% 
  #            pivot_wider(names_from = year, values_from = emiss_intensity),'CO2_emissions_intensity_by_lifecycle_phase.csv')
  
  } else{
    emiss_intensity <- tailpipe_emiss_intensity
  }
  
  

  incumbent_emiss <- incumbent_techs %>%
    left_join(emiss_intensity, by = c('sector','incumbent_tech'='technology')) %>%
    rename(incumbent_emiss_intensity = emiss_intensity) %>%
    group_by(scenario,region,year,sector,incumbent_tech) %>%
    summarize(incumbent_emiss_intensity = sum(incumbent_emiss_intensity)) %>%
    ungroup()
  
  emiss_intensity <- emiss_intensity %>%
    group_by(scenario,region,year,sector,technology) %>%
    fill(output_Units, .direction = 'down') %>%
    ungroup() %>%
    group_by(scenario,region,year,sector,technology,output_Units) %>%
    summarize(emiss_intensity = sum(emiss_intensity)) %>%
    ungroup()
  
  diff_emiss_intensity <- emiss_intensity %>%
    left_join(incumbent_emiss %>% select(scenario,region,year,sector,incumbent_emiss_intensity), by = c('scenario','region','year','sector')) %>%
    mutate(diff_emiss_intensity = incumbent_emiss_intensity - emiss_intensity,
           emiss_Units = 'MTC',
           emiss_intensity_Units = paste0(emiss_Units,' per ',output_Units)) %>%
    mutate(diff_emiss_intensity = if_else(diff_emiss_intensity < 0, 0, diff_emiss_intensity)) %>% #avoid negative abatement cost calculations for techs that increase emissions AND are more expensive relative to the baseline
    select(scenario,region,year,sector,technology,diff_emiss_intensity,emiss_intensity_Units)
  
  
  CO2_prices <- getQuery(prj,'CO2 prices') %>%
    mutate(region = str_split(market,"CO2",simplify=TRUE)[ , 1]) %>%
    filter(!str_detect(region,'air') & !str_detect(market,'CO2 removal') & !str_detect(market,'NearTerm'), !str_detect(market,'_LUC')) %>%
    rename(emiss_price = value) %>%
    mutate(emiss_price = emiss_price / 1000,
           Units = '1990$/kgC') #convert emiss price to $1990 per kgC
  
  
  emiss_price_costs <- CO2_prices %>%
    right_join(diff_emiss_intensity,by = c("scenario","region","year")) %>%
    mutate(emiss_price = if_else(str_detect(emiss_intensity_Units,'million'),emiss_price * 1000 ,emiss_price / 1.790385716),
           emiss_cost_diff = diff_emiss_intensity * emiss_price) %>%
  #convert non transport tech emissions cost to $1975 basis for consistency; 
  #convert transport techs emiss price (with million *** in the units) to $1990 per tC basis to be consistent with emissions intensity units (tC per pass/tonne-km)
  select(scenario,region,year,sector,technology,emiss_cost_diff)
  
  
  incumbent_cost <- incumbent_techs %>%
    left_join(costs, by = c('sector','incumbent_tech'='technology')) %>%
    rename(incumbent_cost = cost)
  
  diff_cost <- costs %>%
    left_join(incumbent_cost, by = c('scenario','region','year','sector','cost_Units')) %>%
    mutate(diff_cost = cost - incumbent_cost) %>%
    select(scenario,region,year,sector,technology,cost,diff_cost,cost_Units) %>%
    left_join(emiss_price_costs, by = c('scenario','region','year','sector','technology')) %>%
    rename(orig_cost = diff_cost) %>%
    mutate(diff_cost = orig_cost + emiss_cost_diff) %>%
    select(scenario,region,year,sector,technology,cost,diff_cost,cost_Units) 
  
  

  
  abatement_cost <- diff_cost %>%
    left_join(diff_emiss_intensity, by = c('scenario','region','year','sector','technology')) %>%
    group_by(scenario,region,sector,technology) %>%
    mutate(diff_emiss_intensity = approx_fun(year, diff_emiss_intensity, rule = 2)) %>%
    fill(emiss_intensity_Units,.direction = "up") %>%
    ungroup() %>%
    mutate(diff_emiss_intensity = diff_emiss_intensity * 44/12,
           inf_adj = if_else(str_detect(cost_Units,"1990"), 1.790385716,
                             if_else(str_detect(cost_Units,"1975"), 3.80666019,0)),
           diff_cost = diff_cost * inf_adj,
           abatement_cost = diff_cost / diff_emiss_intensity,
           abatement_cost = if_else(str_detect(emiss_intensity_Units,'EJ'), abatement_cost * 1000, abatement_cost), 
           # techs with output units of EJ or cost units of kg result in abatement cost estimates per kgCO2, 
           # so convert to per tCO2 basis
           abatement_cost = if_else(str_detect(cost_Units,'kg'), abatement_cost * 1000, abatement_cost)) %>%
    filter(year > 2015,
           !is.na(abatement_cost)) %>%
    select(scenario,region,year,sector,technology,abatement_cost) %>%
    group_by(scenario,region,year,sector,technology) %>%
    summarize(abatement_cost = sum(abatement_cost)) %>%
    ungroup() %>%
    pivot_wider(names_from = year, values_from = abatement_cost) %>%
    mutate(Units = '$2020/tCO2')
  
  return(abatement_cost)
  
}



  



