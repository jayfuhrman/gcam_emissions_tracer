library(rgcam)
library(dplyr)
options(dplyr.summarise.inform = FALSE)
library(tidyr)
library(stringr)
library(readr)

setwd(FOLDER_LOCATION)


#source("functions.R")
#source("main.R")

final_fuel_CO2_disag <- function(all_emissions){

sectors <- read_csv('input/sector_label.csv')
elec_gen_fuels <- read_csv('input/elec_generation.csv')


#temporary until we can query inputs by tech directly
inputs_by_tech <- read_csv('inputs_by_tech.csv') %>% 
    pivot_longer(cols = '1990':'2100',names_to = 'year') %>%
    mutate(scenario = gsub("(.*),.*", "\\1", scenario))
  
  
  CO2_sequestration_by_tech <- read_csv('CO2_sequestration_by_tech.csv') %>% 
    pivot_longer(cols = '1990':'2100',names_to = 'year') %>%
    mutate(scenario = gsub("(.*),.*", "\\1", scenario))
  
  sectors %>%
    filter(type == 'transformation') %>%
    distinct(rewrite) -> transformation_sectors
  
  sectors_elec <- sectors %>%
    filter(rewrite == 'electricity')
  
  inputs_by_subsector <- rgcam::getQuery(prj, "inputs by subsector")
  
  
  
  
  all_emissions %>%
    select(scenario,region,year,direct,transformation,enduse,ghg,value,Units) %>%
    filter(ghg == 'CO2') -> CO2_emiss
  
  
  
  
  
  CO2_emiss_transform <- CO2_emiss %>%
    filter(direct %in% transformation_sectors$rewrite) 
  
  
  #first deal with electricity
  all_emissions %>%
    select(scenario,region,direct,transformation,enduse,year,value,ghg,Units) %>%
    filter((direct != 'electricity') | (ghg != 'CO2'))  -> all_emiss_no_elec_CO2
  
  
  CO2_emiss_elec <- CO2_emiss_transform %>%
    filter(direct == 'electricity')
  
  
  CO2 %>%
    filter(sector %in% sectors_elec$sector) %>%
    group_by(region,scenario,year) %>%
    mutate(normfrac = value/sum(value)) %>%
    ungroup() %>%
    select(-value,-Units) -> tmp
  
  
   tmp%>%
    left_join(CO2_emiss_elec, by = c('scenario','region','year')) %>%
    mutate(value = value * normfrac) %>%
    left_join(elec_gen_fuels, by = c('sector')) %>%
    filter(year >= 2005) %>%
    group_by(scenario,region,fuel,direct,transformation,enduse,year) %>%
    summarise(value = sum(value)) %>%
    ungroup()  %>%
    arrange(year) %>%
    mutate(direct = fuel,
           transformation = if_else(fuel == 'backup_electricity',fuel,transformation),
           direct = if_else(fuel == 'backup_electricity','natural gas',direct),#assign backup electricity to gas since natural gas open cycle is the only tech for this sector
           ghg = 'CO2',
           Units = 'MTCO2e') %>%
    select(-fuel) -> elec_CO2_no_bio_final
    print("Allocated electricity emissions by fuel")
  
  
  
  #### disagregate transportation tailpipe emissions ###
  
  transport_sectors <- read_csv('input/transport.csv')  
  ccoef_mapping <- read_csv('input/ccoef_mapping.csv')
  
  inputs_by_subsector %>% #get all sectors using refined liquids (incl. non transportation)
    filter(input %in% c('refined liquids industrial','refined liquids enduse'),
           !(sector %in% c('elec_refined liquids (CC)','elec_refined liquids (CC CCS)','elec_refined liquids (steam/CT)'))) %>%
    mutate(subsector = if_else(subsector == 'refined liquids',sector,subsector)) %>%
    distinct(subsector)-> refined_liquids_subsector
  
  
  
  all_emiss_no_elec_CO2 %>%
    filter(((enduse %in% transport_sectors$transportation_subsector & direct == 'refining') | direct %in% transport_sectors$transportation_subsector)  & ghg == 'CO2') %>%
    select(-transformation)-> trn_CO2
    
  
  
  ## Filter to get fuels with tailpipe emissions (i.e., natural gas + refined liquids)
  trn_inputs_by_subsector <- inputs_by_subsector %>% 
    filter(subsector %in% transport_sectors$transportation_subsector,
           input %in% c('refined liquids enduse','delivered gas','refined liquids industrial')) %>%
    rename(enduse = subsector,
           PrimaryFuelCO2Coef.name = input) %>%
    left_join(ccoef_mapping,by = c('PrimaryFuelCO2Coef.name')) %>%
    mutate(transformation = if_else((PrimaryFuelCO2Coef.name == 'refined liquids enduse' | PrimaryFuelCO2Coef.name == 'refined liquids industrial'),'refining',
                                    if_else(PrimaryFuelCO2Coef.name == 'delivered gas','gas processing',NA_character_)),
           tailpipe_emiss = value * PrimaryFuelCO2Coef) %>%
    group_by(scenario,region,enduse,year) %>%
    mutate(frac_tailpipe_emiss = tailpipe_emiss / sum(tailpipe_emiss)) %>%
    ungroup() %>%
    select(-sector,-PrimaryFuelCO2Coef.name,-value,-PrimaryFuelCO2Coef,-fuel,-tailpipe_emiss,-Units)
  
  
  
  trn_inputs_by_subsector %>%
    left_join(trn_CO2,by = c('scenario','region','enduse','year')) %>%
    mutate(value = value * frac_tailpipe_emiss,
           fuel = if_else(transformation == 'refining','refining',
                          if_else(transformation == 'gas processing','natural gas',NA_character_)),
           direct = fuel) %>%
    select(-frac_tailpipe_emiss,-fuel) %>%
    filter(year >= 2005) %>%
    group_by(scenario,region,direct,transformation,enduse,year,ghg,Units) %>%
    summarize(value = sum(value)) %>%
    ungroup() -> trn_tailpipe_CO2_disag
  
  
  trn_tailpipe_CO2_disag %>%
    distinct(enduse) -> trn_sectors
  
  all_emiss_no_elec_or_trn_CO2 <- all_emiss_no_elec_CO2 %>%
    filter(!((enduse %in% transport_sectors$transportation_subsector & direct == 'refining') | 
              direct %in% transport_sectors$transportation_subsector) | 
              ghg != 'CO2') 
  
  
  CO2_sequestration_by_tech$year <- as.numeric(CO2_sequestration_by_tech$year)
  
  CO2_sequestration_by_tech %>%
    filter(sector == 'refining') %>%
    mutate(fuel = if_else(subsector == 'biomass liquids','biomass',
                          if_else(subsector == 'coal to liquids','coal',NA_character_))) %>%
    group_by(scenario,region,fuel,year) %>%
    summarize(sum_seq = sum(value)) %>%
    ungroup()-> refining_c_csq
  
  
  inputs_by_subsector %>%  
    filter((sector == 'refining') & (input != 'elect_td_ind')) %>%
    rename(PrimaryFuelCO2Coef.name = input) %>%
    left_join(ccoef_mapping,by = c('PrimaryFuelCO2Coef.name')) %>%
    mutate(c_input = value * PrimaryFuelCO2Coef) %>%
    group_by(scenario,region,year,fuel) %>%
    summarize(c_input = sum(c_input)) %>%
    ungroup() %>%
    left_join(refining_c_csq,by = c('scenario','region','year','fuel')) %>%
    mutate(c_input = if_else(fuel == 'biomass',0,c_input),
           sum_seq = if_else(is.na(sum_seq),0,sum_seq),
           emiss_no_bio = c_input - sum_seq) -> refining_emiss_by_fuel_no_bio
  
  
  refining_emiss_by_fuel_no_bio %>%
    group_by(scenario,region,year) %>%
    mutate(normfrac = emiss_no_bio/sum(emiss_no_bio),
           transformation = 'refining',
           ghg = 'CO2') %>%
    select(-sum_seq,-c_input,-emiss_no_bio) -> refining_emiss_by_fuel_no_bio_norm
  
  
  refining_emiss_by_fuel_no_bio_norm %>%
    left_join(trn_tailpipe_CO2_disag,by = c('scenario','region','year','transformation','ghg')) %>%
    mutate(direct = fuel,
           value = value*normfrac) %>%
    filter(year >= 2005) %>%
    select(-fuel,-normfrac) -> trn_tailpipe_CO2_disag
  
  
  
  ## Now disaggregate hydrogen no bio emissions
  all_emiss_no_elec_or_trn_CO2 %>%
    filter(direct != 'H2 enduse' | ghg != 'CO2') -> all_emiss_no_elec_or_trn_no_H2_CO2 
  
  
  all_emiss_no_elec_or_trn_CO2 %>%
    filter(direct == 'H2 enduse' & ghg == 'CO2') -> H2_CO2_emiss
  
  inputs_by_subsector %>%
    filter(sector == 'H2 central production' | sector == 'H2 forecourt production') %>%
    rename(PrimaryFuelCO2Coef.name = input) %>%
    left_join(ccoef_mapping,by = c('PrimaryFuelCO2Coef.name')) %>%
    mutate(PrimaryFuelCO2Coef = if_else(is.na(PrimaryFuelCO2Coef) | fuel == 'biomass',0,PrimaryFuelCO2Coef),
           fuel = if_else(is.na(fuel),PrimaryFuelCO2Coef.name,fuel),
           c_input = value * PrimaryFuelCO2Coef) %>%
    group_by(scenario,region,year,fuel) %>%
    summarize(c_input = sum(c_input)) %>%
    ungroup() -> H2_inputs
  
  CO2_sequestration_by_tech %>%
    filter(sector == 'H2 central production' | sector == 'H2 forecourt production') %>%
    mutate(fuel = if_else(subsector == 'gas','natural gas',subsector)) %>%
    rename(c_seq = value)-> H2_sequestration
  
  H2_inputs %>%
    left_join(H2_sequestration %>%
                select(-sector,-subsector,-technology),by = c('scenario','region','year','fuel')) %>%
    filter(!is.na(Units)) %>%
    mutate(emiss_no_bio = c_input - c_seq) %>% #mutate to H2 production (the actual transformation) occurs at the end
    group_by(scenario,region,year) %>%
    mutate(normfrac = emiss_no_bio / sum(emiss_no_bio)) %>%
    select(-Units) -> H2_inputs_joined
  
  
  
  H2_inputs_joined %>%
    left_join(H2_CO2_emiss, by = c('scenario','region','year')) %>%
    mutate(direct = fuel,
           value = value * normfrac) %>%
    select(-c_input,-c_seq,-emiss_no_bio,-fuel,-normfrac) -> H2_CO2_emiss_disag
    
  
  #deal with all remaining CO2 emissions
  #break out H2 and refining emissions to be dealt with downstream
  
  c_containing <- c('biomass','coal','natural gas','gas processing','cement limestone','crude oil','CO2 removal')
  
  #all_emissions %>% select(scenario,region,direct,year,transformation,enduse,ghg,value,Units) %>%
  all_emiss_no_elec_or_trn_no_H2_CO2 %>%
    filter(!(direct %in% c_containing) & ghg == 'CO2')  -> remaining_industry_CO2
  
  #all_emissions %>% select(scenario,region,direct,year,transformation,enduse,ghg,value,Units) %>%
  all_emiss_no_elec_or_trn_no_H2_CO2 %>%
    filter(!(!(direct %in% c_containing) & ghg == 'CO2')) -> all_emiss_no_elec_or_trn_no_H2_CO2
  
  
  inputs_by_subsector %>%
    filter(sector %in% remaining_industry_CO2$direct) %>%
    rename(PrimaryFuelCO2Coef.name = input) %>%
    mutate(PrimaryFuelCO2Coef.name = if_else(PrimaryFuelCO2Coef.name == 'process heat dac','wholesale gas',PrimaryFuelCO2Coef.name)) %>%
    left_join(ccoef_mapping,by = c('PrimaryFuelCO2Coef.name')) %>%
    mutate(fuel = if_else(PrimaryFuelCO2Coef.name %in% c('elect_td_ind','elect_td_bld'),'electricity',
                          if_else(PrimaryFuelCO2Coef.name == 'H2 enduse','H2 enduse',
                                  if_else(PrimaryFuelCO2Coef.name %in% c('refined liquids industrial','refined liquids enduse'),'refining',fuel))),
           PrimaryFuelCO2Coef = if_else(is.na(PrimaryFuelCO2Coef),0,PrimaryFuelCO2Coef),
           PrimaryFuelCO2Coef = if_else(fuel == 'biomass',0,PrimaryFuelCO2Coef),
           c_input = value * PrimaryFuelCO2Coef) %>%
    filter(!is.na(c_input)) %>%
    group_by(scenario,region,sector,year) %>%
    mutate(sum_c = sum(c_input),
           normfrac = c_input/sum_c) %>%
    ungroup() %>%
    rename(direct = sector) -> remaining_industry_inputs
  
  
  
  
  remaining_industry_inputs %>%
    select(-PrimaryFuelCO2Coef,-c_input,-sum_c,-PrimaryFuelCO2Coef.name,-subsector,-value,-Units) %>%
    left_join(remaining_industry_CO2,by = c('scenario','region','year','direct')) %>%
    mutate(value = value * normfrac,
           value = if_else(is.na(value),0,value),
           ghg = 'CO2',
           Units = 'MTCO2e',
           enduse = if_else(is.na(enduse),direct,enduse),
           direct = fuel,
           transformation = if_else(fuel == 'electricity' | fuel == 'H2 enduse' | fuel == 'refining' | fuel == 'district heat',fuel,transformation)) %>%
    select(-normfrac,-fuel) %>%
    filter(year >= 2005,
           !(direct %in% c('electricity','H2 enduse'))) -> remaining_industry_disag_w_refining  #filter out electricity + hydrogen since these are already accounted for elsewhere
  
  
  
  remaining_industry_disag_w_refining %>%
    filter(direct == 'refining') -> refining_ind_for_disag
  
  remaining_industry_disag_w_refining %>%
    filter(direct != 'refining') -> remaining_industry_CO2_no_refining
  
  
  refining_emiss_by_fuel_no_bio_norm %>%
    left_join(refining_ind_for_disag,by = c('scenario','region','year','ghg','transformation')) %>%
    filter(year >= 2005) %>%
    mutate(direct = fuel,
           value = value * normfrac) %>%
    select(-fuel,-normfrac) -> remaining_industry_refining_disag 
  
  remaining_industry_disag <- bind_rows(remaining_industry_refining_disag,remaining_industry_CO2_no_refining)
  
  
  
    
  ## Final processing #
  
  #Fix cement emissions to separate out process heat (fossil fuel) from limestone-related emissions  
  df <- bind_rows(all_emiss_no_elec_or_trn_no_H2_CO2,
                              trn_tailpipe_CO2_disag,
                              elec_CO2_no_bio_final,
                              H2_CO2_emiss_disag,
                              remaining_industry_disag) %>%
    mutate(direct = if_else(direct == 'cement limestone','limestone',direct),
           transformation = if_else(transformation == 'cement limestone','calcination',transformation),
           enduse = if_else(enduse == 'cement limestone','cement',enduse)) %>%
    mutate(direct = if_else((direct == 'gas processing') & (ghg == 'CO2'),'natural gas',direct),#assign all direct gas processing CO2 emissions to natural gas since we're using emissions no bio query and bio constitutes a very small fraction of gas processing anyway
           direct = if_else(direct == 'H2 enduse','H2 production',direct),
           transformation = if_else(transformation == 'H2 enduse','H2 production',transformation),
           transformation = if_else(direct == 'natural gas' & transformation != 'electricity' & transformation != 'H2 production' & transformation != 'refining','gas processing',transformation)) %>%
    group_by(scenario,region,direct,transformation,enduse,year,ghg,Units) %>%
    summarize(value = sum(value)) %>%
    ungroup() #%>% 
    #pivot_wider(names_from = year, values_from = value, values_fill = list(value = 0))
  return(df)
}




final_fuel_nonCO2_disag <- function(all_emissions) {


  transport <- read_csv('input/transport.csv')
  
  all_emissions %>%
    filter(ghg %in% c('CH4','N2O') & direct == transformation & transformation == enduse) -> combustion_non_CO2_emiss
  
  all_emissions %>%
    filter(!(ghg %in% c('CH4','N2O') & direct == transformation & transformation == enduse)) -> all_other_emiss  #for mergeback
  
  
  
  #temporary until we can query directly on the cluster
  nonCO2_emissions_by_tech <- read_csv('nonCO2_emissions_by_tech.csv') %>% 
    pivot_longer(cols = '1990':'2100',names_to = 'year') %>%
    mutate(scenario = gsub("(.*),.*", "\\1", scenario))
  
  
  nonCO2_combustion_emissions_by_tech <- nonCO2_emissions_by_tech %>%
    rename(ghg = GHG) %>%
    filter(ghg %in% c('CH4','N2O'),
           sector != 'UnmanagedLand') %>%
    mutate(sector = if_else(subsector %in% transport$transportation_subsector,subsector,sector),
           fuel = if_else(technology %in% c('Liquids','NG','Coal','biomass'),technology,subsector),
           fuel = if_else(fuel %in% c('gas','NG'),'natural gas',fuel),
           fuel = if_else(fuel %in% c('Liquids'),'refined liquids',fuel)) %>%
    filter(!(sector %in% c('H2 central production','district heat','electricity','refining'))) %>% #filter out transformation sector as these are dealt with already
    group_by(scenario,region,sector,ghg,year) %>%
    mutate(normfrac = value / sum(value)) %>%
    ungroup() %>%
    mutate(year = as.numeric(year),
           normfrac = if_else(is.na(normfrac),0,normfrac)) %>%
    rename(enduse = sector)
  
  
  
  nonCO2_combustion_emissions_by_tech %>%
    select(-Units,-technology,-subsector,-value) %>%
    left_join(combustion_non_CO2_emiss, by = c('scenario','region','year','enduse','ghg')) %>%
    filter(year >= 2005,
           Units != is.na(Units)) %>%
    mutate(value = value * normfrac,
           direct = fuel) %>%
    select(-normfrac,-fuel) -> combustion_non_CO2_emiss_disag
  
  all_emiss_w_nonCO2_comb_disag <- bind_rows(combustion_non_CO2_emiss_disag,all_other_emiss) 
  
  all_emiss_w_nonCO2_comb_disag  %>%
    select(scenario,region,direct,transformation,enduse,ghg,year,value,Units) %>%
    group_by(scenario,region,direct,transformation,enduse,ghg,year,Units) %>%
    summarize(value = sum(value)) %>% #sum combustion and resource extraction emissions for some sectors
    ungroup() -> all_emiss_w_nonCO2_comb_disag_distinct
  
  return(all_emiss_w_nonCO2_comb_disag_distinct)
}


