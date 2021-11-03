library(rgcam)
library(dplyr)
options(dplyr.summarise.inform = FALSE)
library(tidyr)
library(stringr)
library(readr)

setwd(FOLDER_LOCATION)


#source("functions.R")
#source("main.R")

sectors <- read_csv('input/sector_label.csv')
elec_gen_fuels <- read_csv('input/elec_generation.csv')


#temporary until we can query inputs by tech directly
inputs_by_tech <- read_csv('inputs_by_tech.csv') %>% 
  pivot_longer(cols = '1990':'2100',names_to = 'year') %>%
  mutate(scenario = gsub("(.*),.*", "\\1", scenario))


CO2_emissions_by_tech <- read_csv('CO2_emissions_by_tech.csv') %>% 
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


all_emissions <- read_csv('all_emissions.csv')


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
  filter(!((enduse %in% transport_sectors$transportation_subsector & direct == 'refining') | direct %in% transport_sectors$transportation_subsector) | ghg != 'CO2' )





CO2_sequestration_by_tech %>%
  filter(sector == 'refining') %>%
  mutate(fuel = if_else(subsector == 'biomass liquids','biomass',
                        if_else(subsector == 'coal to liquids','coal',NA_character_))) %>%
  group_by(scenario,region,fuel,year) %>%
  summarize(sum_seq = sum(value)) %>%
  ungroup()-> refining_c_csq

refining_c_csq$year <- as.numeric(refining_c_csq$year)

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



  
## Final processing #$

#Fix cement emissions to separate out process heat (fossil fuel) from limestone-related emissions  
elec_CO2_disag <- bind_rows(all_emiss_no_elec_or_trn_CO2,trn_tailpipe_CO2_disag,elec_CO2_no_bio_final) %>%
  mutate(direct = if_else(direct == 'cement limestone','limestone',direct),
         transformation = if_else(transformation == 'cement limestone','calcination',transformation),
         enduse = if_else(enduse == 'cement limestone','cement',enduse)) %>%
  mutate(direct = if_else((direct == 'gas processing') & (ghg == 'CO2'),'natural gas',direct),#assign all direct gas processing CO2 emissions to natural gas since we're using emissions no bio query and bio constitutes a very small fraction of gas processing anyway
         direct = if_else(direct == 'H2 enduse','H2 production',direct),
         transformation = if_else(transformation == 'H2 enduse','H2 production',transformation)) %>%
  group_by(scenario,region,direct,transformation,enduse,year,ghg,Units) %>%
  summarize(value = sum(value)) %>%
  ungroup() %>% 
  pivot_wider(names_from = year, values_from = value, values_fill = list(value = 0))


write.csv(elec_CO2_disag,'output/emissions-GCAM_CWF-elec_trn_disag.csv')












