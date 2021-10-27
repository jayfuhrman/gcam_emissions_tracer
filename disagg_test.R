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



sectors %>%
  filter(type == 'transformation') %>%
  distinct(rewrite) -> transformation_sectors

sectors_elec <- sectors %>%
  filter(rewrite == 'electricity')

CO2_emiss <- read_csv('all_emissions.csv') %>%
  #all_emissions %>%
  select(scenario,region,year,direct,transformation,enduse,ghg,value,Units) %>%
  filter(ghg == 'CO2') %>%
  mutate(direct = if_else(direct == 'cement limestone','limestone',direct),
         transformation = if_else(transformation == 'cement limestone','calcination',transformation),
         enduse = if_else(enduse == 'cement limestone','cement',enduse)) 
          #Fix cement emissions to separate out process heat (fossil fuel) from limestone-related emissions

CO2_emiss_transform <- CO2_emiss %>%
  filter(direct %in% transformation_sectors$rewrite) %>%
  mutate(direct = if_else(direct == 'gas processing','gas',direct)) #assign all direct gas processing CO2 emissions to natural gas since we're using emissions no bio query and bio constitutes a very small fraction of gas processing anyway
#first deal with electricity
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
         direct = if_else(fuel == 'backup_electricity','gas',direct),#assign backup electricity to gas since natural gas open cycle is the only tech for this sector
         ghg = 'CO2',
         Units = 'MTCO2e') %>%
  select(-fuel) -> elec_CO2_no_bio_final
  print("Allocated electricity emissions by fuel")

write.csv(elec_CO2_no_bio_final,'elec_CO2_no_bio_final.csv')

