library(rgcam)
library(dplyr)
library(ggplot2)
library(stringr)
library(tidyr)
library(readr)
library(ggpattern)
library("ggsci")
library("ggplot2")
library("gridExtra")

library(ggsankey)

setwd('C:/Users/fuhr472/Documents/gcam_emissions_tracer/figures')

enduses <- c('Car')
transformations <- c('H2 central production','H2 wholesale dispensing','refining','electricity',enduses)

regions <- c('USA')
years <- c(2025,2050)

transform_pathway <- 'Hydrogen'


df <- readr::read_csv('../output/emissions-GCAM_CWF.csv') %>%
  pivot_longer(`2005`:`2100`,names_to='year') %>%
  filter(transformation %in% transformations,
         region %in% regions,
         year %in% years,
         enduse %in% enduses,
       ghg != 'Captured CO2',
       ghg != 'Feedstock embedded carbon') %>%
  group_by(direct,transformation,enduse,ghg,phase,CWF_Sector,year) %>%
  summarize(value = sum(value)) %>%
  mutate(direct = if_else(direct == 'biomass CCS','biomass',direct),
         ghg = if_else(ghg == 'N2O_AGR','N2O',ghg),
         transformation = if_else(transformation %in% c('H2 central production','H2 wholesale dispensing'),'FCEV',transformation),
         transformation = if_else(transformation %in% c('electricity'),'BEV',transformation),
         transformation = if_else(transformation %in% c('Car','refining'),'Liquids',transformation),
         year = as.numeric(year)) 



output <- rgcam::getQuery(prj, "outputs by tech") %>%
  filter(region %in% regions,
         subsector %in% enduses,
         year %in% years) %>%
  rename(Mvkm = value)

#df$value <- df$value / df$Mvkm * 1000

df <- df %>%
  rename(technology = transformation) %>%
  left_join(output, by = c('technology','year')) %>%
  mutate(value = value / Mvkm * 1000) %>%
  group_by(direct,technology,year,phase) %>%
  summarize(value = sum(value)) %>%
  ungroup()



positions <- c("resource production","midstream","enduse")

p <- ggplot(df) +
  geom_bar(aes(x = phase, y = value, fill = direct), stat = "identity") +
  scale_x_discrete(limits = positions,
                   labels = function(x) str_wrap(x, width = 10)) +
  facet_grid(cols=vars(technology),rows=vars(year)) + 
  ylab("kgCO2-eq per vkm") +
  ggtitle(paste0(enduses,": GHG emissions by lifecycle phase in ",years)) + 
  geom_hline(yintercept=0, linetype="dashed", color = "black") +
  scale_fill_aaas()

filename = paste0("USA_emiss.png")
ggsave(filename = filename, width = 8, height = 4)



#df_sankey <- df %>% 
#  make_long(direct,transformation,CWF_Sector,ghg)


