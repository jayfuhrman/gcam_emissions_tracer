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
source('fuel_cycle_impacts.R')


plot_fuel_cycle_emiss <- function(enduse,emiss_type,region_,years,scenarios){
  
  fuel_cycle_emiss <- emissions_fuel_cycle(all_emissions,enduse)
  
  if (emiss_type == 'ghg'){
    
    fuel_cycle_emiss <- fuel_cycle_emiss %>%
      filter(Units.x == 'grams CO2e')
    
    #enduse <- unique(fuel_cycle_emiss$enduse)
    impact <- unique(fuel_cycle_emiss$Units.x)
    functional_unit <- unique(fuel_cycle_emiss$Units.y)
    
    ghg <- fuel_cycle_emiss %>%
      filter(region == region_,
             scenario %in% scenarios,
             year %in% years) %>%
      mutate(phase = if_else(phase == 'enduse' & transformation == 'calcination', 'enduse (limestone)',
                             if_else(phase == 'enduse' & transformation == 'process heat', 'enduse (process heat)', phase)))
    
    
    p <- ggplot(ghg) + 
      theme_light() + 
      geom_bar(aes(x = technology, y = value, fill = phase), stat = "identity") +
      theme(axis.text.x = element_text(angle = 90,hjust=1,vjust=0.5),
            plot.title=element_text(hjust=0.5)) +
      ylab(paste0(impact, ' per ', functional_unit)) +
      facet_grid(cols=vars(year),rows=vars(scenario)) +
      scale_fill_npg() +
      ggtitle(paste0(enduse,' ',region_)) 
    
    
    filename = paste0(enduse,region_,emiss_type,".png")
    ggsave(filename = filename)
    
  } else if (emiss_type == 'nonghg') {
    fuel_cycle_emiss <- fuel_cycle_emiss %>%
      filter(Units.x == 'grams')
    
    impact <- unique(fuel_cycle_emiss$Units.x)
    functional_unit <- unique(fuel_cycle_emiss$Units.y)
    
    nonghg <- fuel_cycle_emiss %>%
      filter(region == region_,
             scenario %in% scenarios,
             year %in% years)
    
    p <- ggplot(nonghg) + 
      theme_light() + 
      geom_bar(aes(x = technology, y = value, fill = phase), stat = "identity") + #multiply emissions to grams
      theme(axis.text.x = element_text(angle = 90,hjust=1,vjust=0.5),
            plot.title=element_text(hjust=0.5)) + 
      facet_grid(cols=vars(scenario),rows=vars(ghg),scales="free") +
      ylab(paste0('grams per ',functional_unit)) +
      ggtitle(paste0(enduse,' ',region_,' ',unique(nonghg$year))) +
      scale_fill_npg()
    
    filename = paste0(enduse,region_,emiss_type,".png")
    ggsave(filename = filename,width = 6, height = 8)
    
    
  } else{
    stop('please enter a valid emissions type')
  }
  
}





