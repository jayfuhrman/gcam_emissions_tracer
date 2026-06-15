#library(dplyr)
#library(tidyr)

build_prj <- function(DATABASE_LOCATION,DATABASE_FOLDER,DATABASE_NAME = 'database_basexdb',SCENARIO_NAME,QUERY_RESULTS_LOCATION){
  if(RGCAM){
    setwd(DATABASE_LOCATION)
    if (file.exists(paste0(FOLDER_LOCATION, QUERY_RESULTS_LOCATION))){
      print("Database already queried. Opening data file...")
      print(QUERY_RESULTS_LOCATION)
      prj <- rgcam::loadProject(paste0(FOLDER_LOCATION, QUERY_RESULTS_LOCATION))
      print("Data file opened.")
    } else {
      print("Querying database...")
      conn <- rgcam::localDBConn(DATABASE_FOLDER, DATABASE_NAME,maxMemory = '16g')
      if(SCENARIO_NAME == "ALL"){
        for (scenario in rgcam::listScenariosInDB(conn)$name){
          prj <- rgcam::addScenario(conn, paste0(FOLDER_LOCATION, QUERY_RESULTS_LOCATION), scenario,
                                    paste0(FOLDER_LOCATION, 'queries.xml'))
        }

      } else {
        prj <- rgcam::addScenario(conn, paste0(FOLDER_LOCATION, QUERY_RESULTS_LOCATION), SCENARIO_NAME,
                                  paste0(FOLDER_LOCATION, 'queries.xml'))
      }

      print("Database queried.")
    }
    setwd(FOLDER_LOCATION)
    return(prj)
  }
}


# If ratio = 1 and input is not a primary input,
# replace sectors with input name with downstream sector name and type,
# then delete downstream row
downstream_replacer <- function(df, primary_sectors){
  i <- 1
  while (i <= dim(df)[1]){
    if (df$ratio[i] == 1 && !(df$input[i] %in% primary_sectors)){
      df <- df %>%
        mutate(type = if_else(sector == df$input[i],
                              df$type[i],
                              type),
               sector = if_else(sector == df$input[i],
                                df$sector[i],
                                sector)) %>%
        filter(input != df$input[i])
    } else {
      i <- i + 1
    }
  }
  return(df %>% select(-scenario, -region, -year))
}

# If both ratios = 1
# replace inputs with sector name with upstream input name,
# then delete upstream row - also apportions upstream amount
upstream_replacer <- function(df){
  i <- 1
  while (i <= dim(df)[1]){
    if ( round(df$ratio[i],16) == 1 && round(df$ratio2[i],16) == 1 && !(df$sector[i] == 'total biomass') && df$value[i] > 10^-9){
      #skip if sector == total biomass as this is water into biomass cultivation and it is a passthru rather than primary sector in this instance
      val_to_divide <- filter(df, input == df$input[i])$value

      df <- df %>%
        mutate(value = if_else(input == df$sector[i],
                               val_to_divide * ratio,
                               value),
               input = if_else(input == df$sector[i],
                               df$input[i],
                               input)) %>%
        filter(sector != df$sector[i])
    } else {
      i <- i + 1
    }
  }
  return(df %>% select(-scenario, -region, -year, -Units))
}

# use ratios to apportion passthru sectors to transformation sectors
passthru_remove <- function(df, remaining_passthru = NULL){
  if (is.null(remaining_passthru)){
    remaining_passthru <- (df %>% filter(type == "passthru") %>% distinct(sector))$sector
  }

  for (passthru in remaining_passthru){
    new_sectors <- df %>%
      filter(input == passthru) %>%
      mutate(ratio = value / sum(value)) %>%
      select(scenario, region, sector, year, type, ratio)

    to_expand <- df %>%
      filter(sector == passthru) %>%
      select(-sector, -ratio, -ratio2, -type) %>%
      right_join(new_sectors, by = c("scenario", "region", "year") ) %>%
      mutate(value = value * ratio)

    df <- df %>%
      filter(input != passthru | input == 'total biomass',
             sector != passthru) %>%
      bind_rows(to_expand)

  }


  return(df %>% select(-scenario, -region, -year))
}

# remove transformation sectors from inputs of other transformations
transform_distributer <- function(df, transform_sectors){

  in_passthru_remove_ <- df %>%
    filter(!(input %in% transform_sectors)) %>%
    group_by(scenario,region,year,input,Units) %>%
    summarize(value = sum(value)) %>%
    ungroup()

  for (transform in transform_sectors){
    inputs <- (df %>% filter(sector == transform))$input
    inputs_to_expand <- dplyr::intersect(inputs, c(transform_sectors))

    for (inp in inputs_to_expand){

      df_init <- df

      sector_input <- df %>%
        filter(sector == transform,
               input == inp)

      total <- df %>%
        filter(input == inp)

      total <- sum(total$value)

      # get inputs to sector
      input_df <- df %>%
        filter(sector == inp,
               !(input %in% transform_sectors)) %>%
        mutate(ratio = value / total) %>%
        #select(scenario, region, input, year, ratio, Units) %>%
        group_by(scenario,region,year,input,Units) %>%
        summarize(ratio = sum(ratio)) %>%
        ungroup()

      to_expand <- sector_input %>%
        select(scenario, region, sector, year, type, value) %>%
        right_join(input_df, by = c("scenario", "region", "year") ) %>%
        mutate(value = value * ratio,
               elec_for_H2 = if_else(str_detect(transform,'H2 ') & str_detect(inp,'elect_'),TRUE,FALSE))

      ####
      # Need to subtract any resource that goes into other transformation from original transform sector
      ###
      subtract_from_upstream <- df %>%
        filter(sector == inp,
               !(input %in% transform_sectors)) %>%
        #select(-elec_for_H2) %>%
        left_join(to_expand, by = c("scenario", "region", "input", "year", "Units")) %>%
        mutate(value = value.x - value.y,
               elec_for_H2 = if_else(elec_for_H2.x == TRUE, TRUE, FALSE)) %>%
        select(scenario, region, year, input, sector = sector.x, type = type.x, value, Units,elec_for_H2)

      df <- df %>%
        filter(!(input == inp & sector == transform)) %>%
        bind_rows(to_expand) %>%
        filter(!(sector == inp & !(input %in% transform_sectors))) %>%
        bind_rows(subtract_from_upstream) %>%
        group_by(scenario, region, input, sector, type, year, Units,elec_for_H2) %>%
        summarise(value = sum(value)) %>%
        ungroup()

      df_ <- df %>%
        filter(!(input %in% transform_sectors)) %>%
        group_by(scenario,region,year,input,Units) %>%
        summarize(value = sum(value)) %>%
        ungroup() %>%
        left_join(in_passthru_remove_, by = c('scenario','region','year','input','Units')) %>%
        mutate(value = value.x - value.y,
               fuel_mismatch = if_else(abs(value) > 1e-6, TRUE,FALSE)) %>%
        filter(fuel_mismatch == TRUE) %>%
        mutate(sector = transform,
               type = 'transformation',
               elec_for_H2 = if_else(str_detect(transform,'H2 ') & str_detect(inp,'elect_'),TRUE,FALSE)) %>%
        select(colnames(df))

      if(nrow(df_) > 0){

        #print('warning: sumcheck mismatch in transform_distributer, adding back error term')
        #print(paste0('transform = ',transform))
        #print(paste0('inp = ',inp))

        df <- df %>%
          bind_rows(df_ %>%
                      mutate(value = -value)) %>%
          group_by(scenario,region,year,input,sector,type,Units,elec_for_H2) %>%
          summarize(value = sum(value)) %>%
          ungroup()

      }

    }
  }
  return(df %>% select(-scenario, -region, -year))
}

# similar to fuel tracer but also includes emissions-free energy, and water impacts
energy_water_distributor <- function(prj){
  ###################  Data Tidying ###################  
  
  sector_label <- readr::read_csv("input/sector_label.csv")
  
  input_by_tech <- rgcam::getQuery(prj, "inputs by tech") %>%
    filter(sector != "chemical", sector != "industry") %>%
    group_by(scenario,region,year,sector,subsector,input,Units) %>%
    summarize(value = sum(value)) %>%
    ungroup()
  
  hydro <- rgcam::getQuery(prj, "outputs by tech") %>% 
    filter(sector == 'electricity',
           subsector == 'hydro') %>%
    group_by(scenario,region,year,sector,subsector,output,Units) %>%
    summarize(value = sum(value)) %>%
    ungroup() %>%
    select(-output) %>%
    mutate(input = subsector)

  # create a list of traded ng items which are added in gcam7 -- YQ
  g7_traded_ng <- c("traded Afr_MidE pipeline gas", "traded EUR pipeline gas", "traded LA pipeline gas", "traded LNG",
                    "traded N.Amer pipeline gas", "traded PAC pipeline gas", "traded RUS pipeline gas")
  
  # In this query, regional natural gas sector has two general inputs, natural gas and traded ng (lng, different pineline gas...). The traded ng here represent imported ng.
  # Then the traded ng (has different types, lng, different pipeline gas...) represent the exported ng in each region. 
  regional_ng <- rgcam::getQuery(prj, "inputs by sector") %>% 
    filter(sector == 'regional natural gas') %>%
    mutate(subsector = ifelse(input == "natural gas", "domestic natural gas", "imported natural gas"),
           technology = subsector,
           input = ifelse(input == "natural gas", input, "traded natural gas")) %>%
    group_by(Units, scenario, region, sector, subsector, technology, input, year) %>%
    summarise(value = sum(value, na.rm = TRUE))
  
  traded_ng_stat_diff <- 
    input_by_tech %>% 
    filter(sector %in% g7_traded_ng, 
           subsector == "statistical differences")
  
  traded_ng_no_stat <- 
    input_by_tech %>% 
    filter(sector %in% g7_traded_ng, 
           subsector != "statistical differences")
  
  traded_ng_assign <- 
    traded_ng_no_stat %>%
    group_by(scenario, region, sector, year) %>%
    mutate(ratio = value/sum(value)) %>%
    select(scenario,  region,  year, sector, subsector, ratio) %>%
    filter(year < 2020) %>%
    unique()
  
  traded_ng_export_adjust <- traded_ng_stat_diff %>% 
    select(-subsector, -input) %>%
    right_join(traded_ng_assign, by = c("scenario",  "region",  "year", "sector")) %>%
    mutate(Units = "EJ",
           value = ifelse(is.na(value), 0, value)) %>%
    mutate(value_assign = value * ratio) %>%
    select(-value, -ratio) %>% 
    right_join(traded_ng_no_stat, by = c("scenario",  "region",  "year", "sector", "subsector", "Units")) %>%
    
    #mutate(value_ratio = value_assign/value)
    mutate(value_assign = ifelse(is.na(value_assign), 0, value_assign),
           value_total = value + value_assign) %>%
    select(-value_assign, -value) %>%
    rename(value = value_total)
    
  traded_ng_export <- traded_ng_export_adjust %>%
    separate(subsector, c('reg','fuel'), sep = ' traded ') %>%
    mutate(sector = "traded natural gas",
           fuel = "natural gas",
           subsector = paste(reg, fuel, sep = " traded "),
           technology = subsector) %>%
    group_by(Units, scenario, region, year, sector, subsector, technology, input) %>%
    summarise(value = sum(value))
    
  ng_by_tech <- 
    regional_ng %>% rbind(traded_ng_export) %>%
    ungroup() %>%
    select(-technology)


  input_combine <- bind_rows(input_by_tech %>% filter(!sector %in% c(g7_traded_ng, "gas trade statistical differences")),
                     ng_by_tech,
                     hydro)
  
  # disaggregate the CO2 removal to DAC and rock weathering 
  
  input <- input_combine %>% filter(sector == "CO2 removal") %>% 
    mutate(sector = subsector) %>%
    rbind(input_combine %>% filter(sector != "CO2 removal"))
  
  ###### H2 electricity breakout ###### 
  
  # create separate sector name for electricity used for producing and distributing H2
   # elec_H2_prod_dist <- input %>%
   #   filter(str_detect(sector,'H2') & str_detect(input,'elect_td_'))
   # 
   # elect_td_inputs <- input %>%
   #   filter(str_detect(input,'elect_td_')) %>%
   #   group_by(scenario,region,year,input) %>%
   #   summarize(value = sum(value)) %>%
   #   ungroup()
   # 
   # elect_td_sectors <- input %>%
   #   filter(str_detect(sector,'elect_td_')) %>%
   #   group_by(scenario,region,year,sector) %>%
   #   summarize(value = sum(value)) %>%
   #   ungroup()
   # 
   # elect_td_loss_ratios_for_scaleup <- elect_td_inputs %>%
   #   left_join(elect_td_sectors,by = c('scenario','region','year','input' = 'sector')) %>%
   #   mutate(ratio = value.y / value.x)
   # 
   # elec_net_own_use_H2 <- elec_H2_prod_dist %>%
   #   left_join(elect_td_loss_ratios_for_scaleup, by = c('scenario','region','year','input')) %>%
   #   mutate(value = value * ratio,
   #          input = 'electricity_net_ownuse',
   #          sector = 'elect_td_H2',
   #          subsector = 'elect_td_H2') %>%
   #   select(-value.x,-value.y,-ratio)
   # 
   # elec_H2_prod_dist <- elec_H2_prod_dist %>%
   #   mutate(input = 'elect_td_H2')
   # 
   # #create dataframe to subtract out H2 related electricity from corresponding sectors
   # subtract_H2_elec <- input %>%
   #   filter(str_detect(sector,'H2') & str_detect(input,'elect_td')) %>%
   #   mutate(value = -value)
   # 
   # input <- input %>%
   #   bind_rows(elec_net_own_use_H2,elec_H2_prod_dist,subtract_H2_elec) %>%
   #   group_by(scenario,region,year,sector,subsector,input,Units) %>%
   #   summarize(value = sum(value)) %>%
   #   ungroup() %>%
   #   filter(value != 0)
   # 
   # print("Electricity demand for hydrogen production and distribution broken out")


  # let's only track energy for now
  sectors <- input %>% 
    filter(Units %in% c("EJ")) %>%
    # filter(Units %in% c("EJ", "km^3")) %>%
    # Rewrite transportation subsector to sector
    dplyr::mutate(sector = if_else(grepl("trn_", sector), subsector, sector)) 
  
  primary_water <- sectors %>%
    filter(str_detect(input,"_water")) %>%
    distinct(input)
  
  # Primary sectors are inputs, but don't have any inputs
  primary_sectors <- c(dplyr::setdiff(sectors$input, sectors$sector), 
                       "traded unconventional oil",
                       "total biomass",
                       "nuclearFuelGenIII",
                       "nuclearFuelGenII") 
  
  # But we only care about inputs that have associated emissions:
  # c("coal", "natural gas", "crude oil", "traditional biomass", "biomass", "unconventional oil")
  primary_remove <- setdiff(primary_sectors, 
                            c("coal", "natural gas", "crude oil", "traditional biomass", 
                              "total biomass", "traded unconventional oil",
                              "global solar resource","distributed_solar",
                              "onshore wind resource","offshore wind resource",
                              "geothermal",'hydro',
                              "nuclearFuelGenIII","nuclearFuelGenII",
                              "seawater","biophysical water consumption", primary_water$input))
  
  # Enduse sectors have inputs, but don't act as inputs (LH2 is only a sector, but it look like it is not a input to anyother sector.)
  enduse_sectors <- dplyr::setdiff(sectors$sector, unique(c(sectors$input, "LH2"))) 
  
  
  transformation_sectors <- c("delivered biomass", "delivered coal", "delivered gas",
                              "elect_td_bld", "elect_td_ind", "elect_td_trn",
                              "elect_td_H2",
                              "H2 retail delivery","H2 retail dispensing","H2 industrial","H2 wholesale dispensing","H2 enduse",
                              "H2 wholesale delivery", "H2 MHDV", "LH2", "H2 LDV",
                              "refined liquids enduse", "refined liquids industrial",
                              "wholesale gas", 
                              # traditional biomass is not a transformation sector, need to check -- YQ
                              #"traditional biomass", 
                              "district heat")
  
  # All other sectors are pass-thru sectors
  passthru_sectors <- dplyr::setdiff(sectors$sector, 
                                     c(primary_sectors, enduse_sectors, transformation_sectors))
  
  sector_types <- bind_rows(tibble(sector = primary_sectors, type = "primary"),
                            tibble(sector = transformation_sectors, type = "transformation"),
                            tibble(sector = enduse_sectors, type = "enduse"),
                            tibble(sector = passthru_sectors, type = "passthru"))
  
  # group inputs by sector (subsector no longer relevant)
  input_tracing <- sectors %>% 
    group_by(scenario, region, input, sector, year,Units) %>%
    summarise(value = sum(value)) %>%
    ungroup() %>%
    # Add in sector types
    left_join(sector_types, by = "sector") %>%
    mutate(sector = if_else((sector == 'biomass' & Units == 'km^3'),'total biomass',sector))
  
  print("Data ready for distribution.")
  
  ###################  Input Distributing  ###################  
  # First want to remove unnecessary primary inputs and sectors that depend only on them
  #
  primary_remove_df <- input_tracing %>%
    filter(input %in% primary_remove)
  
  single_use_sectors <- input_tracing %>%
    filter(sector %in% primary_remove_df$sector) %>% 
    group_by(scenario, region, sector, year,Units) %>%
    add_count() %>%
    filter(n == 1,
           sector != "regional biomass") %>%
    ungroup() %>%
    # semi_join(x, y) -- keep rows of x that have a match (also exist) in y.
    semi_join(primary_remove_df, by = c("scenario", "region", "input", "sector", "year","Units"))

  # Remove unnecessary sectors
  global_inputs <- input_tracing %>%
    filter(!(input %in% primary_remove)) %>%
    # anti_join(x, y) -- keep rows of x that do not have a match (don't exist) in y.
    anti_join(single_use_sectors, by = c("scenario", "region", "sector", "year", "Units"))  
  
  
  # Get ratio of sector in each input (percentage of each input type used in different sectors)
  in_ratio <- global_inputs %>%
    
    #filter out exported fossil fuels as these will ultimately be counted in the destination countries regional fuel markets
    filter(!(sector %in% c('traded natural gas','traded coal','traded oil','traded biomass'))) %>% 
    
    #now mutate imported fossil fuels to their fuel names, assuming they are then consumed within that same region
    mutate(input = if_else(input == 'traded oil', 'crude oil', input),
           input = if_else(input == 'traded natural gas', 'natural gas', input),
           input = if_else(input == 'traded coal', 'coal', input)) %>%
    group_by(scenario, region, input, year,Units) %>%
    mutate(ratio = value / sum(value)) %>%
    ungroup()  %>%
    # Remove nans - these sectors no longer needed
    na.omit() %>%
    group_by(scenario,region,input,sector,year,type,Units) %>%
    summarize(value = sum(value),
              ratio = sum(ratio)) %>%
    ungroup()

  # If ratio = 1 and input is not a primary input, 
  # replace sectors with input name with downstream sector name and type,
  # then delete downstream row
  # This step is essentially to simplify the supply chain. 
  # E.g., (traded biomass + biomass) -> total biomass -> regional biomass. This is simplified to total biomass -> regional biomass
  #       crude oil -> regional oil -> refining. This is simplified to crude oil -> refining. 
  in_replace_downstream_temp <- in_ratio %>%
    group_by(scenario, region, year) %>%
    group_modify(~downstream_replacer(., primary_sectors), keep=TRUE) %>%
    ungroup()
  
  
  # Sum to combine a few weird sectors (2 regional biomasses bc of trading)
  # Also for a few sectors the sector name is changed, e.g., elec_coal (conv pul) and elec_coal (IGCC) are changed into electricity_net_ownuse, which all have regional coal as input. So we need to re-calculate the ratio. 
  in_replace_downstream <- in_replace_downstream_temp %>% 
    # ratio -- the percentage of one input used in different sector
    group_by(scenario, region, input, sector, year, type,Units) %>%
    summarise(value = sum(value), 
              ratio = sum(ratio)) %>%
    ungroup() %>% 
    # ratio2 -- the contribution (%) of different inputs to a specific sector.
    group_by(scenario, region, sector, year,Units) %>%
    mutate(ratio2 = value / sum(value)) %>%
    ungroup()  %>%
    # Remove nans - these sectors no longer matter
    na.omit()
  
  print("Downstream passthru sectors replaced")

  
  # If both ratios = 1
  # replace inputs with sector name with upstream input name,
  # then delete upstream row
  # this function performs similar functionality as the downstream_replacer function. It basically get rid of upstream processes.
  # Eg. total biomass -> regional biomass -> all different sectors that uses biomass (delivered biomass, H2 central production, refining, gas pipeline, electricity net ownuse). This function basically link total biomass directly to different sectors that uses biomass. and remove the total bioamss -> regional biomass process. 
  # another similar example is coal -> regional coal -> different sectors that use coal. 
  in_replace_upstream <- in_replace_downstream %>%
    group_by(scenario, region, year, Units) %>%
    group_modify(~upstream_replacer(.), keep=TRUE) %>%
    ungroup() %>%
    # change all the elect related input to H2 related sectors (electricity consumption in H2 related sectors) as transformation type.
    mutate(type = if_else(str_detect(sector,'H2') & str_detect(input,'elect'),'transformation',type))
  
  print("Upstream passthru sectors replaced")
  
  # For each pass thru sector left, need to use ratios to apportion to transformation sectors
  # This part is basically get rid of the passthru sectors, and link the upsteam and downstream of the passthru sector.
  # eg. electricity_net_ownuse is a passthru sector, it links various upstream inputs (backup_electricity, geothermal, regional coal, etc) and downstream processes (elect_td_bld, elect_td_ind, and elect_td_trn). 
  # after this the upstream inputs and the downstream processes are directly linked. 
  in_passthru_remove <- in_replace_upstream %>%
    group_by(scenario, region, year) %>%
    group_modify(~passthru_remove(.), keep=TRUE) %>%
    ungroup()
    
  # need to redo ratio of sector in each input
  in_passthru_remove_biomass_coal <- in_passthru_remove %>%
    group_by(scenario, region, input, year) %>%
    # ratio represent the percentage of one input used in different sectors
    mutate(ratio = value / sum(value)) %>%
    ungroup()%>%
  # Getting rid of 1975 bc electricity is different
    filter(year >= 1990)%>%
  # Repeating fuel distribution with delivered biomass and coal
    group_by(scenario, region, year) %>%
    group_modify(~passthru_remove(., c( "delivered biomass", "delivered coal")), keep=TRUE) %>%
    ungroup()
  
  print("Delivered coal & biomass replaced")
  
  # Old years (<= 2010) need to rename regional biomass to biomass
  in_passthru_remove_biomass_coal_modified <- in_passthru_remove_biomass_coal %>%
    mutate(input = if_else(input == "regional biomass", "total biomass", input)) 

  # sum things up
  in_passthru_remove_final <- in_passthru_remove_biomass_coal_modified %>%
    group_by(scenario, region, input, sector, type, year, Units) %>%
    summarise(value = sum(value)) %>%
    ungroup() 
  
  #elect_td_ind is somewhat unique in that it functions both as a transformation sector for energy, as well as a 
  #passthru sector for irrigation.  After removing passthru sectors above we are left in effect with elect_td_ind 
  #consuming itself, which represents electricity use for irrigation. 
  #Therefore we adjust upwards the amount of electricity for all sectors by the amount of electricity used to pump water, 
  #and drop any remaining rows where sectors consume themselves as inputs

   energy_for_water <- in_passthru_remove_final %>%
     filter(input == sector) %>%
     select(-sector,-Units,-value,-type) %>%
     left_join(in_passthru_remove_final, by = c("scenario","region","year","input")) %>%
     group_by(scenario,region,year,input,Units) %>%
     mutate(total_input = sum(value),
            own_use = sum(value[which(input==sector)])/total_input,
            value = value * 1 + own_use) %>%
     filter(!(sector == input)) %>%
     ungroup() %>%
     select(-total_input,-own_use)
   
   in_passthru_remove_ready <- in_passthru_remove_final %>%
     anti_join(energy_for_water,by = c("scenario","region","input","year")) %>%
     bind_rows(energy_for_water) %>%
     mutate(elec_for_H2 = if_else(str_detect(sector,'H2') & str_detect(input,'elect_td_') | input == 'elect_td_H2' | sector == 'elect_td_H2',TRUE,FALSE))
   
    
  print("Remaining passthru sectors replaced")
  
  
  transform_sectors <- c("H2 enduse","H2 retail delivery","H2 retail dispensing","H2 wholesale dispensing","H2 industrial",
                         "H2 wholesale delivery", "H2 MHDV", "LH2", "H2 LDV",
                         "elect_td_bld", "elect_td_trn", "elect_td_ind",
                         "district heat", "refined liquids enduse", "refined liquids industrial",
                         "delivered gas","wholesale gas")

  in_primary <- in_passthru_remove_ready %>%
    group_by(scenario, region, year) %>%
    group_modify(~transform_distributer(., transform_sectors), keep=TRUE) %>%
    ungroup() 
  

  remaining_transform_sectors <- in_primary %>%
    filter(sector %in% transform_sectors & input %in% transform_sectors)
  
  remaining_transform_sectors <- remaining_transform_sectors$sector
  
  count = 0 
  
  while (length(remaining_transform_sectors) > 0) {
    
    in_primary <- in_primary %>%
      group_by(scenario, region, year) %>%
      group_modify(~transform_distributer(., remaining_transform_sectors), keep=TRUE) %>%
      ungroup() 
    
    remaining_transform_sectors <- in_primary %>%
      filter(sector %in% transform_sectors & input %in% transform_sectors)
    
    remaining_transform_sectors <- remaining_transform_sectors$sector
    
    count = count + 1
  
  }
  
  #clean up negative values from H2 industrial into elec and vice versa
  in_primary <- in_primary %>%
    mutate(elec_for_H2 = if_else(value < 0 & elec_for_H2 == FALSE, TRUE,
                                 if_else(value <0 & elec_for_H2 == TRUE, FALSE,
                                         elec_for_H2))) %>%
    mutate(input = if_else(input == 'H2 retail dispensing','H2 wholesale dispensing',input),
           sector = if_else(sector == 'H2 retail dispensing','H2 wholesale dispensing',sector)) %>%
    group_by(scenario,region,year,input,sector,type,Units,elec_for_H2) %>%
    summarize(value = sum(value)) %>%
    ungroup() 
  
  # H2_inputs <- in_passthru_remove %>%
  #   filter(str_detect(sector,'H2'),
  #          input %in% in_primary$input,
  #          !(input %in% in_passthru_remove$sector))
  # 
  # #if inputs found that are not directly used for H2 production, they must be from electricity that is then used to produce H2
  # in_primary <- in_primary %>%
  #   mutate(elec_for_H2 = if_else(str_detect(sector,'H2') & !(input %in% H2_inputs$input),TRUE, elec_for_H2))
  

  print("Transformation sectors removed as inputs to other transformations")
  if (count > 0){
    print(paste0(count,' additional iterations required to remove all transformation to transformation inputs'))
  }
  
  # Now need to separate transformation from enduse
  # Apportion primary -> transformation -> enduse
  transform_df <- in_primary %>%
    filter(type == "transformation")
  
  # Get ratio of enduse in each transformation sector
  enduse_df <- in_primary %>%
    filter(type == "enduse") %>%
    rename(enduse = sector) %>% 
    group_by(scenario, region, input, year, Units) %>%
    mutate(ratio_enduse_in_input = value / sum(value)) %>%
    ungroup() %>%
    select(-elec_for_H2)
  
  # Get ratio of input in each transformation sector
  transform_df <- transform_df %>% 
    group_by(scenario, region, sector, year, Units,elec_for_H2) %>%
    mutate(ratio_primary_in_trans = value / sum(value)) %>%
    ungroup() %>%
    rename(transformation = sector, primary = input) %>%
    select(-type)
  
  # Add in natural gas for unconventional oil production
  gas_in_unconventional_oil <- in_ratio %>% 
    filter(sector == "unconventional oil production",
           input == "regional natural gas") %>%
    mutate(input = "natural gas",
           enduse = sector) %>%
    select(scenario, region, year, primary = input, transformation = sector, enduse, value, Units) %>%
    mutate(elec_for_H2 = FALSE)
  
  
  
  resid_ene_mapping <- 
    sector_label %>% 
    filter(rewrite %in% c("resid cooling", "resid heating", "resid others")) %>% 
    select(-type)
  
  # Expand all transformation inputs in enduse_df to get primary inputs
  final_df_all <- enduse_df %>%
    full_join(transform_df, by = c("scenario", "region", "year", "input" = "transformation")) %>%
    mutate(primary = if_else(is.na(primary), input, primary),
           value = if_else(is.na(value.y), value.x, value.y * ratio_enduse_in_input),
           input = if_else(input==primary, enduse,input)) %>%
    select(scenario, region, year, primary, transformation = input, enduse, value,elec_for_H2) %>%
    bind_rows(gas_in_unconventional_oil)
  
  # Group electricity, refining, gas processing, H2 production
  final_df <- final_df_all %>%
    left_join(resid_ene_mapping, by = c("transformation" = "sector")) %>%
    mutate(transformation = if_else(is.na(rewrite), transformation, rewrite), 
           transformation = if_else(transformation %in% c("elect_td_bld", "elect_td_ind","elect_td_trn"),
                                    "electricity", transformation),
           transformation = if_else(transformation %in% c("refined liquids enduse", "refined liquids industrial"),
                                    "refining", transformation),
           transformation = if_else(transformation %in% c("delivered gas", "wholesale gas"),
                                    "gas processing", transformation),
           primary = if_else(str_detect(primary,'_water consumption'),'water consumption', primary),
           primary = if_else(str_detect(primary,'_water withdrawals'),'water withdrawals', primary),
           enduse = if_else(is.na(rewrite), enduse, rewrite), 
           Units = if_else(primary %in% c('water consumption','water withdrawals','biophysical water consumption','seawater'),'km^3','EJ'),
           elec_for_H2 = if_else(is.na(elec_for_H2),FALSE,elec_for_H2)) %>%
    
    group_by(scenario, region, year, primary, transformation, enduse, Units,elec_for_H2) %>%
    summarise(value = sum(value)) %>%
    ungroup()
  
  # final_df <- final_df %>%
  #   # Get ratio of enduse in primary
  #   group_by(scenario, region, year, primary, Units) %>%
  #   mutate(ratio_enduse_in_primary = value / sum(value)) %>%
  #   # Get ratio of enduse in transformation 
  #   group_by(scenario, region, year, transformation, Units) %>%
  #   mutate(ratio_enduse_in_transformation = value / sum(value)) %>%
  #   ungroup() 
  
  print("Final distributions calculated.")
  
  ###################  Checking Totals  ###################  
  # NOW NEED TO DO A CHECK TO MAKE SURE MATH ADDS UP FOR EACH FUEL/REGION
  original_totals <- in_ratio %>%
    filter(year >= 1990) %>%
    filter(input %in% c("coal", "natural gas", "crude oil", "regional biomass", "traded unconventional oil",
                        "global solar resource","distributed_solar","geothermal","onshore wind resource",
                        "offshore wind resource","biophysical water consumption", "traditional biomass")) %>%
    group_by(scenario, region, year, input) %>%
    summarise(value = sum(value)) %>%
    ungroup() %>%
    mutate(input = if_else(input == "regional biomass", "total biomass", input))
  
  new_totals <- final_df %>%
    filter(primary %in% c("coal", "natural gas", "crude oil", "total biomass", "traded unconventional oil",
                          "global solar resource","distributed_solar","geothermal","onshore wind resource",
                          "offshore wind resource","biophysical water consumption", "traditional biomass")) %>%
    group_by(scenario, region, year, primary) %>%
    summarise(value = sum(value)) %>%
    ungroup() %>%
    rename(input = primary)
  
  comp <- original_totals %>%
    rename(original_total = value) %>%
    left_join(new_totals %>% rename(new_total = value), by = c("scenario", "region", "year", "input")) %>%
    mutate(diff = round(original_total - new_total, 3))
  
  
  
  if (any(is.na(comp))){
    print("NAs in fuel total comparison (likely due to historical oil)")
    write_csv(comp %>%
                filter(is.na(new_total)),'NAS_in_fuel_totals.csv')
  }
  
  if (any(abs(comp$diff) > 0, na.rm = TRUE)){
    rows <- nrow(comp %>% filter(abs(diff) > 0))
    print(paste0("Fuel totals incorrect in ", rows, " rows."))
    write_csv(comp %>% filter(abs(diff) > 0),'fuel_totals_mismatched.csv')
    write_csv(final_df,'energy_water_tracing.csv')
  } else {
    print("All fuel totals correct.")
  }
  
  
  return(final_df)
}

fuel_distributor <- function(prj){
  ###################  Data Tidying ###################  
  input <- rgcam::getQuery(prj, "inputs by tech") %>%
    # Remove industry and chemical, as they simply aggregate more detailed sectors
    filter(sector != "chemical", sector != "industry") %>%
    group_by(scenario,region,year,sector,subsector,input,Units) %>%
    summarize(value = sum(value)) %>%
    ungroup()
  
  sectors <- input %>% 
    filter(Units == "EJ") %>%
    # Rewrite transportation subsector to sector
    dplyr::mutate(sector = if_else(grepl("trn_", sector), subsector, sector)) 
  
  # Primary sectors are inputs, but don't have any inputs
  primary_sectors <- c(dplyr::setdiff(sectors$input, sectors$sector), 
                       "traded unconventional oil",
                       "total biomass")
  # But we only care about inputs that have associated emissions:
  # c("coal", "natural gas", "crude oil", "traditional biomass", "biomass", "unconventional oil")
  primary_remove <- setdiff(primary_sectors, 
                            c("coal", "natural gas", "crude oil", "traditional biomass", 
                              "total biomass", "traded unconventional oil"))
  
  # Enduse sectors have inputs, but don't act as inputs
  enduse_sectors <- dplyr::setdiff(sectors$sector, sectors$input)
  
  transformation_sectors <- c("delivered biomass", "delivered coal", "delivered gas",
                              "elect_td_bld", "elect_td_ind", "elect_td_trn",
                              "H2 retail delivery","H2 retail dispensing","H2 industrial","H2 wholesale dispensing","H2 enduse",
                              "refined liquids enduse", "refined liquids industrial",
                              "wholesale gas", "traditional biomass", "district heat")
  
  # All other sectors are pass-thru sectors
  passthru_sectors <- dplyr::setdiff(sectors$sector, 
                                     c(primary_sectors, enduse_sectors, transformation_sectors))
  
  sector_types <- bind_rows(tibble(sector = primary_sectors, type = "primary"),
                            tibble(sector = transformation_sectors, type = "transformation"),
                            tibble(sector = enduse_sectors, type = "enduse"),
                            tibble(sector = passthru_sectors, type = "passthru"))
  
  # group inputs by sector (subsector no longer relevant)
  input_tracing <- sectors %>% 
    group_by(scenario, region, input, sector, year) %>%
    summarise(value = sum(value)) %>%
    ungroup() %>%
    # Add in sector types
    left_join(sector_types, by = "sector")
  
  print("Data ready for distribution.")
  
  ###################  Input Distributing  ###################  
  # First want to remove unnecessary primary inputs and sectors that depend only on them
  #
  primary_remove_df <- input_tracing %>%
    filter(input %in% primary_remove)
  
  single_use_sectors <- input_tracing %>%
    filter(sector %in% primary_remove_df$sector) %>% 
    group_by(scenario, region, sector, year) %>%
    add_count() %>%
    filter(n == 1,
           sector != "regional biomass") %>%
    ungroup() %>%
    semi_join(primary_remove_df, by = c("scenario", "region", "input", "sector", "year"))
  
  # Remove unnecessary sectors
  global_inputs <- input_tracing %>%
    filter(!(input %in% primary_remove)) %>%
    anti_join(single_use_sectors, by = c("scenario", "region", "sector", "year"))  
  
  
  exported_fuels_for_subtraction <- input %>%
    filter(sector %in% c('traded coal','traded oil','traded natural gas')) %>%
    separate(subsector, c('reg','fuel'), sep = ' traded') %>% #need the leading space character for matching by region later
    mutate(value = -value, #this value will be subtracted from each region's fuel total as it is exported and not consumed there
           region = reg,
           type = 'passthru',
           sector = if_else(input == 'coal', 'regional coal',
                            if_else(input == 'natural gas', 'regional natural gas',
                                    if_else(input == 'crude oil', 'regional oil', NA_character_)))) %>%
    select(scenario,region,sector,year,input,value,type)
  
  # Next, want to get rid of passthru sectors that only have 1 input
  
  # Get ratio of sector in each input
  in_ratio <- global_inputs %>%
    
    #filter out exported fossil fuels as these will ultimately be counted in the destination countries regional fuel markets
    filter(!(sector %in% c('traded natural gas','traded coal','traded oil'))) %>% 
    
    #now mutate imported fossil fuels to their fuel names, assuming they are then consumed within that same region
    mutate(input = if_else(input == 'traded oil', 'crude oil', input),
           input = if_else(input == 'traded natural gas', 'natural gas', input),
           input = if_else(input == 'traded coal', 'coal', input)) %>%
    group_by(scenario, region, input, year) %>%
    mutate(ratio = value / sum(value)) %>%
    ungroup()  %>%
    # Remove nans - these sectors no longer needed
    na.omit() %>%
    group_by(scenario,region,input,sector,year,type) %>%
    summarize(value = sum(value),
              ratio = sum(ratio)) %>%
    ungroup()
  
  # If ratio = 1 and input is not a primary input, 
  # replace sectors with input name with downstream sector name and type,
  # then delete downstream row
  in_replace_downstream <- in_ratio %>%
    group_by(scenario, region, year) %>%
    group_modify(~downstream_replacer(., primary_sectors), keep=TRUE) %>%
    ungroup()
  
  print("Downstream passthru sectors replaced")
  
  # Sum to combine a few weird sectors (2 regional biomasses bc of trading)
  in_replace_downstream <- in_replace_downstream %>% 
    group_by(scenario, region, input, sector, year, type) %>%
    summarise(value = sum(value), 
              ratio = sum(ratio)) %>%
    ungroup()
  
  # Get ratio of inputs in each sector
  in_replace_downstream <- in_replace_downstream %>% 
    group_by(scenario, region, sector, year) %>%
    mutate(ratio2 = value / sum(value)) %>%
    ungroup()  %>%
    # Remove nans - these sectors no longer matter
    na.omit() %>%
    mutate(Units = 'EJ') #add dummy units column for compatibility with full energy water tracer version of upstream replacer function
  
  
  # If both ratios = 1
  # replace inputs with sector name with upstream input name,
  # then delete upstream row
  in_replace_upstream <- in_replace_downstream %>%
    group_by(scenario, region, year) %>%
    group_modify(~upstream_replacer(.), keep=TRUE) %>%
    ungroup()
  
  print("Upstream passthru sectors replaced")
  
  # For each pass thru sector left, need to use ratios to apportion to transformation sectors
  #
  in_passthru_remove <- in_replace_upstream %>%
    group_by(scenario, region, year) %>%
    group_modify(~passthru_remove(.), keep=TRUE) %>%
    ungroup()
  
  # need to redo ratio of sector in each input
  in_passthru_remove <- in_passthru_remove %>%
    group_by(scenario, region, input, year) %>%
    mutate(ratio = value / sum(value)) %>%
    ungroup()
  
  # Getting rid of 1975 bc electricity is different
  in_passthru_remove <- in_passthru_remove %>%
    filter(year >= 1990)
  
  # Repeating fuel distribution with delivered biomass and coal
  in_passthru_remove <- in_passthru_remove %>%
    group_by(scenario, region, year) %>%
    group_modify(~passthru_remove(., c( "delivered biomass", "delivered coal")), keep=TRUE) %>%
    ungroup()
  
  print("Delivered coal & biomass replaced")
  
  # Old years (<= 2010) need to rename regional biomass to biomass
  in_passthru_remove <- in_passthru_remove %>%
    mutate(input = if_else(input == "regional biomass", "total biomass", input)) 
  
  # sum things up
  in_passthru_remove <- in_passthru_remove %>%
    group_by(scenario, region, input, sector, type, year) %>%
    summarise(value = sum(value)) %>%
    ungroup() 
  
  
  print("Remaining passthru sectors replaced")
  
  # Now need to remove transformation sectors from inputs of other transformations
  # ASSUMING THAT REFINED LIQUIDS ARE UPSTREAM OF ELECTRICITY
  # ASSUME ELECTRICITY UPSTREAM OF HYDROGEN
  
  transform_sectors <- c("H2 enduse","H2 retail delivery","H2 retail dispensing","H2 wholesale dispensing","H2 industrial",
                         "elect_td_bld", "elect_td_ind", "elect_td_trn",
                         "district heat", "refined liquids enduse", "refined liquids industrial",
                         "delivered gas","wholesale gas")
  
  in_primary <- in_passthru_remove %>%
    mutate(Units = 'EJ') %>%
    group_by(scenario, region, year) %>%
    group_modify(~transform_distributer(., transform_sectors), keep=TRUE) %>%
    ungroup() 
  
  print("Transformation sectors removed as inputs to other transformations")
  
  # Now need to separate transformation from enduse
  # Apportion primary -> transformation -> enduse
  transform_df <- in_primary %>%
    filter(type == "transformation")
  
  # Get ratio of enduse in each transformation sector
  enduse_df <- in_primary %>%
    filter(type == "enduse") %>%
    rename(enduse = sector) %>% 
    group_by(scenario, region, input, year) %>%
    mutate(ratio_enduse_in_input = value / sum(value)) %>%
    ungroup()
  
  # Get ratio of input in each transformation sector
  transform_df <- transform_df %>% 
    group_by(scenario, region, sector, year) %>%
    mutate(ratio_primary_in_trans = value / sum(value)) %>%
    ungroup() %>%
    rename(transformation = sector, primary = input) %>%
    select(-type)
  
  # Add in natural gas for unconventional oil production
  gas_in_unconventional_oil <- in_ratio %>% 
    filter(sector == "unconventional oil production",
           input == "regional natural gas") %>%
    mutate(input = "natural gas",
           enduse = sector) %>%
    select(scenario, region, year, primary = input, transformation = sector, enduse, value)
  
  # Expand all transformation inputs in enduse_df to get primary inputs
  final_df <- enduse_df %>%
    full_join(transform_df, by = c("scenario", "region", "year", "input" = "transformation")) %>%
    mutate(primary = if_else(is.na(primary), input, primary),
           value = if_else(is.na(value.y), value.x, value.y * ratio_enduse_in_input),
           input = if_else(input==primary, enduse,input)) %>%
    select(scenario, region, year, primary, transformation = input, enduse, value) %>%
    filter(!(primary == 'natural gas' & transformation == 'unconventional oil production')) %>%
    bind_rows(gas_in_unconventional_oil) 
  
  # Group electricity, refining, gas processing, H2 production
  final_df <- final_df %>%
    mutate(transformation = if_else(transformation %in% c("elect_td_bld", "elect_td_ind","elect_td_trn"),
                                    "electricity", transformation),
           transformation = if_else(transformation %in% c("refined liquids enduse", "refined liquids industrial"),
                                    "refining", transformation),
           transformation = if_else(transformation %in% c("delivered gas", "wholesale gas"),
                                    "gas processing", transformation)) %>%
    group_by(scenario, region, year, primary, transformation, enduse) %>%
    summarise(value = sum(value)) %>%
    ungroup()
  
  ####Disentangle the H2 web...
  if (!any(str_detect('H2 enduse',final_df$transformation))){
  fuel_tracing <- final_df
  
  outputs_by_subsector <- rgcam::getQuery(prj,'outputs by tech')   %>%
    group_by(scenario,region,year,sector,subsector,output,Units) %>%
    summarize(value = sum(value)) %>%
    ungroup()
  
  H2_forecourt_sectors <-  outputs_by_subsector %>%
    filter(subsector == 'forecourt production') %>%
    distinct(sector)
  
  H2_central_frac <- outputs_by_subsector %>%
    filter(sector %in% H2_forecourt_sectors$sector) %>%
    group_by(scenario,region,year,sector) %>%
    mutate(central_production_frac = if_else(subsector == 'forecourt production',1-value/sum(value),value/sum(value))) %>%
    ungroup() %>%
    rename(transformation = sector) %>%
    select(scenario,region,year,transformation,central_production_frac) %>%
    distinct(scenario,region,year,transformation,.keep_all = TRUE)
  
  fuel_tracing_fix_H2 <- fuel_tracing %>%
    filter(str_detect(transformation,'H2')) %>%
    mutate(transformation = if_else(transformation == 'H2 retail dispensing','H2 wholesale dispensing',transformation))
  
  fuel_tracing_fix_H2_wholesale_dispensing <- fuel_tracing_fix_H2 %>%
    filter(transformation == 'H2 wholesale dispensing') %>%
    left_join(H2_central_frac,by = c('scenario','region','year','transformation'))
  
  fuel_tracing_wholesale_dispensing_forecourt <- fuel_tracing_fix_H2_wholesale_dispensing %>%
    mutate(value = value * (1-central_production_frac)) %>%
    select(-central_production_frac)
  
  fuel_tracing_wholesale_dispensing_central <- fuel_tracing_fix_H2_wholesale_dispensing %>%
    mutate(value = value * central_production_frac,
           transformation = 'H2 central production') %>%
    select(-central_production_frac)
  
  fuel_tracing_central <- fuel_tracing_fix_H2 %>%
    filter(transformation %in% c('H2 industrial','H2 retail delivery')) %>%
    mutate(transformation = 'H2 central production')
  
  fuel_tracing_FIXED_H2 <- bind_rows(fuel_tracing_wholesale_dispensing_central,fuel_tracing_wholesale_dispensing_forecourt,fuel_tracing_central)
  
  fuel_tracing_no_H2 <- fuel_tracing %>%
    filter(!str_detect(transformation,'H2')) %>%
    bind_rows(fuel_tracing %>% filter(transformation == 'H2 enduse')) #add back H2 enduse for backwards compatibility
  
  final_df <- bind_rows(fuel_tracing_no_H2,fuel_tracing_FIXED_H2)
  } 
  #####
  
  final_df <- final_df %>%
    # Get ratio of enduse in primary
    group_by(scenario, region, year, primary) %>%
    mutate(ratio_enduse_in_primary = value / sum(value)) %>%
    # Get ratio of enduse in transformation 
    group_by(scenario, region, year, transformation) %>%
    mutate(ratio_enduse_in_transformation = value / sum(value)) %>%
    ungroup() 
  
  print("Final distributions calculated.")
  
  ###################  Checking Totals  ###################  
  # NOW NEED TO DO A CHECK TO MAKE SURE MATH ADDS UP FOR EACH FUEL/REGION
  original_totals <- in_ratio %>%
    filter(year >= 1990) %>%
    filter(input %in% c("coal", "natural gas", "crude oil", "regional biomass", "traded unconventional oil")) %>%
    group_by(scenario, region, year, input) %>%
    summarise(value = sum(value)) %>%
    ungroup() %>%
    mutate(input = if_else(input == "regional biomass", "total biomass", input))
  
  new_totals <- final_df %>%
    filter(primary %in% c("coal", "natural gas", "crude oil", "total biomass", "traded unconventional oil")) %>%
    group_by(scenario, region, year, primary) %>%
    summarise(value = sum(value)) %>%
    ungroup() %>%
    rename(input = primary)
  
  comp <- original_totals %>%
    rename(original_total = value) %>%
    left_join(new_totals %>% rename(new_total = value), by = c("scenario", "region", "year", "input")) %>%
    mutate(diff = round(original_total - new_total, 3))
  
  
  
  if (any(is.na(comp))){
    print("NAs in fuel total comparison (likely due to historical oil)")
    write_csv(comp %>% filter(is.na(diff)),'nas_in_fuel_total_comparison.csv')
  }
  
  if (any(abs(comp$diff) > 0, na.rm = TRUE)){
    rows <- nrow(comp %>% filter(abs(diff) > 0))
    print(paste0("Fuel totals incorrect in ", rows, " rows."))
    write_csv(comp %>% filter(abs(diff) > 0),'fuel_totals_mismatched.csv')
    write_csv(final_df,'fuel_tracing.csv')
  } else {
    print("All fuel totals correct.")
  }
  
  return(final_df)
}

H2_tracer <- function(final_df){
  ####Disentangle the H2 web...
  if (!any(str_detect('H2 enduse',final_df$transformation))){
    fuel_tracing <- final_df
    
    outputs_by_subsector <- rgcam::getQuery(prj,'outputs by tech')   %>%
      group_by(scenario,region,year,sector,subsector,output,Units) %>%
      summarize(value = sum(value)) %>%
      ungroup()
    
    H2_forecourt_sectors <-  outputs_by_subsector %>%
      filter(subsector == 'forecourt production') %>%
      distinct(sector)
    
    H2_central_frac <- outputs_by_subsector %>%
      filter(sector %in% H2_forecourt_sectors$sector) %>%
      group_by(scenario,region,year,sector) %>%
      mutate(central_production_frac = if_else(subsector == 'forecourt production',1-value/sum(value),value/sum(value))) %>%
      ungroup() %>%
      rename(transformation = sector) %>%
      select(scenario,region,year,transformation,central_production_frac) %>%
      distinct(scenario,region,year,transformation,.keep_all = TRUE)
    
    fuel_tracing_fix_H2 <- fuel_tracing %>%
      filter(str_detect(transformation,'H2')) %>%
      mutate(transformation = if_else(transformation == 'H2 retail dispensing','H2 wholesale dispensing',transformation))
    
    fuel_tracing_fix_H2_wholesale_dispensing <- fuel_tracing_fix_H2 %>%
      filter(transformation == 'H2 wholesale dispensing') %>%
      left_join(H2_central_frac,by = c('scenario','region','year','transformation'))
    
    fuel_tracing_wholesale_dispensing_forecourt <- fuel_tracing_fix_H2_wholesale_dispensing %>%
      mutate(value = value * (1-central_production_frac)) %>%
      select(-central_production_frac)
    
    fuel_tracing_wholesale_dispensing_central <- fuel_tracing_fix_H2_wholesale_dispensing %>%
      mutate(value = value * central_production_frac,
             transformation = 'H2 central production') %>%
      select(-central_production_frac)
    
    fuel_tracing_central <- fuel_tracing_fix_H2 %>%
      filter(transformation %in% c('H2 industrial','H2 retail delivery')) %>%
      mutate(transformation = 'H2 central production')
    
    fuel_tracing_FIXED_H2 <- bind_rows(fuel_tracing_wholesale_dispensing_central,fuel_tracing_wholesale_dispensing_forecourt,fuel_tracing_central)
    
    fuel_tracing_no_H2 <- fuel_tracing %>%
      filter(!str_detect(transformation,'H2')) %>%
      bind_rows(fuel_tracing %>% filter(transformation == 'H2 enduse')) #add back H2 enduse for backwards compatibility
    
    final_df <- bind_rows(fuel_tracing_no_H2,fuel_tracing_FIXED_H2)
    
    final_df
    
    return(final_df)
  } 
  #####
  
  final_df <- final_df %>%
    # Get ratio of enduse in primary
    group_by(scenario, region, year, primary) %>%
    mutate(ratio_enduse_in_primary = value / sum(value)) %>%
    # Get ratio of enduse in transformation 
    group_by(scenario, region, year, transformation) %>%
    mutate(ratio_enduse_in_transformation = value / sum(value)) %>%
    ungroup()
  
  print("Final distributions calculated.")
}


# this function is used to disaggregate CO2 emission. This includes 
# 1. disaggregating CO2 emissions from electricity generation by fuel types.
# 2. disaggregating CO2 emissions from refining processes (used in transport) by fuel types.
# 3. disaggregating CO2 emissions from H2 production by fuel types.

final_fuel_CO2_disag <- function(all_emissions){
  
  # Step 1. prepare for some input data
  sectors <- read_csv('input/sector_label.csv')
  elec_gen_fuels <- read_csv('input/elec_generation.csv')
  transport_sectors_input <- read_csv('input/transport.csv')  
  ccoef_mapping <- read_csv('input/ccoef_mapping.csv')
  CO2_sequestration_by_tech <- rgcam::getQuery(prj, 'CO2 sequestration by tech')
  CO2_bio <- rgcam::getQuery(prj, "CO2 emissions by tech (excluding resource production)")
  CO2 <- rgcam::getQuery(prj, "CO2 emissions by sector (no bio) (excluding resource production)")
  output_by_tech <- rgcam::getQuery(prj, 'outputs by tech')
  
  transformation_sectors <- (sectors %>%
    filter(type == 'transformation') %>%
    distinct(rewrite))$rewrite
  
  transport_sectors <- transport_sectors_input$transportation_subsector
  
  sectors_elec <- sectors %>%
    filter(rewrite == 'electricity')
  
  inputs_by_subsector_raw <- 
    rgcam::getQuery(prj, "inputs by tech") %>%
    group_by(scenario,region,year,sector,subsector,input,Units) %>%
    summarize(value = sum(value)) %>%
    ungroup()
  
  # disaggregate for co2 removal to rock weathering and dac
  inputs_by_subsector <- 
    inputs_by_subsector_raw %>% 
    filter(sector == "CO2 removal") %>% 
    mutate(sector = subsector) %>%
    rbind(inputs_by_subsector_raw %>% 
            filter(sector != "CO2 removal"))
  
  # separate all emission to CO2 vs non CO2 and process them separately
  CO2_emiss <- all_emissions %>%
    select(scenario,region,year,direct,transformation,enduse,ghg,value,Units,elec_for_H2) %>%
    filter(ghg == 'CO2') 
  
  nonCO2_emiss <- all_emissions %>%
    select(scenario,region,year,direct,transformation,enduse,ghg,value,Units,elec_for_H2) %>%
    filter(ghg != 'CO2') 
  
  CO2_emiss_transform <- CO2_emiss %>%
    filter(direct %in% transformation_sectors) 
  
  # Step 2. first deal with electricity
  all_emissions %>%
    select(scenario,region,direct,transformation,enduse,year,value,ghg,Units,elec_for_H2) %>%
    filter((direct != 'electricity') | (ghg != 'CO2'))  -> all_emiss_no_elec_CO2
  
  
  CO2_emiss_elec <- CO2_emiss %>%
    filter(direct == 'electricity') 
  
  # 2.1 -- disaggregate CO2 emission from electricity generation (transformation) based on fuel type
  # calculate CO2 emission share of different fuel type
  # calculate the CO2 emission share of fuel type (no CCS) -- It seem there are some CO2 emission from 
  # electricity sector (in the CO2 nobio query) that are from other techs other than coal, gas, and refined liquids (without CCS)
  # This is probably a very tiny amount of emission. For now, let's assign them to coal, gas, and refined liquids (with CCS)
  # in the fraction calculation. Later we will need to address this issue.
  
  # "elec_coal (conv pul CCS)", "elec_coal (IGCC CCS)", 
  # "elec_gas (CC CCS)", "elec_refined liquids (CC CCS)"
  elec_co2_fossil_frac <- 
    CO2_bio %>% 
    filter(sector %in% c("elec_coal (conv pul)", "elec_gas (CC)", 
                         "elec_gas (steam/CT)", "elec_refined liquids (steam/CT)")) %>%
    group_by(Units, scenario, region, sector, year) %>%
    summarise(value = sum(value, na.rm = TRUE)) %>% 
    group_by(Units, scenario, region, year) %>%
    mutate(frac = value/sum(value)) %>%
    select(-value)
  

  # calculate the CO2 emission share of all fuel type
  tmp_elec <- CO2 %>%
    filter(sector == "electricity") %>% 
    left_join(elec_co2_fossil_frac, 
              by = c("Units", "scenario", "region", "year")) %>%
    mutate(value = value * frac) %>%
    select(Units, scenario, region, sector = sector.y, year, value) %>% 
    na.omit() %>%
    rbind(CO2 %>%
            filter(sector %in% sectors_elec$sector, 
                   sector != "electricity")) %>%
    group_by(region,scenario,year) %>%
    mutate(normfrac = value/sum(value),
           transformation = "electricity") %>%
    ungroup() %>%
    select(-value,-Units)
  
  #   
  # tmp_elec %>%
  #   left_join(elec_gen_fuels, by = c('sector')) %>%
  #   mutate(fuel = if_else(fuel == 'backup_electricity','natural gas',fuel)) %>%
  #   group_by(scenario,region,year,fuel) %>%
  #   summarize(normfrac = sum(normfrac)) %>%
  #   ungroup() -> temp_elec_fuels
  
  elec_CO2_no_bio_final <- 
    CO2_emiss_elec %>% 
    left_join(tmp_elec, 
              by = c('scenario','region','year', "transformation")) %>%
    mutate(value = value * normfrac) %>%
    left_join(elec_gen_fuels, 
              by = c('sector')) %>%
    #filter(year >= 2005) %>%
    group_by(scenario,region,fuel,direct,transformation,enduse,year) %>%
    summarise(value = sum(value)) %>%
    ungroup()  %>%
    arrange(year) %>%
    mutate(direct = fuel,
           transformation = if_else(fuel == 'backup_electricity',fuel,transformation),
           # assign backup electricity to gas since natural gas open cycle is the only tech for this sector,
           # note that this is only applied for CO2 emission, in current cwf branch, we also have H2 backup
           # electricity, which does not emit CO2, but emit N2O, need to be careful when deal with non CO2
           direct = if_else(fuel == 'backup_electricity','natural gas',direct),
           ghg = 'CO2',
           Units = 'MTCO2e') %>%
    select(-fuel)
  print("Allocated electricity emissions by fuel")
  
  # 2.2 -- get electricity emissions intensity per unit generated as it is a fuel for certain other transform sectors (e.g., H2 production)
  elec_outputs <- 
    output_by_tech %>%
    group_by(scenario,region,year,sector,subsector,output,Units) %>%
    summarize(value = sum(value)) %>%
    ungroup() %>%
    filter(sector == 'electricity') %>%
    group_by(scenario,region,year,Units) %>%
    summarize(value = sum(value)) %>%
    ungroup() %>%
    rename(elec_output = value)
  
  tot_elec_emissions <- 
    elec_CO2_no_bio_final %>%
    group_by(scenario,region,year,ghg,Units) %>%
    summarize(value = sum(value)) %>%
    ungroup() %>%
    rename(elec_emiss = value)
  
  elec_emiss_intensity <- 
    tot_elec_emissions %>%
    left_join(elec_outputs %>% 
                select(-Units),
              by = c('scenario','region','year')) %>%
    mutate(value = elec_emiss / elec_output,
           Units = 'MtCO2-per-EJ') %>%
    select(scenario,region,year,ghg,Units,value)
  
  # check electricity emission remain the same
  
  if (round(sum(elec_CO2_no_bio_final$value) - sum(CO2_emiss_elec$value), 0) != 0){
    print("Electricity CO2 emission disaggregation: Total emissions from 1990 to 2100 do NOT match.")
    print("percent difference between raw GCAM output data and initial disaggregation pass emissions is:")
    print(100*(sum(CO2_emiss_elec$value - sum(elec_CO2_no_bio_final$value))/sum(CO2_emiss_elec$value)))
    }
  
  # Step 3. disagregate transportation tailpipe emissions, the tailpipe emission could come from gas or refined liquids
  
  # refined_liquids_subsector <- 
  #   inputs_by_subsector %>% 
  #   # get all sectors using refined liquids (incl. non transportation, but does not inclue power sector)
  #   filter(input %in% c('refined liquids industrial','refined liquids enduse'),
  #          !(sector %in% c('elec_refined liquids (CC)','elec_refined liquids (CC CCS)','elec_refined liquids (steam/CT)'))) %>%
  #   mutate(subsector = if_else(subsector == 'refined liquids', sector, subsector)) %>%
  #   distinct(subsector)
  
  CO2_emiss_no_elec <- CO2_emiss %>%
    filter(direct != 'electricity') 
  
  CO2_emiss_no_elec %>%
    filter(((enduse %in% transport_sectors & direct == 'refining') | direct %in% transport_sectors)) %>%
    select(-transformation) -> trn_CO2
  
  ## calculate the emission fraction of different fuel type (i.e., natural gas + refined liquids) by mode, based on fuel consumption 
  #  and fuel emission factor. 
  trn_inputs_by_subsector <- 
    inputs_by_subsector %>% 
    filter(subsector %in% transport_sectors,
           input %in% c('refined liquids enduse','delivered gas','refined liquids industrial')) %>%
    rename(enduse = subsector,
           PrimaryFuelCO2Coef.name = input) %>%
    left_join(ccoef_mapping, by = c('PrimaryFuelCO2Coef.name')) %>%
    mutate(transformation = if_else((PrimaryFuelCO2Coef.name == 'refined liquids enduse' | PrimaryFuelCO2Coef.name == 'refined liquids industrial'),'refining',
                                    if_else(PrimaryFuelCO2Coef.name == 'delivered gas','gas processing',NA_character_)),
           tailpipe_emiss = value * PrimaryFuelCO2Coef) %>%
    group_by(scenario,region,enduse,year) %>%
    mutate(frac_tailpipe_emiss = tailpipe_emiss / sum(tailpipe_emiss),
           frac_tailpipe_emiss = if_else(tailpipe_emiss ==0 & sum(tailpipe_emiss) == 0,
                                         0, frac_tailpipe_emiss)) %>%
    ungroup() %>%
    select(-sector,-PrimaryFuelCO2Coef.name,-value,-PrimaryFuelCO2Coef,-fuel,-tailpipe_emiss,-Units)
  
  # calculate the CO2 emission of fuel type by mode, multiplying emission by mode by the fuel type fraction (the output data
  # have and combines both emissions from fuel refining process and the fuel combustion at the vehicle)
  trn_tailpipe_CO2_for_disag <-  
    trn_inputs_by_subsector %>%
    left_join(trn_CO2,by = c('scenario','region','enduse','year')) %>%
    mutate(frac_tailpipe_emiss = if_else(is.na(frac_tailpipe_emiss) & value == 0,
                                         0, frac_tailpipe_emiss),
           value = value * frac_tailpipe_emiss,
           fuel = if_else(transformation == 'refining','refining',
                          if_else(transformation == 'gas processing','natural gas',NA_character_)),
           direct = fuel) %>%
    select(-frac_tailpipe_emiss,-fuel) %>%
    filter(year >= 2005) %>%
    group_by(scenario,region,direct,transformation,enduse,year,ghg,Units) %>%
    summarize(value = sum(value)) %>%
    ungroup() 
  
  # 
  # trn_tailpipe_CO2_for_disag %>%
  #   distinct(enduse) -> trn_sectors
  
  # This step is to disaggregate the refining emission by fuel type. The refining process takes biomass and fossil fuel, so the emission
  # could be both negative or positive. Here we first calculate the CO2 sequestration by biomass and other fuel. 
  refining_c_csq <- CO2_sequestration_by_tech %>%
    filter(sector == 'refining') %>%
    mutate(fuel = if_else(subsector == 'biomass liquids','biomass',
                          if_else(subsector == 'coal to liquids','coal',NA_character_))) %>%
    group_by(scenario,region,fuel,year) %>%
    summarize(sum_seq = sum(value)) %>%
    ungroup()
  
  # we calculate the total carbon input for each fuel that goes into the refining, and then subtract the carbon sequestration calculated in 
  # the previous step, then we get the CO2 output from the refining by fuel. 
  # we assume C input of biomass to be 0. We will use C output of the refining process to calculate fuel fraction in the next step. 
  refining_emiss_by_fuel_no_bio <- 
    inputs_by_subsector %>%  
    filter((sector == 'refining') & !(input %in% c('elect_td_ind','H2 industrial')) & subsector != "dac to liquids") %>%
    rename(PrimaryFuelCO2Coef.name = input) %>%
    left_join(ccoef_mapping, by = c('PrimaryFuelCO2Coef.name')) %>%
    mutate(c_input = value * PrimaryFuelCO2Coef) %>%
    group_by(scenario,region,year,fuel) %>%
    summarize(c_input = sum(c_input)) %>%
    ungroup() %>%
    left_join(refining_c_csq,by = c('scenario','region','year','fuel')) %>%
    mutate(c_input = if_else(fuel == 'biomass',0,c_input),
           sum_seq = if_else(is.na(sum_seq),0,sum_seq),
           emiss_no_bio = c_input - sum_seq) %>%
    na.omit() 
  
  
  # inputs_by_subsector %>%  
  #   filter((sector == 'refining') & (input == 'elect_td_ind')) %>%
  #   left_join(elec_emiss_intensity %>%
  #               select(-Units) %>%
  #               rename(emiss_intensity = value),by = c('scenario','region','year')) %>%
  #   mutate(value = value * emiss_intensity * 12 / 44, #convert to MtC
  #          Units = 'MTC') %>%
  #   select(-emiss_intensity)-> refining_elec_input
  # 
  # temp_elec_fuels %>%
  #   left_join(refining_elec_input,by = c('scenario','region','year')) %>%
  #   mutate(c_input = value * normfrac,
  #          sum_seq = 0,
  #          emiss_no_bio = c_input - sum_seq) %>%
  #   select(scenario,region,year,fuel,c_input,sum_seq,emiss_no_bio) -> refining_elec_input_joined #disaggregate electricity CO2 emissions for refining
  
  #refining_emiss_by_fuel_no_bio_bind <- bind_rows(refining_emiss_by_fuel_no_bio,refining_elec_input_joined) %>%
  refining_emiss_by_fuel_no_bio_norm <- 
    refining_emiss_by_fuel_no_bio %>%
    mutate(emiss_no_bio = if_else(is.na(emiss_no_bio),0,emiss_no_bio)) %>%
    group_by(scenario,region,year,fuel) %>%
    summarize(emiss_no_bio = sum(emiss_no_bio)) %>%
    ungroup() %>%
    group_by(scenario,region,year) %>%
    mutate(emiss_no_bio = if_else(is.na(emiss_no_bio),0,emiss_no_bio),
           normfrac = emiss_no_bio/sum(emiss_no_bio),
           normfrac = if_else(is.na(normfrac),0,normfrac),
           transformation = 'refining',
           ghg = 'CO2') %>%
    select(-emiss_no_bio)
  
  #write_csv(refining_emiss_by_fuel_no_bio_norm,'refining_emiss_by_fuel_no_bio_norm.csv')
  
  # Apply the fraction to the tailpipe CO2 data, now the trn emission is disaggregated to fuel level. But this is not disaggregated by 
  # life cycle stage (e.g., midstream vs end use), this will be done later. 
  trn_tailpipe_CO2_disag <- 
    refining_emiss_by_fuel_no_bio_norm %>%
    left_join(trn_tailpipe_CO2_for_disag %>% 
                filter(transformation == 'refining'),
              by = c('scenario','region','year','transformation','ghg')) %>%
    mutate(direct = fuel,
           value = value * normfrac) %>%
    filter(year >= 2005) %>%
    select(-fuel,-normfrac) %>%
    bind_rows(trn_tailpipe_CO2_for_disag %>% 
                filter(transformation == 'gas processing'))
  
  #write_csv(trn_tailpipe_CO2_disag,'trn_tailpipe_CO2_disag.csv')
  # check trn tailpipe emission remain the same
  if (round(sum(trn_CO2$value) - sum(trn_tailpipe_CO2_disag$value), 0) != 0){
    print("Electricity CO2 emission disaggregation: Total emissions from 1990 to 2100 do NOT match.")
    print("percent difference between raw GCAM output data and initial disaggregation pass emissions is:")
    print(100*(sum(trn_CO2$value - sum(trn_tailpipe_CO2_disag$value))/sum(trn_CO2$value)))
  }
  
  
  ## Step 4 -- Now disaggregate hydrogen no bio emissions
  
  CO2_emiss_no_elec_trn <- CO2_emiss_no_elec %>%
    filter(!((enduse %in% transport_sectors & direct == 'refining') | direct %in% transport_sectors)) 
  
  H2_CO2_emiss <- 
    CO2_emiss_no_elec_trn %>%
    filter(direct %in% c('H2 enduse',
                         "H2 industrial", 
                         "H2 retail delivery", 
                         "H2 retail dispensing",
                         "H2 wholesale delivery",
                         "H2 wholesale dispensing",
                         "H2 central production"))
  
  all_emiss_no_elec_trn_H2 <- 
    CO2_emiss_no_elec_trn %>%
    filter(!direct %in% c('H2 enduse',
                          "H2 industrial", 
                          "H2 retail delivery", 
                          "H2 retail dispensing",
                          "H2 wholesale delivery",
                          "H2 wholesale dispensing",
                          "H2 central production"))
  
  # calculate the c input to the H2 production
  H2_inputs_no_elec <- 
    inputs_by_subsector %>%
    filter(sector %in% c('H2 central production','H2 industrial','H2 wholesale dispensing','H2 forecourt production')) %>%
    rename(PrimaryFuelCO2Coef.name = input) %>%
    left_join(ccoef_mapping,by = c('PrimaryFuelCO2Coef.name')) %>%
    mutate(PrimaryFuelCO2Coef = if_else(is.na(PrimaryFuelCO2Coef) | fuel == 'biomass',0,PrimaryFuelCO2Coef),
           fuel = if_else(is.na(fuel),PrimaryFuelCO2Coef.name,fuel),
           c_input = value * PrimaryFuelCO2Coef) %>%
    group_by(scenario,region,year,fuel) %>%
    summarize(c_input = sum(c_input)) %>%
    ungroup() %>%
    filter(!(fuel %in% c('elect_td_ind','elect_td_trn')))
  
  # inputs_by_subsector %>%
  #   filter(sector %in% c('H2 central production','H2 wholesale dispensing','H2 forecourt production'),
  #          input %in% c('elect_td_ind','elect_td_trn')) %>%
  #   group_by(scenario,region,year,Units) %>%
  #   summarize(value = sum(value)) %>%
  #   ungroup() %>%
  #   left_join(elec_emiss_intensity %>% 
  #               select(-Units) %>%
  #               rename(emiss_intensity = value),by = c('scenario','region','year')) %>%
  #   mutate(c_input = value * emiss_intensity * 12 /44, #need to convert back to MtC from MtCO2e
  #          fuel = 'electricity') %>%
  #   select(-emiss_intensity,-value) -> H2_grid_elec_inputs 
  
  # H2_inputs <- bind_rows(H2_inputs_no_elec)
  
  # calculate the c sequestration 
  H2_sequestration<- 
    CO2_sequestration_by_tech %>%
    filter(sector %in% c('H2 central production','H2 wholesale dispensing','H2 forecourt production')) %>%
    mutate(fuel = if_else(subsector == 'gas','natural gas',subsector)) %>%
    rename(c_seq = value) 
  
  # H2_sequestration %>%
  #   distinct(scenario,region,year) %>%
  #   mutate(fuel = 'electricity',
  #          c_seq = 0) -> H2_elec_seq
  # 
  # H2_sequestration <- bind_rows(H2_sequestration) #,H2_elec_seq)

  # Double check the code
  
  # calculate the c output fraction 
  H2_inputs_joined <- 
    H2_inputs_no_elec %>%
    left_join(H2_sequestration %>%
                select(-sector,-subsector,-technology), 
              by = c('scenario','region','year','fuel')) %>%
    mutate(c_seq = if_else(is.na(c_seq),0,c_seq),
           emiss_no_bio = c_input - c_seq,
           Units = 'MTC') %>% #mutate to H2 production (the actual transformation) occurs at the end
    group_by(scenario,region,year) %>%
    mutate(normfrac = emiss_no_bio / sum(emiss_no_bio)) %>%
    select(-Units, -c_input, -c_seq)
  
  H2_CO2_emiss_disag <- 
    H2_inputs_joined %>%
    left_join(H2_CO2_emiss, 
              by = c('scenario','region','year')) %>%
    mutate(direct = fuel,
           value_cal = value * normfrac,
           value_cal = if_else(is.na(value_cal) & is.na(normfrac) & emiss_no_bio  == 0, 0, value_cal)) %>%
    filter(!(value_cal == 0 & is.na(ghg))) %>%
    select(-emiss_no_bio,-fuel,-normfrac, -value) %>%
    rename(value = value_cal) %>%
    filter(direct %in% c('electricity','biomass','coal','natural gas','crude oil')) %>%
    mutate(transformation = if_else(direct == 'electricity','H2 grid electrolysis',transformation)) %>%
    filter(direct != 'electricity')
  
  # check h2 emission remain the same
  if (round(sum(H2_CO2_emiss$value) - sum(H2_CO2_emiss_disag$value), 0) != 0){
    print("Electricity CO2 emission disaggregation: Total emissions from 1990 to 2100 do NOT match.")
    print("percent difference between raw GCAM output data and initial disaggregation pass emissions is:")
    print(100*(sum(H2_CO2_emiss$value - sum(H2_CO2_emiss_disag$value))/sum(H2_CO2_emiss$value)))
  }
  

  # H2_CO2_emiss_elec <- 
  #   H2_CO2_emiss_disag %>%
  #   filter(direct == 'electricity') 
  # 
  # temp_elec_fuels %>%
  #   left_join(H2_CO2_emiss_elec,by = c('scenario','region','year')) %>%
  #   filter(!is.na(Units)) %>%
  #   mutate(value = normfrac * value) %>%
  #   select(-direct,-normfrac) %>%
  #   rename(direct = fuel) -> H2_elec_CO2_disag


  # Step 5 -- deal with all remaining CO2 emissions
  # break out H2 and refining emissions to be dealt with downstream
  
  c_containing <- c('biomass','coal','natural gas','cement limestone','crude oil','airCO2')
  
  remaining_industry_CO2 <- 
    all_emiss_no_elec_trn_H2 %>%
    filter(!(direct %in% c_containing))
  
  all_emiss_all_remaining <- 
    all_emiss_no_elec_trn_H2 %>%
    filter(!(!(direct %in% c_containing)))
  

  
  # 5.1 disaggregate CO2 emission of iron and steel by fuel type, because iron and steel intakes multiple carbon-containing fuels and sequesters 
  # a blend of their associated emissions we need to deal with it separately.
  point_source_iron_steel <- 
    remaining_industry_CO2 %>%
    filter(direct == transformation & transformation == enduse,
           direct == 'iron and steel')
  
  iron_steel_capture_coef <- 0.9 # here we assume an equal share of all fuel carbon is sequestered for iron and steel
  
  iron_steel_emiss_disag <- 
    rgcam::getQuery(prj,'inputs by tech') %>%
    filter(sector == 'iron and steel',
           !(input %in% c('elect_td_ind','H2 industrial','H2 enduse','scrap'))) %>%
    rename(PrimaryFuelCO2Coef.name = input) %>%
    left_join(ccoef_mapping,by = c('PrimaryFuelCO2Coef.name')) %>%
    mutate(c_emiss = PrimaryFuelCO2Coef * value,
           c_emiss = if_else(fuel == 'biomass',0,c_emiss),
           c_seq = if_else(technology %in% c('BLASTFUR CCS','EAF with DRI CCS','EAF with SCRAP CCS'),c_emiss * iron_steel_capture_coef,0),
           c_emiss = c_emiss - c_seq) %>%
    group_by(scenario,region,year) %>%
    mutate(normfrac = c_emiss / sum(c_emiss)) %>%
    ungroup() %>%
    group_by(scenario,region,year,fuel) %>%
    summarize(normfrac = sum(normfrac)) %>%
    ungroup() %>%
    rename(direct = fuel) %>%
    left_join(point_source_iron_steel %>% select(-direct),by = c('scenario','region','year')) %>%
    mutate(value = value * normfrac,
           transformation = 'iron and steel',
           transformation = if_else(direct == 'refining','refining',transformation)) %>%
    select(-normfrac) %>%
    filter(year >= 2005)
  
  remaining_industry_CO2_net_iron <- 
    remaining_industry_CO2 %>% 
    anti_join(point_source_iron_steel)
  
  # sum(point_source_iron_steel$value)
  # sum(iron_steel_emiss_disag$value)
  

  # 5.2 Deal with feedstocks separately because they contain multiple carbon containing fuels with different sequestration factors which causes issues with taking ratios

  feedstock_CO2 <- 
    remaining_industry_CO2_net_iron %>%
    filter(direct %in% c('construction feedstocks','chemical feedstocks','industrial feedstocks', 
                         "other industrial feedstocks"))
  
  #we therefore account for all uncaptured (positive) emissions from coal and gas, and filter out refined liquids.  
  feedstock_emiss <-  
    rgcam::getQuery(prj, "inputs by tech") %>%
    filter(sector %in% c('construction feedstocks','chemical feedstocks','industrial feedstocks', 
                         "other industrial feedstocks")) %>%
    rename(PrimaryFuelCO2Coef.name = input) %>%
    left_join(ccoef_mapping,
              by = c('PrimaryFuelCO2Coef.name')) %>%
    filter(PrimaryFuelCO2Coef.name != 'oil-credits') %>%
    mutate(remove.fraction = if_else(technology == 'refined liquids',1,0),
           value = value * PrimaryFuelCO2Coef * (1-remove.fraction)) %>%
    rename(enduse = sector,
           c_emiss = value) %>% 
    filter(year >= 2005)
  
  industrial_cseq_by_fuel <- 
    CO2_sequestration_by_tech %>%
    left_join(ccoef_mapping %>% 
                rename(technology = PrimaryFuelCO2Coef.name), 
              by = c('technology')) %>%
    select(-PrimaryFuelCO2Coef) %>%
    rename(enduse = sector,
           c_seq = value) %>%
    mutate(fuel = ifelse(subsector %in% c("biochar", "dac", "rock weathering"), subsector, fuel),
           fuel = ifelse(subsector == "direct ocean capture", "dac", fuel)) %>%
    filter(year >= 2005)
  
  feedstock_net_CO2_with_cseq <- 
    feedstock_emiss %>%
    left_join(industrial_cseq_by_fuel %>% 
                select(-subsector,-Units), 
              by = c('scenario','region','year','enduse','technology')) %>%
    mutate(c_seq = if_else(is.na(c_seq),0,c_seq),
           c_emiss = (c_emiss - c_seq) * 44 / 12) %>%
    rename(direct = fuel.x,
           value = c_emiss) %>%
    filter(direct != 'refining') %>% 
    group_by(scenario,region,year,enduse) %>%
    mutate(pos_emiss = sum(value),
           transformation = enduse,
           ghg = 'CO2',
           Units = 'MTCO2e') %>%
    filter(year >= 2005) %>%
    select(scenario,region,year,enduse,direct,value,pos_emiss,transformation,ghg,Units)
  

  
  # Finally, we use simple addition and subtraction to create an error term between positive gas + coal emissions and the original aggregated result 
  # which based on the CO2 no bio query.
  # any difference between the two will be attributable to biomass carbon embedded in refined liquids input which is then embedded in the feedstocks
  # this is done to avoid offsetting but extremely large gross positive and negative emissions and negative emissions in years when overall sectoral emissions approach zero 
  # when the full refined liquids carbon coefficient is used (not accounting for the generally much smaller fraction of biomass embedded carbon)
  
  feedstock_emiss_corrected <- 
    feedstock_net_CO2_with_cseq %>% 
    group_by(scenario,region,year,enduse) %>%
    summarise(pos_emiss = sum(value, na.rm = TRUE)) %>%
    # distinct(scenario,region,year,enduse,pos_emiss) %>%
    # select(scenario,region,year,enduse,pos_emiss) %>%
    right_join(feedstock_CO2,
               by = c('scenario','region','year','enduse')) %>%
    mutate(pos_emiss = if_else(is.na(pos_emiss),0,pos_emiss),
           value = value - pos_emiss,
           direct = 'Non-energy') %>%
    select(-pos_emiss)
  
  feedstock_CO2_disag <- 
    feedstock_net_CO2_with_cseq %>%
    select(-pos_emiss) %>%
    bind_rows(feedstock_emiss_corrected)
  
  remaining_industry_CO2_net_iron_feedstock <- 
    remaining_industry_CO2_net_iron %>% 
    anti_join(feedstock_CO2)
  
  # sum(feedstock_CO2$value)
  # sum(feedstock_CO2_disag$value)
  
  # 
  # # check feedstock emission remain the same
  # if (round(sum(feedstock_CO2$value) - sum(feedstock_CO2_disag$value), 0) != 0){
  #   print("Electricity CO2 emission disaggregation: Total emissions from 1990 to 2100 do NOT match.")
  #   print("percent difference between raw GCAM output data and initial disaggregation pass emissions is:")
  #   print(100*(sum(feedstock_CO2$value - sum(feedstock_CO2_disag$value))/sum(feedstock_CO2$value)))
  # }
  
  # 5.3 Deal with district heat 
  
  district_heating_norm <- 
    inputs_by_subsector %>%
    filter(sector == 'district heat') %>%
    rename(PrimaryFuelCO2Coef.name = input,
           transformation = sector) %>%
    left_join(ccoef_mapping,
              by = c('PrimaryFuelCO2Coef.name')) %>%
    mutate(fuel = if_else(PrimaryFuelCO2Coef.name == 'refined liquids industrial','refining',fuel),
           PrimaryFuelCO2Coef = if_else(fuel == 'biomass',0,PrimaryFuelCO2Coef),
           value = value * PrimaryFuelCO2Coef,
           Units = 'MTC') %>%
    group_by(scenario,region,year,transformation) %>%
    mutate(emiss_frac = value/sum(value)) %>%
    ungroup() %>%
    select(-value,-Units,-subsector,-PrimaryFuelCO2Coef.name,-PrimaryFuelCO2Coef)
  
  district_heating_for_disag <- 
    remaining_industry_CO2_net_iron_feedstock %>%
    filter(transformation == 'district heat')
  
  district_heat_disag <- 
    district_heating_norm %>%
    left_join(district_heating_for_disag %>% select(-direct), 
              by = c('scenario','region','transformation','year')) %>%
    rename(direct = fuel) %>%
    mutate(value = value * emiss_frac) %>%
    filter(year >= 2005)
  
  remaining_industry_CO2_net_iron_feedstock_distheat <- 
    remaining_industry_CO2_net_iron_feedstock %>% 
    anti_join(district_heating_for_disag)
    
  # sum(district_heating_for_disag$value)
  # sum(district_heat_disag$value)
  
  # ind_inputs_by_subsector_temp <- 
  #   inputs_by_subsector %>%
  #   filter(sector %in% remaining_industry_CO2$enduse,
  #          input %in% c_containing_ind_fuels) %>%
  #   mutate(input = if_else(input == 'unconventional oil','crude oil',input),
  #          input = if_else(input == 'traditional biomass','biomass',input)) %>%
  #   rename(PrimaryFuelCO2Coef.name = input)
  
  
  ## Filter to get fuels with tailpipe/smokestack emissions (i.e., natural gas + refined liquids)
  
  # 5.4 deal with point source industrial CO2
  
  # first break out tailpipe/smokestack emissions (i.e,. where direct == transformation == enduse)
  point_source_industrial_CO2 <- 
    remaining_industry_CO2_net_iron_feedstock_distheat %>%
    filter(direct == transformation & transformation == enduse)
  
  # 5.4.1 get the unconventional oil and cdr tech 
  unconventional_oil_cdr <- 
    point_source_industrial_CO2 %>%
    filter(enduse %in% c('unconventional oil production', "ces", "direct air capture", "rock weathering", "biochar"))
  
  point_source_industrial_CO2_net_oil_cdr <- 
    point_source_industrial_CO2 %>% 
    anti_join(unconventional_oil_cdr)
  
  # 5.4.2 deal with gas processing 
  
  c_containing_ind_fuels <- c('delivered biomass', 'delivered coal', 'delivered gas',
                              'regional natural gas', 'unconventional oil', 'wholesale gas', 'traditional biomass',
                              'refined liquids industrial', 'refined liquids enduse', 'limestone', 'district heat',
                              'process heat cement', 'process heat dac', 'airCO2')
  
  transform_ind <- c('district heat','process heat dac')
  
  #correct for biomass gasification, which would slightly lower the emissions intensity of gas
  gas_c_inputs <- 
    rgcam::getQuery(prj, "inputs by tech") %>%
    filter(sector %in% c('gas processing'),
           input != 'elect_td_ind') %>% 
    rename(PrimaryFuelCO2Coef.name = input) %>%
    left_join(ccoef_mapping,by = c('PrimaryFuelCO2Coef.name')) %>%
    filter(PrimaryFuelCO2Coef.name != 'oil-credits') %>%
    mutate(PrimaryFuelCO2Coef = if_else(fuel %in% c('biomass'), 0, PrimaryFuelCO2Coef),
           value = value * PrimaryFuelCO2Coef,
           Units = 'MTC') %>%
    group_by(scenario,region,year,sector) %>%
    summarize(value = sum(value)) %>%
    ungroup()
  
  gas_output <- 
    rgcam::getQuery(prj, "outputs by tech") %>%
    filter(sector %in% c('gas processing')) %>%
    group_by(scenario,region,year,sector) %>%
    summarize(value = sum(value)) %>%
    ungroup()
  
  gas_c_emiss_intensity <- 
    gas_c_inputs %>%
    left_join(gas_output,by = c('scenario','region','year','sector')) %>%
    mutate(PrimaryFuelCO2Coef = value.x / value.y,
           fuel = 'natural gas') %>%
    select(scenario,region,year,PrimaryFuelCO2Coef,fuel)
  
  point_source_ind_en_inputs <- 
    rgcam::getQuery(prj, "inputs by tech") %>%
    filter(sector %in% c(unique(point_source_industrial_CO2_net_oil_cdr$enduse),'process heat cement'),
           !(sector %in% c(transform_ind)),
           !(input %in% c(transform_ind)),
           input %in% c_containing_ind_fuels) %>%
    mutate(input = if_else(input == 'unconventional oil','crude oil',input),
           input = if_else(input == 'traditional biomass','biomass',input),
           sector = if_else(sector == 'process heat cement','cement',sector))
  
  point_source_ind_C_input <- 
    point_source_ind_en_inputs %>%
    rename(PrimaryFuelCO2Coef.name = input) %>%
    left_join(ccoef_mapping,by = c('PrimaryFuelCO2Coef.name')) %>%
    filter(PrimaryFuelCO2Coef.name != 'oil-credits') %>%
    mutate(PrimaryFuelCO2Coef = if_else(fuel %in% c('biomass'), 0, PrimaryFuelCO2Coef),
           value = value * PrimaryFuelCO2Coef,
           Units = 'MTC')
  
  point_source_ind_C_input_no_gas <- 
    point_source_ind_C_input %>%
    filter(fuel != 'natural gas')
  
  point_source_ind_C_input_corrected_gas <- 
    point_source_ind_C_input %>%
    select(-PrimaryFuelCO2Coef) %>%
    filter(fuel == 'natural gas') %>%
    left_join(gas_c_emiss_intensity,by = c('scenario','region','year','fuel')) %>%
    mutate(value = value * PrimaryFuelCO2Coef / 14.2)
  
  
  point_source_ind_C_input <- bind_rows(point_source_ind_C_input_corrected_gas,
                                        point_source_ind_C_input_no_gas)
  
  point_source_ind_CO2_emiss <- 
    point_source_ind_C_input %>%
    rename(enduse = sector) %>%
    filter(year >= 2005) %>%
    left_join(industrial_cseq_by_fuel, by = c('scenario','region','year','enduse','subsector','technology','fuel','Units')) %>%
    mutate(c_seq = if_else(is.na(c_seq),0,c_seq),
           value = (value - c_seq) * 44/12,
           transformation = enduse,
           Units = 'MTCO2e',
           ghg = 'CO2',
           fuel = if_else(fuel == 'biomass','biomass CCS',fuel),
           fuel = if_else(subsector == "direct ocean capture", "dac", fuel)) %>%
    filter(value != 0,
           fuel != 'refining', #filter out for now as we will assume the difference between other sectoral emissions and point source CO2 from tracer will be refining, crude oil if positive, biomass CCS if negative
           enduse != 'unconventional oil production',
           fuel != 'dac') %>%
    rename(direct = fuel) %>%
    select(scenario,region,year,direct,transformation,enduse,value,ghg,Units) 
  
 
  ## calculate the sum of emission from all the fuels except for refining.
  point_source_ind_CO2_no_refining <- 
    point_source_ind_CO2_emiss %>%
    group_by(scenario,region,year,enduse) %>%
    summarize(tot_emiss_no_refining = sum(value)) %>%
    ungroup()
  
  ## subtract emission of non-refining and assign them to be refining.
  point_source_industrial_CO2_refining_no_bio <- 
    point_source_industrial_CO2_net_oil_cdr %>%
    left_join(point_source_ind_CO2_no_refining, by = c('scenario','region','year','enduse')) %>%
    # To calculate point-source emissions associated with refined liquids use in industry, subtract emissions from all other non-refining sources 
    # from the total point source emissions of the sector (point_source_industrial_CO2).  Any net-negative negative values will be assumed to be from carbon of biomass origin 
    # in the refined products to which CCS is applied.  Conversely, any positive values are assigned to crude oil.  
    # Note that due to the precision of the emissions no bio query, this is an approximation and may result in reporting a small amount of BECCS in historical years
    mutate(tot_emiss_no_refining = if_else(is.na(tot_emiss_no_refining),0,tot_emiss_no_refining),
           value = value - tot_emiss_no_refining,
           transformation = 'refining',
           direct = if_else(value > 0, 'crude oil','biomass CCS')) %>%
    select(-tot_emiss_no_refining)
  
  # 5.5. Filter to get emissions from gas processing and refining (refining -- for non transport sector)
  gas_processing_refining_non_transport <- 
    remaining_industry_CO2_net_iron_feedstock_distheat %>% 
    anti_join(point_source_industrial_CO2)
  
  # ind_inputs_transform_for_disag <- ind_inputs_by_subsector_temp %>%
  #   filter(PrimaryFuelCO2Coef.name %in% transform_ind)
  
  remaining_industry_disag <- 
    bind_rows(iron_steel_emiss_disag,
              feedstock_CO2_disag,
              district_heat_disag,
              unconventional_oil_cdr,
              point_source_ind_CO2_emiss,
              point_source_industrial_CO2_refining_no_bio,
              gas_processing_refining_non_transport) %>%
    mutate(ghg = 'CO2',
           Units = 'MTCO2e',
           direct = if_else(direct == 'ces','CO2 removal',direct),
           direct = if_else(enduse == 'unconventional oil production','natural gas',direct),
           direct = if_else(direct == 'refining','crude oil',direct))
  
  remaining_industry_CO2
  
  # check h2 emission remain the same
  if (round(sum(remaining_industry_CO2$value) - sum(remaining_industry_disag$value), 0) != 0){
    print("Electricity CO2 emission disaggregation: Total emissions from 1990 to 2100 do NOT match.")
    print("percent difference between raw GCAM output data and initial disaggregation pass emissions is:")
    print(100*(sum(remaining_industry_CO2$value - sum(remaining_industry_disag$value))/sum(remaining_industry_CO2$value)))
  }
  
  ## Final processing #
 
  #Fix cement emissions to separate out process heat (fossil fuel) from limestone-related emissions  
  df <- bind_rows(elec_CO2_no_bio_final,
                  trn_tailpipe_CO2_disag,
                  H2_CO2_emiss_disag,
                  remaining_industry_disag,
                  all_emiss_all_remaining,
                  nonCO2_emiss) %>%
    mutate(transformation = if_else(direct == 'limestone','calcination',transformation)) %>%
    mutate(direct = if_else((direct == 'gas processing') & (ghg == 'CO2'),'natural gas',direct), #,#assign all direct gas processing CO2 emissions to natural gas since we're using emissions no bio query and bio constitutes a very small fraction of gas processing anyway
           direct = if_else(direct %in% c('H2 enduse'),'H2 production',direct),
           transformation = if_else(transformation %in% c('H2 enduse'),'H2 production',transformation)) %>%
    group_by(scenario,region,direct,transformation,enduse,year,ghg,Units,elec_for_H2) %>%
    #mutate(elec_for_H2 = if_else(is.na(elec_for_H2),FALSE,TRUE)) %>%
    # Modify this code to make sure elec_for_H2 is correctly modified
    mutate(elec_for_H2 = if_else(is.na(elec_for_H2),FALSE,elec_for_H2)) %>%
    summarize(value = sum(value)) %>%
    ungroup() #%>% 
  
  return(df)
}


final_fuel_nonCO2_disag <- function(all_emissions_after_co2_disag) {
  
  transport <- read_csv('input/transport.csv')
  nonCO2_emissions_by_tech <- rgcam::getQuery(prj,'nonCO2 emissions by tech (excluding resource production)')
  
  # all_emissions_after_co2_disag <- all_emissions2
  
  # assign phase = enduse for those direct = transformation and transformation = enduse
  all_emissions_after_co2_disag_raw <- 
    all_emissions_after_co2_disag %>%
    mutate(phase = if_else(direct == transformation & transformation == enduse,'enduse',NA_character_))
  
  # Step 1 -- update non-CO2 emission for electricity sector, use non CO2 emission data to calculation fraction of emission by fuel, 
  # here we exclude SF6, which is a emission that generated in the transmission and distribution process, in regardless of fuel type. 
  elec_nonco2_frac <- 
    nonCO2_emissions_by_tech %>% 
    filter(grepl("electricity", sector), ghg != "SF6") %>%

    group_by(Units, scenario, region, subsector, ghg, year) %>% 
    summarise(value = sum(value, na.rm = TRUE)) %>%
    group_by(Units, scenario, region, ghg, year) %>%
    mutate(frac = value/sum(value),
           sector = "electricity") %>%
    ungroup() %>%
    select(-value, -Units)
  
  # apply the fraction to nonCO2 emission data of electricity -- assign the phase to be midstream, because it is during the electricigy generation process
  elec_nonco2_data <- 
    all_emissions_after_co2_disag_raw %>% 
    filter(direct == "electricity", ghg!= "SF6") %>%
    left_join(elec_nonco2_frac, 
              by = c("scenario", "region", "direct" = "sector", "ghg",  "year")) %>%
    mutate(value = value * frac) %>%
    select(scenario, region, direct = subsector, transformation, enduse, year, ghg, Units, elec_for_H2, value, phase) %>% 
    mutate(phase = "midstream")
    
  all_emissions_after_co2_disag_adj <- 
    elec_nonco2_data %>% 
    # this rbind will also include electricity generation CO2 results, the CO2 result is already disaggregated in the previous CO2 function.
    rbind(all_emissions_after_co2_disag_raw %>% 
            filter(direct != "electricity")) %>% 
    rbind(all_emissions_after_co2_disag_raw %>% 
            filter(direct == "electricity", ghg == "SF6") %>% 
            mutate(phase = "midstream")) 
  
  
  #write_csv(all_emissions,'preliminary_phase_definition.csv')
  
  ## Here we decompose the all_emissions_after_co2_disag_adj into two major components
  ## -- combustion_non_CO2_emiss
  ## -- all_other_emission
  
  combustion_non_CO2_emiss <- all_emissions_after_co2_disag_adj %>%
    filter(ghg %in% c('CH4','N2O') & direct == transformation & transformation == enduse)
  
  
  all_other_emiss <- all_emissions_after_co2_disag_adj %>%
    filter(!(ghg %in% c('CH4','N2O') & direct == transformation & transformation == enduse)) %>%
    mutate(phase = if_else(ghg != 'CO2' & direct %in% c('biomass','coal','crude oil','natural gas','unconventional oil') & is.na(phase),'resource production', phase),
           
           phase = if_else(ghg != 'CO2' & direct %in% c('refining','electricity','backup_electricity','H2 central production',
                                                                'H2 wholesale dispensing','H2 production') & is.na(phase),'midstream',phase))  #for mergeback
  
  # sum(combustion_non_CO2_emiss$value) +
  # sum(all_other_emiss$value)
  # sum(all_emissions_after_co2_disag_adj$value)
  
  #write_csv(all_other_emiss,'all_other_emiss_non_CO2.csv')
  
  ## We first deal with combustion_non_CO2_emiss
  
  nonCO2_combustion_emissions_by_tech <- 
    nonCO2_emissions_by_tech %>%
    filter(ghg %in% c('CH4','N2O')) %>%
    mutate(sector = if_else(subsector %in% transport$transportation_subsector,subsector,sector),
           fuel = if_else(technology %in% c('Liquids','NG','Coal','biomass'),technology,subsector),
           fuel = if_else(fuel %in% c('gas','NG'),'natural gas',fuel),
           fuel = if_else(fuel %in% c('Liquids'),'refined liquids',fuel),
           fuel = if_else(fuel %in% c('mobile','stationary'),technology,fuel),
           fuel = if_else(sector %in% c('iron and steel'),technology,fuel)) %>% 
    left_join(sector_label %>% select(-type), by = c("sector")) %>% 
    mutate(sector = if_else(sector == rewrite, sector, rewrite)) %>% 
    group_by(Units, scenario, region, sector, subsector, technology, ghg, year, fuel) %>% 
    summarise(value = sum(value, na.rm = TRUE)) %>%
    filter(!(sector %in% c('H2 retail dispensing',"H2 central production", 'H2 retail delivery',
                           'H2 retail dispensing','H2 wholesale dispensing','elect_td_H2','H2 enduse',
                           'electricity', "backup_electricity", 'refining','district heat'))) %>% #filter out transformation sector as these will be dealt with separately
    group_by(scenario,region,sector,ghg,year) %>%
    mutate(normfrac = value / sum(value)) %>%
    ungroup() %>%
    mutate(year = as.numeric(year),
           normfrac = if_else(is.na(normfrac),0,normfrac)) %>%
    rename(enduse = sector)
  
# unique(combustion_non_CO2_emiss_disag$enduse)
  
  nonCO2_combustion_emissions_by_tech %>%
    select(-Units,-technology,-subsector,-value) %>%
    left_join(combustion_non_CO2_emiss, by = c('scenario','region','year','enduse','ghg')) %>%
    filter(Units != is.na(Units)) %>%
    mutate(value = value * normfrac,
           direct = fuel) %>%
    select(-normfrac,-fuel) %>%
    mutate(phase = 'enduse') -> combustion_non_CO2_emiss_disag 

  iron_steel_combustion_nonCO2_for_disag <- combustion_non_CO2_emiss_disag %>%
    filter(enduse %in% c('iron and steel'))
  
  iron_steel_inputs <- rgcam::getQuery(prj,'inputs by tech') %>%
    filter(sector %in% c('iron and steel'),
           technology %in% unique(iron_steel_combustion_nonCO2_for_disag$direct),
           !input %in% c('elect_td_ind','scrap')) %>% # filter out inputs that aren't combusted
    mutate(input = if_else(input == 'delivered coal','coal',
                           if_else(input == 'wholesale gas','natural gas',
                                   if_else(input == 'refined liquids industrial','crude oil',input)))) %>%
    rename(direct = technology) %>%
    group_by(scenario,region,year,direct) %>%
    mutate(input_frac = value / sum(value)) %>%
    ungroup() %>%
    select(scenario,region,year,direct,input,input_frac) 
  
  
  iron_steel_combustion_nonCO2_disag <- iron_steel_inputs %>%
    left_join(iron_steel_combustion_nonCO2_for_disag,by = c('scenario','region','year','direct')) %>%
    filter(!is.na(value)) %>%
    mutate(value = value * input_frac) %>%
    select(-input_frac,-direct) %>%
    rename(direct = input) %>%
    group_by(scenario,region,year,direct,enduse,transformation,ghg,Units,phase) %>%
    summarize(value = sum(value)) %>%
    ungroup()
  
  all_other_combustion_nonCO2_disag <- combustion_non_CO2_emiss_disag %>%
    filter(!enduse %in% c('iron and steel'))
  
  ## Here we get the combustion_non_CO2_emiss_disag, which should be equal to combustion_non_CO2_emiss
  combustion_non_CO2_emiss_disag <- bind_rows(all_other_combustion_nonCO2_disag,
                                              iron_steel_combustion_nonCO2_disag)
  
  # sum(iron_steel_combustion_nonCO2_for_disag$value)
  
  
  ## Here, we deal with all_other_emission, we further decompose this dataset into two components
  ## -- other_emiss_transform_for_disag
  ## -- all_other_emiss_no_transform_combustion
  
  other_emiss_transform_for_disag <- all_other_emiss %>%
    filter(direct %in% c('H2 retail dispensing', "H2 central production", 'H2 retail delivery',
                         'H2 retail dispensing','H2 wholesale dispensing','elect_td_H2', 'H2 enduse',
                         'electricity', 'refining','district heat') & ghg %in% c('CH4','N2O')) %>%
    rename(sector = direct)
  
  all_other_emiss_no_transform_combustion <- all_other_emiss %>%
    filter(!(direct %in% c('H2 retail dispensing',"H2 central production", 'H2 retail delivery',
                           'H2 retail dispensing','H2 wholesale dispensing','elect_td_H2','H2 enduse',
                           'electricity','refining','district heat') & ghg %in% c('CH4','N2O')))
  
  #write_csv(all_other_emiss_no_transform_combustion,'all_other_emiss_no_transform_combustion.csv')
  
  
  
  nonCO2_emissions_by_tech_transform <- 
    nonCO2_emissions_by_tech %>%
    #    rename(ghg = GHG) %>%
    filter(ghg %in% c('CH4','N2O') & sector %in% c('H2 retail dispensing',"H2 central production", 'H2 retail delivery',
                                                   'H2 retail dispensing','H2 wholesale dispensing','elect_td_H2','H2 enduse',
                                                   'electricity',"backup_electricity", 'district heat','refining')) %>%
    mutate(sector = if_else(sector == 'backup_electricity','electricity', sector),
           fuel = if_else(subsector %in% c('biomass','biomass liquids'),'biomass',
                          if_else(subsector %in% c('coal','coal to liquids'),'coal',
                                  if_else(subsector %in% c('gas','gas to liquids'),'natural gas',subsector)))) %>%
    group_by(scenario,region,sector,year,fuel,ghg) %>%
    summarize(value = sum(value)) %>%
    ungroup() %>%
    group_by(scenario,region,sector,year,ghg) %>%
    mutate(normfrac = value / sum(value),
           year = as.numeric(year)) %>%
    select(-value) %>%
    ungroup()
  

  
   nonCO2_emissions_by_tech_transform %>%
     filter(sector != "electricity") %>%
    left_join(other_emiss_transform_for_disag,by = c('scenario','region','sector','year','ghg')) %>%
    filter(!is.na(normfrac)) %>%
    mutate(value = value * normfrac,
           direct = fuel) %>%
    select(scenario,region,year,direct,transformation,enduse,ghg,value,Units,phase) ->
    nonCO2_emissions_by_tech_transform_disag
  
  
  ## Combine the two datasets to be all_other_emiss_disag, this dataset should be equal to all_other_emission
   
  all_other_emiss_disag <- bind_rows(nonCO2_emissions_by_tech_transform_disag,
                                     all_other_emiss_no_transform_combustion)
  
  ## further combine all_other_emiss_disag and all_other_emiss_disg, to get the overall nonCO2 emission dataset.
  all_emiss_w_nonCO2_comb_disag <- bind_rows(combustion_non_CO2_emiss_disag,
                                             all_other_emiss_disag) 
  
  all_emiss_w_nonCO2_comb_disag  %>%
    select(scenario,region,direct,transformation,enduse,ghg,year,value,Units,phase,elec_for_H2) %>%
    group_by(scenario,region,direct,transformation,enduse,ghg,year,Units,phase,elec_for_H2) %>%
    summarize(value = sum(value)) %>% 
    ungroup() %>%
    arrange(year) %>%
    mutate(phase = if_else(ghg %in% c('CH4','N2O') & is.na(phase) & transformation == 'district heat' & direct %in% c('coal','refined liquids','crude oil','biomass','natural gas'),'midstream',phase)) %>%
    select(scenario,region,year,direct,transformation,enduse,ghg,value,Units,phase,elec_for_H2) %>%
    mutate(elec_for_H2 = if_else(is.na(elec_for_H2),FALSE, elec_for_H2)) -> all_emiss_w_nonCO2_comb_disag_distinct
  
  return(all_emiss_w_nonCO2_comb_disag_distinct)
}


lifecycle_CO2_emiss_phase_disag <- function(data_input){
  
  # data_input <- all_emissions2
  
  
  data_input %>%
    mutate(phase = if_else(!(transformation %in% c('refining','gas processing','electricity','backup_electricity')) & ghg == 'CO2','enduse', 
                           if_else((transformation %in% c('electricity','backup_electricity') & ghg == 'CO2'),'midstream',phase))) -> df_for_disag 
  #assign all CO2 emissions for fuels with zero tailpipe / smokestack emissions to midstream
  
  df_for_bindback <- df_for_disag %>%
    filter(!is.na(phase) | ghg != 'CO2')
  
  ccoef_mapping <- read_csv('input/ccoef_mapping.csv',show_col_types = FALSE)
  
  outputs_by_subsector <- rgcam::getQuery(prj, 'outputs by tech') %>%
    group_by(scenario,region,year,sector,subsector,output,Units) %>%
    summarize(value = sum(value)) %>%
    ungroup()
  inputs_by_subsector <- rgcam::getQuery(prj, 'inputs by tech') %>%
    group_by(scenario,region,year,sector,subsector,input,Units) %>%
    summarize(value = sum(value)) %>%
    ungroup()
  CO2_sequestration_by_tech <- rgcam::getQuery(prj, 'CO2 sequestration by tech')
  
  inputs_by_subsector %>%
    filter(sector == 'refining' | sector == 'gas processing') %>%
    rename(PrimaryFuelCO2Coef.name = input) %>%
    left_join(ccoef_mapping,by = c('PrimaryFuelCO2Coef.name')) %>%
    mutate(c_input = value * PrimaryFuelCO2Coef) %>%
    select(-Units) %>%
    filter(!(PrimaryFuelCO2Coef.name %in% c('elect_td_ind','H2 industrial', "global solar resource", "onshore wind resource"))) -> upstream_inputs_by_subsector
  
  
  CO2_sequestration_by_tech %>%
    filter(sector == 'refining') %>%
    mutate(fuel = if_else(subsector == 'biomass liquids','biomass',
                          if_else(subsector == 'coal to liquids','coal',NA_character_))) %>%
    group_by(scenario,region,fuel,year,subsector) %>%
    summarize(sum_seq = sum(value)) %>%
    ungroup() %>%
    mutate(sector = 'refining') -> refining_c_seq  
  
  upstream_inputs_by_subsector %>%
    left_join(refining_c_seq,by = c('scenario','region','fuel','year','sector','subsector')) %>%
    mutate(sum_seq = if_else(is.na(sum_seq),0,sum_seq),
           upstream_c_emiss = c_input) %>%
    group_by(scenario,region,sector,subsector,year,fuel) %>%
    summarize(upstream_c_input = sum(upstream_c_emiss)) %>% 
    ungroup() -> upstream_c_input
  
  outputs_by_subsector%>%
    filter(sector == 'refining' | sector == 'gas processing') %>%
    rename(PrimaryFuelCO2Coef.name = output) %>%
    left_join(ccoef_mapping,by = c('PrimaryFuelCO2Coef.name')) %>%
    mutate(c_output = value * PrimaryFuelCO2Coef,
           c_output = if_else(is.na(c_output),0,c_output)) %>%
    group_by(scenario,region,sector,year) %>%
    summarize(c_output = sum(c_output)) %>%
    ungroup() -> downstream_c_output
  
  
  emiss_fracs_by_lifecycle_phase <- upstream_c_input %>%
    left_join(downstream_c_output, by = c('scenario','region','sector','year')) %>%
    group_by(scenario,region,sector,year) %>%
    summarize(downstream_emiss_frac = c_output / sum(upstream_c_input)) %>%
    ungroup() %>%
    distinct(scenario,region,sector,year,downstream_emiss_frac) %>%
    mutate(upstream_emiss_frac = 1-downstream_emiss_frac) %>%
    rename(transformation = sector)
  
  df_for_further_processing <- df_for_disag %>%
    filter(is.na(phase) & ghg == 'CO2')
  
  
  df_downstream <- df_for_further_processing %>%
    left_join(emiss_fracs_by_lifecycle_phase,by = c('scenario','region','transformation','year')) %>%
    mutate(phase = 'enduse',
           downstream_emiss_frac = if_else(direct == 'biomass',0,downstream_emiss_frac),
           upstream_emiss_frac = if_else(direct == 'biomass',1,upstream_emiss_frac),
           value = value * downstream_emiss_frac)
  
  df_upstream <-   df_for_further_processing %>%
    left_join(emiss_fracs_by_lifecycle_phase,by = c('scenario','region','transformation','year')) %>%
    mutate(phase = 'midstream',
           downstream_emiss_frac = if_else(direct == 'biomass',0,downstream_emiss_frac),
           upstream_emiss_frac = if_else(direct == 'biomass',1,upstream_emiss_frac),
           value = value * upstream_emiss_frac)

  
  df_lifecycle_disag <- bind_rows(df_upstream, df_downstream, df_for_bindback) %>%
    select(-downstream_emiss_frac,-upstream_emiss_frac) %>%
    mutate(phase = if_else(ghg == 'CO2' & transformation %in% c('district heat','H2 central production','H2 wholesale dispensing','H2 enduse','H2 production'),'midstream',phase))

    
  
  data_input <- data_input %>%
    mutate(value = if_else(is.na(value),0,value))
  
  df_lifecycle_disag <- df_lifecycle_disag %>%
    mutate(value = if_else(is.na(value),0,value))
  
  
  #print(sum(df_lifecycle_disag$value))
  #print(sum(data_input$value))
  
  return(df_lifecycle_disag)
}


direct_aggregation <- function(all_emissions){
  non_energy <- c('limestone','electricity','other industrial processes','industrial processes',
                  'adipic acid','nitric acid','solvents','waste_incineration','wastewater treatment',
                  'comm cooling','resid cooling','urban processes')
  food_agriculture <- c('Wheat','Corn','SugarCrop','SheepGoat','Beef','Dairy','FiberCrop','FodderGrass',
                        'FodderHerb','MiscCrop','OilCrop','OtherGrain','PalmFruit','Pork','Poultry',
                        'Rice','RootTuber', "Fruits", "Legumes", "NutsSeeds", "Soybean", "Vegetables", "OilPalm")
  unique(food_agriculture)
  cwf_mapping <- read_csv('input/CWF-sector-mapping.csv')

  # the units.csv file is updated to include units for SO2_2, SO2_2_AWB, SO2_3, SO2_3_AWB, SO2_4, SO2_4_AWB, PM2.5, PM10, they all have unit of Tg.
  Units <- read_csv('input/Units.csv')
  
  resid_ene_mapping <- 
    sector_label %>% 
    filter(rewrite %in% c("resid cooling", "resid heating", "resid others")) %>% 
    select(-type)
  
  # all_emissions <- all_emissions4
  
  all_emissions %>% 
    left_join(resid_ene_mapping, by = c("enduse" = "sector")) %>% 
    mutate(transformation = if_else(!is.na(rewrite) & transformation == enduse, rewrite, transformation)) %>%
    mutate(enduse = if_else(is.na(rewrite), enduse, rewrite)) %>% 
    mutate(direct = if_else(direct %in% non_energy & Units == 'MTCO2e','Non-energy',direct)) %>%
    mutate(direct = if_else(direct == 'refined liquids','crude oil',direct)) %>%
    mutate(direct = if_else(direct == 'traditional biomass','biomass',direct)) %>%
    mutate(direct = if_else(direct == 'unconventional oil','crude oil',direct)) %>%
    mutate(direct = if_else(direct %in%c('direct air capture', "biochar", "rock weathering"),'CO2 removal',direct)) %>%
    mutate(direct = if_else(direct %in% food_agriculture,'Food and agriculture',direct)) %>%
    mutate(direct = if_else(ghg == 'LUC CO2','LULUCF',direct)) %>%
    mutate(direct = if_else(ghg == 'CO2' & direct %in% c('coal','crude oil','natural gas') & value < 0,'biomass CCS',direct)) %>%
    mutate(direct = if_else(ghg == 'CO2' & direct == 'biomass' & value <= 0,'biomass CCS',direct)) %>%
    mutate(direct = if_else(ghg == 'CO2' & (direct == 'biomass CCS' | direct == 'biomass') & value >= 0,'natural gas',direct)) %>% #due to numerical precision some small amount of biomass CO2 emissions come out as small positive numbers (and vise versa for )
    mutate(transformation = if_else(transformation == 'H2 grid electrolysis','H2 production and distribution',transformation)) %>%
    mutate(transformation = if_else(transformation %in% c('hydrogen','H2 central production','H2 wholesale dispensing'),'H2 production and distribution',transformation)) %>%
    mutate(direct = if_else(enduse == 'UnmanagedLand','LULUCF',direct)) %>%
    mutate(direct = if_else(direct == 'Coal','coal',direct)) %>%
    mutate(enduse = if_else(enduse == 'dac','direct air capture',enduse)) %>%
    mutate(transformation = if_else(transformation == 'dac','direct air capture',transformation)) %>%
    mutate(transformation = if_else(transformation == 'backup_electricity','electricity',transformation)) %>%
    mutate(transformation = if_else(direct == 'limestone','calcination',transformation),
           direct = if_else(direct %in% c('limestone','landfills','wastewater'),'Non-energy',direct),
           ghg = if_else(ghg == 'Captured CO2' & enduse %in% c('chemical feedstocks','industrial feedstocks','construction feedstocks'),'Feedstock embedded carbon',ghg),
           phase = if_else(direct == 'biomass CCS' & phase == 'enduse' & ghg == 'CO2' & transformation %in% c('gas processing','refining','H2 production'),'midstream',phase),
           phase = if_else(direct == 'LULUCF','resource production',phase),
           phase = if_else(direct == 'district heat' & Units == 'Tg','midstream',phase),
           phase = if_else(enduse == 'unconventional oil production','resource production',phase),
           transformation = if_else(enduse %in% c('direct air capture','cement') & direct %in% c('coal','crude oil','natural gas','biomass') & phase == 'enduse','process heat',transformation),
           direct = if_else(direct == 'gas','natural gas',direct),
           direct = if_else(direct == 'biomass CCS' & str_detect(enduse,'feedstocks') & phase == 'enduse','biomass',direct),
           phase = if_else(direct == "biomass CCS" & str_detect(enduse,'feedstocks'), "enduse", phase),
           elec_for_H2 = if_else(direct == "hydrogen" & transformation == "electricity", FALSE, elec_for_H2),
           transformation = if_else(elec_for_H2 == TRUE, 'H2 production and distribution', transformation),
           direct = if_else(elec_for_H2 == TRUE & !(direct %in% c('coal','crude oil','natural gas','biomass','biomass CCS','natural gas','Non-energy')),'electricity',direct),
           elec_for_H2 = if_else(is.na(elec_for_H2),FALSE,elec_for_H2),
           direct = if_else(direct == "CO2 removal" & transformation == "refining", "e-fuel production", direct)) %>% 
    group_by(scenario, region, year, direct, transformation, enduse,  ghg, phase) %>%
    summarize(value = sum(value)) %>%
    ungroup() %>%
    left_join(cwf_mapping,by = c('enduse')) %>%
    left_join(Units,by=c('ghg')) -> all_emissions
  
  # all_emissions %>% filter(direct == "e-fuel production") %>%
  #   group_by(scenario, region, year, direct, transformation, enduse, ghg, CWF_Sector, Units) %>%
  #   summarise(value = sum(value, na.rm = TRUE)) %>% 
  #   mutate(phase = "enduse") %>% 
  #   rbind(all_emissions %>% filter(direct != "e-fuel production")) -> all_emissions
  
  return(all_emissions)
}



# Function to distribute co2 sequestration same as fuel tracer
co2_sequestration_distributor <- function(prj, fuel_tracing, primary_map, WIDE_FORMAT = TRUE){
  
  fuel_tracing <- fuel_tracing %>% 
    filter(primary %in% c('crude oil','coal','natural gas','total biomass', "traditional biomass")) %>%
    # mainly to modify the tradition biomass used for residential heating and other to total biomass
    mutate(primary = if_else(primary == "traditional biomass", "total biomass", primary)) %>%
    # sometime if there are solution errors, the fuel_tracing file will have some NA values, which causes mismatch 
    # in runnning the co2_sequestration_distributor function, so we just filter out the NA value if needed
    filter(!is.na(value))
  
  # List of transformation sectors
  transf_sectors <- (fuel_tracing %>% 
                       distinct(transformation) %>%
                       filter(!(transformation %in% fuel_tracing$enduse)))$transformation
  
  # Ratio of primary in transformation
  primary_in_trans <- fuel_tracing %>%
    # filter(region == "China", year == 2050) %>% # TEMPORARY
    group_by(scenario, region, year, primary, transformation) %>%
    summarise(value = sum(value)) %>%
    group_by(scenario, region, year, transformation) %>%
    mutate(ratio_primary_in_trans = value / sum(value)) %>%
    ungroup() %>%
    select(-value, primary_map = primary, trans_map = transformation) %>%
    filter(trans_map %in% transf_sectors)
  #filter(trans_map != "iron and steel")
  
  # Ratio of enduse in transformation
  enduse_in_trans <- 
    fuel_tracing %>%
    # rename all the H2 related process to be "H2 production and distribution", this aligns with the term used in the seq3_phase table in the following script
    mutate(transformation = if_else(transformation %in% 
                                      c("H2 LDV", "H2 MHDV", "LH2", "H2 wholesale delivery", "H2 industrial"), "H2 production and distribution", transformation)) %>%
    group_by(scenario, region, year, transformation, enduse) %>%
    summarise(value = sum(value)) %>%
    group_by(scenario, region, year, transformation) %>%
    mutate(ratio_enduse_in_trans = value / sum(value)) %>%
    ungroup() %>%
    filter(transformation %in% c(transf_sectors, "H2 production and distribution")) %>%
    select(-value, enduse_map = enduse, enduse = transformation)
  
  # Fix iron and steel, expand direct
  ironsteel_inputs <- getQuery(prj, "inputs by tech") %>%
    # filter(region == "China", year == "2050") %>% # TEMPORARY
    filter(sector == "iron and steel",
           str_detect(technology, "CCS")) %>%
    # Get rid of electricity and scrap
    filter(!(input %in% c("elect_td_ind", "scrap","H2 industrial","H2 enduse"))) %>%
    # Fix input names
    mutate(input = str_replace(input, "delivered coal", "coal"),
           input = str_replace(input, "wholesale gas", "natural gas")) %>%
    left_join(read_csv('input/ccoef_mapping.csv') %>%
                rename(input = PrimaryFuelCO2Coef.name),by = c('input')) %>%
    mutate(value = value * PrimaryFuelCO2Coef,
           value = if_else(is.na(value),0,value)) %>%
    # Get percentage of carbon input in total
    # The implicit assumption here is that the each fuel's sequestered carbon will be proportional to its contribution to overall fuel carbon input
    group_by(scenario, region, year, sector, subsector, technology) %>%
    mutate(ratio_input_in_tech = value / sum(value),
           ratio_input_in_tech = if_else(value == 0 & sum(value) == 0,
                                         0, ratio_input_in_tech)) %>%
    ungroup() %>%
    select(scenario, region, year, technology, input, ratio_input_in_tech) 
  
  # Expand iron and steel from co2 sequestration
  iron_steel_replace <- getQuery(prj, "CO2 sequestration by tech") %>%
    # filter(region == "China", year == "2050") %>%
    filter(sector == "iron and steel") %>%
    left_join(ironsteel_inputs, by = c("scenario", "region", "year", "technology")) %>%
    mutate(input = if_else(is.na(input), "crude oil", input),
           input = if_else(input == 'refined liquids industrial','crude oil',input),
           ratio_input_in_tech = if_else(is.na(ratio_input_in_tech) & input == "crude oil", 
                                         1, ratio_input_in_tech),
           value = value * ratio_input_in_tech) %>%
    select(-subsector, -ratio_input_in_tech, subsector = input)
  
  tot_seq_original <- filter(getQuery(prj, "CO2 sequestration by tech"), region != "Global" & year >= 2005)$value %>% sum(na.rm = T) 

  # Replace CO2 seq of iron&steel in CO2 seq query with the updated iron&steel CO2 seq data
  seq_raw <- getQuery(prj, "CO2 sequestration by tech") %>%
    # filter(region == "China", year == "2050") %>% # TEMPORARY
    filter(sector != "iron and steel", year > 1990) %>%
    bind_rows(iron_steel_replace) %>%
    mutate(sector = if_else(str_detect(sector, "elec_"), "electricity", sector)) %>%
    mutate(sector = if_else(str_detect(sector, "H2"), "H2 production and distribution", sector)) %>%
    group_by(Units, scenario, region, sector, subsector, year) %>%
    summarise(value = sum(value)) %>%
    ungroup()

  tot_seq1 <- filter(seq_raw, region != "Global", year >= 2005)$value %>% sum(na.rm = T)
  
  if (round(tot_seq_original - tot_seq1,0) != 0){
    print("WARNING: Total sequestration from 2005 to 2100 do NOT match after seq data table.")
  }
  
  # Have disaggregated representation for dac and rock weathering 
  seq2 <- seq_raw %>% 
    filter(subsector %in% c("dac", "rock weathering", "direct ocean capture")) %>% 
    mutate(sector = subsector) %>% 
    rbind(seq_raw %>% filter(!subsector %in% c("dac", "rock weathering", "direct ocean capture")))
  
  tot_seq2 <- filter(seq2, region != "Global", year >= 2005)$value %>% sum(na.rm = T)
  
  if (round(tot_seq_original - tot_seq2,0) != 0){
    print("WARNING: Total sequestration from 2005 to 2100 do NOT match after seq2 data table.")
  }
  
  # disaggregate CO2 seq by upstream fuel type. 
  seq3 <- seq2 %>%
    left_join(primary_map, by = "subsector") %>%
    mutate(transformation = sector) %>%
    left_join(primary_in_trans, 
              by = c("scenario", "region", "year", "primary" = "trans_map")) %>%
    # If join occurred, enduse should be old transform,
    # transform should be old primary
    # primary should be primary_map
    mutate(enduse = transformation,
           transformation = if_else(!is.na(ratio_primary_in_trans) & !(transformation %in% transf_sectors), 
                                    primary, transformation),
           primary = if_else(!is.na(ratio_primary_in_trans), 
                             primary_map, primary),
           primary = if_else(transformation == 'process heat dac','natural gas',primary)) %>%
    # Multiply value by ratio of primary in transformation
    mutate(ratio_primary_in_trans = 
             if_else(is.na(ratio_primary_in_trans), 1, ratio_primary_in_trans),
           value = value * ratio_primary_in_trans) %>%
    select(-primary_map, -ratio_primary_in_trans)
  
  ## Here we need to assign phase to the CO2 sequestered. There is a distinction between CO2 sequestered at the enduse vs transformation phases. 
  ## Take refined liquids (RFs) as an example, RLs (RFs with CCS) is used as a energy input for the chemical energy use sector at the enduse phase. 
  ## the CO2 sequestered by the RFs (with CCS) would be considered as an enduse phase seqestration at the chemical energy use sector. In this case,
  ## primary (coal or natural gas, or crude oil)|transformation (refining)|enduse(chemical energy use) would be considered as "enduse" phase,
  ## In a other case, if the CO2 is sequestered during the refining process, and the refining process is further linked to the downstream enduses 
  ## (e.g, chemical energy use). The portion of CO2 sequestration that is happening in the refining process and linked to the downstream chemical 
  ## energy use would be considered "midstream" phase (it is like a embeded CO2 sequestration). 
  ## here we are assigninig the end use phase CO2 sequestration.
  seq3_phase <- seq3 %>% 
    mutate(phase = if_else(transformation %in% 
                             c(transf_sectors, "H2 production and distribution") & transformation == enduse, 
                           "midstream", "enduse"),
           # H2 production also take some gas from the gas processing process, both of them are considered transformation processes,
           # If the end use is H2 production, we just have the transformation process to be all H2 production, and phase to be midstream
           transformation = if_else(enduse == "H2 production and distribution", "H2 production and distribution", transformation),
           phase = if_else(enduse == "H2 production and distribution", "midstream", phase)) 
    
  # tot_seq3 <- filter(seq2, region != "Global")$value %>% sum(na.rm = T) 
  # tot_seq3 <- filter(seq3, region != "Global")$value %>% sum(na.rm = T) 
  tot_seq3 <- filter(seq3_phase, region != "Global")$value %>% sum(na.rm = T) 
  
  if (round(tot_seq_original - tot_seq3,0) != 0){
    print("WARNING: Total sequestration from 2005 to 2100 do NOT match after seq3.")
  }
  

  # disaggregate CO2 seq by downstream applications. 
  # Split out enduses that are actually transformations

  resid_ene_mapping <- 
    sector_label %>% 
    filter(rewrite %in% c("resid cooling", "resid heating", "resid others")) %>% 
    select(-type)
  
  seq4 <- 
    seq3_phase %>%
    left_join(enduse_in_trans, 
              by = c("scenario", "region", "year", "enduse")) %>%
    # If match, replace enduse
    mutate(enduse = if_else(!is.na(enduse_map), 
                            enduse_map, enduse)) %>%
    # Multiply value by ratio of enduse in transformation
    mutate(ratio_enduse_in_trans = 
             if_else(is.na(ratio_enduse_in_trans), 1, ratio_enduse_in_trans),
           value = value * ratio_enduse_in_trans) %>%
    select(-enduse_map, -ratio_enduse_in_trans, -sector, -subsector) %>%
    mutate(value = value * 44/12,
           Units = "MTCO2e",
           ghg = "Captured CO2") %>%
    # rewrite traditional oil to crude oil
    mutate(direct = primary,
           direct = if_else(direct == "traded unconventional oil", "crude oil",direct),
           direct = if_else(direct == "traded oil", "crude oil", direct),
           direct = if_else(direct == "traded coal", "coal", direct),
           direct = if_else(direct == "traded natural gas", "natural gas", direct),
           direct = if_else(direct == "total biomass", "biomass", direct),
           direct = if_else(direct == "atmospheric CO2","CO2 removal",direct),
           enduse = if_else(enduse == "ces","direct air capture",enduse),
           enduse = if_else(enduse == "process heat dac","dac",enduse),
           transformation = if_else(transformation == 'ces','direct air capture',transformation)) %>%
    left_join(resid_ene_mapping, by = c("enduse" = "sector")) %>%
    mutate(enduse = if_else(is.na(rewrite), enduse, rewrite)) %>%
    group_by(scenario, region, direct, transformation, enduse, year, ghg, Units, phase) %>%
    summarise(value = sum(value)) %>%
    ungroup() %>%
    mutate(value = if_else(is.na(value),0,value))
  
  tot_seq4 <- filter(seq4, region != "Global")$value %>% sum(na.rm = T)
  
  if (round(tot_seq_original * 44 / 12 - tot_seq4,0) != 0){
    print("WARNING: Total sequestration from 2005 to 2100 do NOT match after seq4.")
  }
  
  
  # combine direct air capture and direct ocean capture as direct air capture
  seq5 <- 
    seq4 %>% 
    filter(enduse %in% c("dac", "direct ocean capture")) %>%
    mutate(direct = if_else(direct == "dac", "direct air capture", direct),
           transformation = if_else(transformation == "dac", "direct air capture", transformation),
           transformation = if_else(transformation == "direct ocean capture", "direct air capture", transformation),
           enduse = "direct air capture") %>% 
    group_by(scenario, region, direct, transformation, enduse, year, ghg, Units, phase) %>%
    summarise(value = sum(value)) %>%
    ungroup() %>%
    mutate(value = if_else(is.na(value),0,value)) %>%
    rbind(seq4 %>% filter(!enduse %in% c("dac", "direct ocean capture")))

  global <- seq5 %>%
    group_by(scenario, year, direct, transformation, enduse, ghg, Units, phase) %>%
    summarise(value = sum(value)) %>%
    ungroup() %>%
    mutate(region = "Global")
  
  cwf_mapping <- read_csv('input/CWF-sector-mapping.csv')
  
  seq5_comb <- 
    bind_rows(seq5, global) %>% 
    filter(year >= 2005) %>%
    mutate(value = if_else(is.na(value),0,value)) %>%
    # mutate(phase = if_else(transformation %in% c('electricity','H2 enduse','gas processing','refining'),'midstream','enduse')) %>%
    mutate(ghg = if_else(enduse %in% c('chemical feedstocks','industrial feedstocks','construction feedstocks', "other industrial feedstocks"),'Feedstock embedded carbon',ghg)) %>%
    mutate(transformation = if_else(direct == 'limestone','calcination',transformation),
           direct = if_else(direct == 'limestone','Non-energy',direct)) %>%
    left_join(cwf_mapping,by = c('enduse'))

  tot_seq_final <- filter(seq5_comb, region != "Global")$value %>% sum(na.rm = T)
  
  if (round(tot_seq_original* 44 / 12 - tot_seq_final, 0) != 0){
    print("WARNING: Total sequestration from 2005 to 2100 do NOT match original values on final data table")}  
  
  if (WIDE_FORMAT){
    seq5_final <- seq5_comb %>%
      arrange(year) %>%
      pivot_wider(names_from = year, values_from = value) %>%
      arrange(region, direct) %>%
      mutate_all(~ifelse(is.na(.), 0, .))
    #seq3[is.na(seq3_final)] <- 0
  }
  return(seq5_final)
}

# emissions calculation
emissions <- function(CO2, CO2_bio, resource_CO2, nonCO2, LUC, 
                      fuel_tracing, input_raw,
                      GWP, sector_label, land_aggregation, 
                      wide = TRUE){

  # update fuel_tracing to only include primary fuel and biomass, because these flows will have emissions
  fuel_tracing <- fuel_tracing %>% 
    filter(primary %in% c('crude oil','coal','natural gas','total biomass', "traditional biomass")) %>%
    # mainly to modify the tradition biomass used for residential heating and other to total biomass
    mutate(primary = if_else(primary == "traditional biomass", "total biomass", primary)) %>%
  # sometime if there are solution errors, the fuel_tracing file will have some NA values, which causes mismatch 
  # in runnning the emission function, so we just filter out the NA value if needed
    filter(!is.na(value))
  
  ## Step 1 -- update CO2 emission data
  
  # 1.1 the CO2 input is the sector level emission (no_bio), but for transport sector, we need detailed information at the subsector level
  # so the first step is to calculate transport subsector fraction using the CO2 by tech query (with bio), and then we will replace 
  # transport sector emissions with subsector emissions
  
  trn_co2 <- CO2_bio %>%
    group_by(Units, scenario, region, sector, subsector, year) %>%
    summarise(value = sum(value, na.rm = TRUE)) %>%
    filter(stringr::str_detect(sector, "^trn_")) %>%
    mutate(ghg = "CO2") %>%
    group_by(Units, scenario, region, sector, ghg, year) %>%
    mutate(sector_sum = sum(value),
           ratio = value / sector_sum) %>%
    ungroup() %>%
    mutate(ratio = if_else(sector_sum == 0 & is.na(ratio), 0, ratio)) %>%
    select(-value, -sector_sum)
  
  trn_co2_no_bio <- CO2 %>%
    filter(stringr::str_detect(sector, "^trn_")) %>%
    left_join(trn_co2, by = c("Units", "scenario", "region", "sector", "year")) %>%
    mutate(ratio = if_else(value == 0 & is.na(ratio), 1, ratio),
           value = value * ratio,
           sector = subsector) %>%
    select(-subsector, -ghg, -ratio)
  
  CO2_trn_update <- CO2 %>%
    filter(!stringr::str_detect(sector, "^trn_")) %>%
    bind_rows(trn_co2_no_bio) %>%
    mutate(ghg = "CO2") %>%
    bind_rows(resource_CO2 %>%
                group_by(Units, scenario, region, sector, ghg, year) %>%
                summarise(value = sum(value, na.rm = TRUE)))
  
  # 1.2 next step is to remove air CO2 that goes into dac to liquid from the total air CO2, because the air CO2 that goes into dac to liquid will be embeded in fuel,
  # which will ultimately combusted and release, so we assume this portion of air CO2 will be cancelled, and then need to be removed from the total air CO2. 
  
  airco2_to_refine <- input_raw %>%
    filter(subsector == "dac to liquids", input == "airCO2") %>%
    group_by(scenario,region,year,sector,subsector,input,Units) %>%
    summarize(value = sum(value)) %>%
    ungroup() %>%
    mutate(Units = "MTC", sector = input) %>% 
    select(-input, -subsector)
  
  CO2_air_update_1 <- 
    CO2_trn_update %>% 
    filter(sector == "airCO2") %>% 
    left_join(airco2_to_refine, 
              by = c("Units", "scenario", "region", "sector", "year")) %>%
    mutate(value = value.x + value.y) %>%
    select(-value.x, -value.y) 
  
  # 1.3 then we also disaggregate the air CO2 into dac vs rock weathering, 
  # we use the CO2 sequestion query to calculate the share of dac vs rock weathering 
  # here we consider direct ocean capture as dac as well. 
  airCO2_share <- getQuery(prj, "CO2 sequestration by tech") %>% 
    filter(sector == "CO2 removal") %>% 
    group_by(Units, scenario, region, sector, subsector, year) %>%
    summarise(value = sum(value, na.rm = TRUE)) %>% 
    # mutate(sector = subsector) %>%
    group_by(Units, scenario, region, sector, year) %>%
    mutate(ratio = value/sum(value)) %>% 
    ungroup() %>%
    select(-Units, -sector, -value)
  
  CO2_air_update_2 <- 
    CO2_air_update_1 %>%
    left_join(airCO2_share,
              by = c("scenario", "region", "year")) %>%
    mutate(value_keep = ratio * value,
           sector = subsector) %>% 
    select(Units, scenario, region, sector, year, 
           ghg, value = value_keep)
  
  # CO2 update completed.
  CO2_all <- 
    CO2_air_update_2 %>% 
    rbind(CO2_trn_update %>% 
            filter(sector != "airCO2"))
  
  

  # Step 2 -- nonCO2 update 
  
  # similar to CO2, we also need the non CO2 emission for transport at the subsector level, so here we just keep the subsector level detail, and get
  # rid of the sector information.
  trn_nonco2 <- nonCO2 %>%
    filter(stringr::str_detect(sector, "^trn_")) %>%
    mutate(sector = subsector)

  # add the transport non CO2 back to all non CO2 emissions
  nonCO2_all <- nonCO2 %>%
    # Remove co2 emissions and transport emissions, then sum to sector 
    filter(!stringr::str_detect(sector, "^trn_")) %>%
    # Add in transport nonCO2s and resource nonCO2s
    bind_rows(trn_nonco2, 
              resource_nonCO2) %>%
    group_by(Units, scenario, region, sector, ghg, year) %>%
    summarise(value = sum(value)) %>%
    ungroup()


  # Then combine all emissions (note that here ghg includes both ghg and non-ghg, let's just all this data set as ghg for now)
  ghg <- CO2_all %>% 
    bind_rows(nonCO2_all)
  
  
  # Add in GWPs and calculate CO2e (for all non-ghg, we just assign GWP as 1, keep them as they are)
  ghg_co2eq <- ghg %>%
    left_join(GWP, by = c("ghg", "Units")) %>%
    mutate(value = value * GWP,
           Units = if_else(type %in% c('CO2','Super Pollutant'),"MTCO2e",Units)) %>%
    na.omit() %>%
    select(-type, -GWP) %>%
    # remove all the H2 emissions
    filter(!ghg %in% c('H2', "H2_AWB")) %>% 
    ## YQ -- this is a workaround, H2 central production has H2 production in 2020, but there is no consumption of H2 central production in 2020, that
    ## cause some NA value in the downstream processes, here we remove the emission related to H2 central production in 2020 to avoid the issue. 
    ## We need to address this later
    filter(!(sector == "H2 central production" & year == 2020)) %>%
    # only keep data starting from 1990
    filter(year > 1990)
  
  # Here, all the pre-process work for emission data are completed, next, we disaggregate the mission to different phases (primary, transformation, enduse)
  ghg_rewrite <- ghg_co2eq  %>%
    left_join(sector_label, by = "sector") %>%
    # Remove delivered gas, delivered biomass, and wholesale gas
    filter(type != "eliminate") %>%
    # Sum by rewrite sector
    group_by(Units, scenario, region, year, type, rewrite, ghg) %>%
    summarise(value = sum(value)) %>%
    ungroup() %>%
    rename(direct = rewrite) 
  
  
  # sector_w_NA <- ghg_rewrite %>% filter(is.na(rewrite))
  # unique(sector_w_NA$sector)
  
  # Step 3, disaggregate the emission into different phases. 
  # 3.1 -- Enduse sectors are fine as is - just need to add passthrough and enduse columns
  enduse <- ghg_rewrite %>%
    filter(type == "enduse") %>%
    mutate(transformation = direct,
           enduse = direct) %>%
    select(-type)

  
  # 3.2 -- process the H2 related fuel use (need to check the purpose of this)
  H2_emiss <- ghg_rewrite %>%
    filter(str_detect(direct,'H2 '))
  
  if (nrow(H2_emiss) == 0){
    fuel_tracing <- fuel_tracing %>%
      mutate(elec_for_H2 = if_else(str_detect(transformation,'H2'),TRUE,FALSE))
  }
  
  
  fuel_tracing_adj_h2 <- 
    # fuel_tracing %>% 
    fuel_tracing %>% 
    #fuel_tracing_debug %>%
    mutate(transformation = if_else(transformation %in% c("H2 industrial", "H2 LDV", "H2 MHDV", "H2 wholesale delivery", "LH2") & elec_for_H2 == FALSE, "H2 central production", transformation)) %>%
    group_by(scenario, region, year, primary, transformation, enduse, Units, elec_for_H2) %>%
    summarise(value = sum(value, na.rm = TRUE))
  
  
  # recalculate ratios with filtered inputs (ratio of end use in primary, and ratio of end use in transformation)
  fuel_tracing_adj <- 
    fuel_tracing_adj_h2 %>%
    mutate(transformation = if_else(elec_for_H2 == TRUE,'electricity',transformation)) %>% 
    # Get ratio of enduse in primary
    group_by(scenario, region, year, primary) %>%
    mutate(ratio_enduse_in_primary = value / sum(value)) %>%
    # Get ratio of enduse in transformation (disaggregated by primary category)
    group_by(scenario, region, year, transformation) %>%
    mutate(ratio_enduse_in_transformation = value / sum(value)) %>%
    ungroup() 

  # 3.3 -- disaggregate emission based on end use in transformation ratio
  # Transformation sectors need to be distributed to enduse -- the share of end use in transformation
  transform_division <- 
    fuel_tracing_adj %>%
    mutate(ratio_enduse_in_transformation = if_else(
      value == 0 & is.na(ratio_enduse_in_transformation), 0, ratio_enduse_in_transformation)) %>%
    group_by(scenario, region, year, transformation, enduse, elec_for_H2) %>%
    summarize(ratio = sum(ratio_enduse_in_transformation)) %>%
    ungroup() %>%
    mutate(direct = transformation)
  
  if (any(str_detect('H2 enduse',transform_division$transformation))){
    
    ghg_rewrite <- ghg_rewrite %>%
      mutate(direct = if_else(direct %in% c('H2 central production','H2 forecourt production','H2 retail delivery','H2 retail dispensing','H2 wholesale dispensing','elect_td_H2'), 'H2 enduse', direct)) %>%
      group_by(Units,scenario,region,year,type,direct,ghg) %>%
      summarize(value = sum(value)) %>%
      ungroup()
  }
  
  # allocate the emission of transformation to the specific end use
  transformation <- ghg_rewrite %>%
    filter(type == "transformation") %>%
    left_join(transform_division, by = c("scenario", "region", "year", "direct")) %>%
    filter(!(value == 0 & is.na(ratio))) %>%
    mutate(value = value * ratio) %>%
    select(-ratio, -type)
  
  # 3.4 -- disaggregate emission based on end use in transformation ratio
  # rename some of the primary sector names 
  primary_division <- fuel_tracing_adj %>%
    mutate(primary = stringr::str_replace(primary, "total biomass", "biomass"),
           primary = stringr::str_replace(primary, "traded unconventional oil", "unconventional oil"),
           primary = stringr::str_replace(primary, "traded oil", "crude oil"),
           primary = stringr::str_replace(primary, "traded coal", "coal"),
           primary = stringr::str_replace(primary, "traded natural gas", "natural gas"),
           direct = primary,
           ratio = ratio_enduse_in_primary) %>%
    select(-ratio_enduse_in_primary, -ratio_enduse_in_transformation, -value, -Units) 
  
  primary <- ghg_rewrite %>%
    filter(type == "primary") %>%
    left_join(primary_division, by = c("scenario", "region", "year", "direct")) %>%
    filter(!(value == 0 & is.na(ratio))) %>%
    mutate(value = value * ratio) %>%
    select(-ratio, -type, -primary)

  # 3.5 -- Take care of LUC emissions
  LUC_emissions <- LUC %>%
    left_join(land_aggregation, by = c("sector" = "landtype")) %>%
    group_by(Units, scenario, region, sector = agg_land, year, ghg) %>%
    summarise(value = sum(value)) %>%
    ungroup() %>%
    mutate(direct = sector,
           transformation = sector,
           enduse = sector) %>%
    select(-sector) %>%
    left_join(GWP, by = c("Units", "ghg")) %>%
    mutate(value = value * GWP,
           Units = if_else(type %in% c('CO2','Super Pollutant'),"MTCO2e",Units)) %>%
    select(-GWP, -type) 
  
  
  all_emissions <- bind_rows(primary, transformation, enduse, LUC_emissions) %>%
    filter(year > 1990) %>%
    select(scenario, region, year, direct, transformation, enduse, ghg, value, Units,elec_for_H2) %>%
    # This line of code seems to be a mistake, because it changes NA to False, and original False to TRUE.
    # In fact, we only want to change NA to False, Remain original TRUE/FALSE as they are. I updated to code. 
    # mutate(elec_for_H2 = if_else(is.na(elec_for_H2),FALSE,TRUE))
    mutate(elec_for_H2 = if_else(is.na(elec_for_H2), FALSE, elec_for_H2))

  
  all_emissions_rus <- all_emissions
  
  original_emissions <- sum(filter(ghg_co2eq, year > 1990)$value) + 
    sum(filter(LUC_emissions, year > 1990)$value)
  
  calculated_emissions_rus <- filter(all_emissions_rus, region != "Global")$value %>% sum(na.rm = T)
  
  
  ## mismatch check
  
  # sum((primary%>%filter(year > 1990))$value, na.rm = TRUE) + 
  #   sum((transformation%>%filter(year > 1990))$value, na.rm = TRUE) + 
  #   sum((enduse%>%filter(year > 1990))$value, na.rm = TRUE) + 
  #   sum((LUC_emissions%>%filter(year > 1990))$value, na.rm = TRUE)
  
  # sum((ghg_rewrite %>% filter(type == "primary"))$value)
  # sum((ghg_rewrite %>% filter(type == "transformation"))$value)
  # sum((ghg_rewrite %>% filter(type == "enduse"))$value)
  
  # 
  # sum(ghg_co2eq$value)
  # sum(ghg_rewrite$value)
  
  
  if (round(original_emissions - calculated_emissions_rus,0) != 0){
    print("Initial disaggregation pass: Total emissions from 1990 to 2100 do NOT match.")
    print("percent difference between raw GCAM output data and initial disaggregation pass emissions is:")
    print(100*(original_emissions - calculated_emissions_rus)/original_emissions)
    
    
    ghg_rewrite_grouped <- ghg_rewrite %>%
      bind_rows(LUC_emissions) %>%
      #rename(direct = rewrite) %>%
      group_by(scenario,region,ghg,year,direct) %>%
      summarize(value = sum(value)) %>%
      ungroup()
    
    calculated_emissions_rus_grouped <- all_emissions_rus %>%
      filter(region != 'Global') %>%
      group_by(scenario,region,year,ghg,direct,Units) %>%
      summarize(value = sum(value)) %>%
      ungroup()
    
    write_csv(ghg_rewrite_grouped,'ghg_rewrite_grouped.csv')
    write_csv(calculated_emissions_rus_grouped,'calculated_emissions_rus_grouped.csv')
    
    joined_emiss <- ghg_rewrite_grouped %>%
      rename(orig_value = value) %>%
      full_join(calculated_emissions_rus_grouped %>% 
                  rename(calc_value = value),by = c('scenario','region','year','ghg','direct')) %>%
      mutate(diff = orig_value - calc_value) %>%
      filter(year > 1990)
      
    write_csv(joined_emiss,'DEBUG_joined_initial_disag.csv')
    
    
    all_emissions_rus_grouped <- all_emissions_rus %>%
      group_by(scenario,region,year,ghg) %>%
      summarize(initial_disag = sum(value)) %>%
      ungroup()
    
    original_emissions_grouped <- ghg %>%
      group_by(scenario,region,year,ghg) %>%
      summarize(original_emissions = sum(value)) %>%
      ungroup()
    
    original_left_join_initial_disag <- original_emissions_grouped %>%
      left_join(all_emissions_rus_grouped,by = c('scenario','region','year','ghg')) %>%
      mutate(diff = abs(initial_disag - original_emissions)) %>%
      arrange(desc(diff)) %>%
      filter(diff != 0)
    
    
    initial_disag_left_join_original <- all_emissions_rus_grouped %>%
      left_join(original_emissions_grouped ,by = c('scenario','region','year','ghg')) %>%
      mutate(diff = abs(initial_disag - original_emissions)) %>%
      arrange(desc(diff)) %>%
      filter(diff != 0)
  
    
    
  } else {
    print("Inital disaggregation pass: Total emissions from 1990 to 2100 match.")
  }
  
  #final fuel processing - JF
  
  # sometime if there are solution errors, the all_emissions file will have some NA values, which causes issues 
  # in runnning the final_fuel_CO2_disag function, so we just filter out the NA value if needed
  all_emissions <- all_emissions %>% filter(!is.na(value))
  
  all_emissions1 <- final_fuel_CO2_disag(all_emissions)
  
  # the following code will need to be removed once the historical negative biomass CCS issue is resolved
  all_emissions2 <- all_emissions1 #%>% 
    #filter(direct == "biomass CCS", year <= 2015) %>% 
    #mutate(value = 0) %>% 
    #rbind(all_emissions1 %>% filter(direct != "biomass CCS" | year > 2015))
  
  all_emissions3 <- final_fuel_nonCO2_disag(all_emissions2) 
  
  all_emissions4 <- lifecycle_CO2_emiss_phase_disag(all_emissions3)
  
  all_emissions_region <- direct_aggregation(all_emissions4)
  
  # filter(all_emissions4, region != "Global")$value %>% sum(na.rm = T)
  # 
  # calculated_emissions_rus <- filter(all_emissions_rus, region != "Global")$value %>% sum(na.rm = T)
  

  # sum((all_emissions%>%filter(year >= 2005))$value, na.rm = TRUE)
  # sum((all_emissions1%>%filter(year >= 2005))$value, na.rm = TRUE)
  # sum((all_emissions2%>%filter(year >= 2005))$value, na.rm = TRUE)
  # sum((all_emissions3%>%filter(year >= 2005))$value, na.rm = TRUE)
  # sum((all_emissions4%>%filter(year >= 2005))$value, na.rm = TRUE)

  
  # Combine all emissions and add global region
  all_emissions_global <- all_emissions_region %>%
    group_by(scenario, year, direct, transformation, enduse, ghg, Units, phase, CWF_Sector) %>%
    #group_by(scenario, year, direct, transformation, enduse, ghg, Units) %>%
    summarise(value = sum(value)) %>%
    ungroup() %>%
    mutate(region = "Global")
  
  all_emissions_all <- bind_rows(all_emissions_region, all_emissions_global) %>% 
    filter(year >= 2005) %>%
    mutate(value = if_else(is.na(value),0,value)) %>%
    arrange(scenario, region, direct, transformation, enduse, CWF_Sector, phase, year)
  

  calculated_emissions <- filter(all_emissions_all, region != "Global")$value %>% sum(na.rm = T)
  
  if (round(original_emissions - calculated_emissions, 0) != 0){
    print("Total emissions from 1990 to 2100 do NOT match.")
    print("percent difference between raw GCAM output data and fully disaggregated emissions is:")
    print(100*(original_emissions - calculated_emissions)/original_emissions)
    
    print("Writing inputs by tech csv for debugging... ")
    
#    inputs_by_tech <- rgcam::getQuery(prj,'inputs by tech') %>%
#      filter(Units %in% c('EJ','million pass-km','million ton-km'))
    
    #write_csv(inputs_by_tech,'DEBUG_inputs_by_tech.csv')
    if (round(calculated_emissions_rus - calculated_emissions,0) != 0){
      print("Sum of initial disaggregation does not equal sum of final disaggregation.  Check grouped output frames for debugging")
      
      initial_disag <- all_emissions_rus %>%
        group_by(scenario,region,year,enduse,ghg,Units) %>%
        summarize(initial_disag = sum(value))
      
      
      final_disag <- all_emissions_all %>%
        filter(region != 'Global') %>%
        group_by(scenario,region,year,enduse,ghg,Units) %>%
        summarize(final_disag = sum(value))
      
      
      final_disag %>%
        left_join(initial_disag %>% mutate(enduse = if_else(enduse == 'ces','direct air capture',enduse)), by = c('scenario','region','year','enduse','ghg','Units')) %>%
        mutate(match = if_else(final_disag == initial_disag, TRUE,FALSE),
               diff = abs(final_disag - initial_disag)) %>%
        arrange(desc(diff)) %>%
        filter(diff != 0) -> final_leftjoin_init
      
      
      initial_disag %>%
        left_join(final_disag %>% mutate(enduse = if_else(enduse == 'direct air capture','ces',enduse)), 
                  by = c('scenario','region','year','enduse','ghg','Units')) %>%
        mutate(match = if_else(final_disag == initial_disag, TRUE,FALSE),
               diff = abs(final_disag - initial_disag)) %>%
        arrange(desc(diff)) %>%
        filter(diff != 0) -> init_leftjoin_final
      
  

      write_csv(final_leftjoin_init,'DEBUG_final_left_join_init.csv')
      write_csv(init_leftjoin_final,'DEBUG_init_left_join_final.csv')
      

      
      
    } else {
      print("")
    }

  } else {
    print("Total emissions from 1990 to 2100 match.")
  }
  if (wide == TRUE){
    all_emissions_final <- all_emissions_all %>% 
      filter(abs(value) > 10^-9) %>%
      pivot_wider(names_from = year, values_from = value, values_fill = list(value = 0))
  }
  
  return(all_emissions_final)
}

# land change tracker
land_change_tracker <- function(prj, land_aggregation, wide = TRUE){
  land_alloc <- rgcam::getQuery(prj, "detailed land allocation") %>%
    # # fix land names separated by underscore
    # dplyr::mutate(landleaf = sub("Root_Tuber","RootTuber",landleaf),
    #               landleaf = sub("biomass_grass","biomassgrass",landleaf),
    #               landleaf = sub("biomass_tree","biomasstree",landleaf)) %>%
    # extract land type and location
    dplyr::mutate(landtype = stringr::str_split_fixed(landleaf, "_", 4)[,1],
                  location = stringr::str_split_fixed(landleaf, "_", 4)[,2]) %>%
    # Sum over different water and efficiency types
    dplyr::group_by(scenario, region, landtype, location, year, Units) %>%
    dplyr::summarise(value = sum(value)) %>%
    dplyr::ungroup() %>%
    # Join in aggregate land type
    dplyr::left_join(land_aggregation, by = "landtype")
  
  # Calculate changes in land type each time period
  land_change <- land_alloc %>%
    # Group by landtype and location to get net change from previous year
    dplyr::group_by(scenario, region, landtype, location) %>%
    dplyr::mutate(change = round(value - dplyr::lag(value),3)) %>%
    # Calculate total land change in each location/year:
    # since we have positive and negative values, sum absolute value, divide by 2
    dplyr::group_by(scenario, region, location, year) %>%
    dplyr::mutate(total_change = sum(abs(change))/2) %>%
    dplyr::ungroup()
  
  # Need a full list of to and from landtypes in each location
  a <- land_change %>%
    dplyr::select(scenario, region, landtype, location, agg_land, year) %>%
    dplyr::distinct()
  b <- land_change %>%
    dplyr::select(scenario, region, landtype, location, agg_land) %>%
    dplyr::distinct()
  
  land_change_apportion <- dplyr::left_join(a, b, by = c("scenario", "region", "location")) %>%
    dplyr::filter(landtype.x != landtype.y) %>%
    # Join in change to
    dplyr::left_join(land_change, by = c("scenario", "region", "landtype.x" = "landtype", "location", "year")) %>%
    # Join in change from
    dplyr::left_join(land_change, by = c("scenario", "region", "landtype.y" = "landtype", "location", "year")) %>%
    # Apportion change
    dplyr::mutate(change_apportion = 0,
                  change_apportion = dplyr::if_else((change.x > 0 & change.y < 0),
                                                    -1*change.x * change.y / total_change.x,
                                                    change_apportion),
                  change_apportion = dplyr::if_else((change.x < 0 & change.y > 0),
                                                    change.x * change.y / total_change.x,
                                                    change_apportion)) %>%
    dplyr::select(scenario, region, location, landtype.from = landtype.y, landtype.to = landtype.x,
                  agg_land.from = agg_land.y, agg_land.to = agg_land.x,
                  year, value = change_apportion, Units = Units.x)
  
  land_change_final <- land_change_apportion %>%
    group_by(scenario, region, agg_land.from, agg_land.to, year, Units) %>%
    summarise(value = sum(value)) %>%
    ungroup() %>%
    filter(year > 1975,
           agg_land.from != agg_land.to)
  
  if (wide == TRUE){
    land_change_final <- land_change_final %>%
      pivot_wider(names_from = year, values_from = value, values_fill = list(value = 0))
  }
  
  return(land_change_final)
  
}

# Split apart queries into tibbles
query_splitter <- function(query_file){
  # Read in queries and save as rgcam-style list of tibbles
  # This for loop divides up the query into individual queries, based on where "scenario" occurs in the query output
  query_text <- readr::read_lines(query_file)
  query_start <- grep("scenario", query_text)
  query_names <- unique(query_text[query_start - 1])
  
  tibble_list <- list()
  # Loop through for each query name 
  for (name in query_names){
    num <- which(query_text[query_start - 1] == name)
    stopifnot(length(num) == 1)
    query_pos <- query_start[num]
    # If the query is not the last query in file, only read in until next query
    if (num != length(query_start)){
      next_query_pos <- query_start[num+1]
      df <- read_query(query_file, skip = query_pos - 1, n_max = next_query_pos - query_pos - 2)
      # Otherwise, read until end of file
    }else{
      df <- read_query(query_file, skip = query_pos - 1)
    }
    df <- df %>%
      pivot_longer(cols = matches("[0-9]{4}"),
                   names_to = "year") %>%
      mutate(year = as.integer(year)) %>%
      select(-matches("X([0-9]+)")) %>%
      rename_with(tolower, -Units)
    
    # Need to split to get into rgcam formatting
    df2 <- split(df, df$scenario)
    
    for (scenario in names(df2)){
      tibble_list[[scenario]] [[name]] <- df2[[scenario]]
    }
    
  }
  return(tibble_list)
}
# Read queries quietly
read_query <- function(...){
  suppressMessages(readr::read_csv(...)) %>%
    mutate(scenario = date_remover(scenario))
}
# Remove dates from queries
date_remover <- function (x){
  pos <- regexpr(",date", x)
  ifelse(pos > -1L, substr(x, 1, pos - 1L), x)
}


#tmp <- fuel_tracing 

#H2_inputs <- getQuery(prj,'inputs by tech') %>%
#  filter(str_detect(sector,'H2'),
#         input %in% c('regional coal','regional biomass','delivered gas','regional natural gas'))
