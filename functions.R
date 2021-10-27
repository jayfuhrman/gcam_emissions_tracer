library(dplyr)
library(tidyr)

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
    if (df$ratio[i] == 1 && df$ratio2[i] == 1 ){
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
  return(df %>% select(-scenario, -region, -year))
}

# use ratios to apportion passthru sectors to transformation sectors
passthru_remove <- function(df, remaining_passthru = NULL){
  if (is.null(remaining_passthru)){
    remaining_passthru <- (df %>% filter(type == "passthru") %>% distinct(sector))$sector
  }
  
  for (passthru in remaining_passthru){
    new_sectors <- df %>%
      filter(input == passthru) %>%
      select(scenario, region, sector, year, type, ratio)
    
    to_expand <- df %>%
      filter(sector == passthru) %>%
      select(-sector, -ratio, -ratio2, -type) %>%
      right_join(new_sectors, by = c("scenario", "region", "year") ) %>%
      mutate(value = value * ratio)
    
    df <- df %>% 
      filter(input != passthru,
             sector != passthru) %>%
      bind_rows(to_expand)
  }
  
  
  return(df %>% select(-scenario, -region, -year))
}

# remove transformation sectors from inputs of other transformations
transform_distributer <- function(df, transform_sectors){
  for (transform in transform_sectors){
    inputs <- (df %>% filter(sector == transform))$input
    inputs_to_expand <- dplyr::intersect(inputs, transform_sectors)
    
    for (inp in inputs_to_expand){
      sector_input <- df %>%
        filter(sector == transform,
               input == inp)
      
      input_df <- df %>%
        filter(sector == inp,
               !(input %in% transform_sectors))
      
      total <- sum(df$value)
      input_df <- input_df %>%
        mutate(ratio = value / total) %>%
        select(scenario, region, input, year, ratio)
      
      to_expand <- sector_input %>%
        select(scenario, region, sector, year, type, value) %>%
        right_join(input_df, by = c("scenario", "region", "year") ) %>%
        mutate(value = value * ratio)
      ####
      # Need to subtract any resource that goes into other transformation from original transform sector
      ###
      subtract_from_upstream <- df %>%
        filter(sector == inp,
               !(input %in% transform_sectors)) %>%
        left_join(to_expand, by = c("scenario", "region", "input", "year")) %>%
        mutate(value = value.x - value.y) %>%
        select(scenario, region, year, input, sector = sector.x, type = type.x, value)
      
      
      df <- df %>% 
        filter(!(input == inp & sector == transform)) %>%
        bind_rows(to_expand) %>%
        filter(!(sector == inp & !(input %in% transform_sectors))) %>%
        bind_rows(subtract_from_upstream) %>%
        group_by(scenario, region, input, sector, type, year) %>%
        summarise(value = sum(value)) %>%
        ungroup() 
    }
  }

  return(df %>% select(-scenario, -region, -year))
}

# MAIN FUNCTION for fuel tracing
fuel_distributor <- function(prj){
  ###################  Data Tidying ###################  
  input <- rgcam::getQuery(prj, "inputs by subsector") %>%
    # Remove industry and chemical, as they simply aggregate more detailed sectors
    filter(sector != "chemical", sector != "industry")
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
                              "H2 enduse", "refined liquids enduse", "refined liquids industrial",
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
  
  # single_use_sectors <- input_tracing %>%
  #   filter(sector %in% primary_remove_df$sector) %>% 
  #   group_by(scenario, region, sector, year) %>%
  #   count() %>%
  #   filter(n == 1,
  #          sector != "regional biomass") %>%
  #   ungroup()
  
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
  
  # Next, want to get rid of passthru sectors that only have 1 input
  #
  
  # Get ratio of sector in each input
  in_ratio <- global_inputs %>% 
    group_by(scenario, region, input, year) %>%
    mutate(ratio = value / sum(value)) %>%
    ungroup()  %>%
    # Remove nans - these sectors no longer needed
    na.omit()
  
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
    na.omit()
  
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
  
  print("Remaining passthru sectors replaced")
  
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
  
  # Now need to remove transformation sectors from inputs of other transformations
  # ASSUMING THAT REFINED LIQUIDS ARE UPSTREAM OF ELECTRICITY
  
  transform_sectors <- c("elect_td_bld", "elect_td_ind", "elect_td_trn",
                         "H2 enduse", "district heat", "refined liquids enduse", "refined liquids industrial",
                         "wholesale gas", "delivered gas")
  
  in_primary <- in_passthru_remove %>%
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
    bind_rows(gas_in_unconventional_oil) 
  
  # Group electricity, refining, gas processing
  final_df <- final_df %>%
    mutate(transformation = if_else(transformation %in% c("elect_td_bld", "elect_td_ind","elect_td_trn"),
                                    "electricity", transformation),
           transformation = if_else(transformation %in% c("refined liquids enduse", "refined liquids industrial"),
                                    "refining", transformation),
           transformation = if_else(transformation %in% c("delivered gas", "wholesale gas"),
                                    "gas processing", transformation)
    ) %>%
    group_by(scenario, region, year, primary, transformation, enduse) %>%
    summarise(value = sum(value)) %>%
    ungroup()
  
  
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
    left_join(new_totals, by = c("scenario", "region", "year", "input")) %>%
    mutate(diff = round(value.x - value.y, 3))
  
  if (any(is.na(comp))){
    print("NAs in fuel total comparison (likely due to historical oil)")
  }
  
  if (any(abs(comp$diff) > 0, na.rm = TRUE)){
    rows <- nrow(comp %>% filter(abs(diff) > 0))
    print(paste0("Fuel totals incorrect in ", rows, " rows."))
  } else {
    print("All fuel totals correct.")
  }
  
  return(final_df)
}

# emissions calculation
emissions <- function(CO2, nonCO2, LUC, fuel_tracing, GWP, sector_label, land_aggregation, wide = TRUE){
  #
  # First replace transport sector emissions with subsector emissions
  #
  
  # Get all transport nonCO2 emissions
  trn_nonco2 <- nonCO2 %>%
    filter(stringr::str_detect(sector, "^trn_"), 
           ghg!= "CO2") %>%
    mutate(sector = subsector)
  
  # Get all transport CO2 emissions and ratio of each subsector in sector
  trn_co2 <- nonCO2 %>%
    filter(stringr::str_detect(sector, "^trn_"), 
           ghg == "CO2") %>%
    group_by(Units, scenario, region, sector, ghg, year) %>%
    mutate(ratio = value / sum(value)) %>%
    ungroup() %>%
    select(-value)
  
  # Clean up nonCO2 and add in transport and resource emissions
  nonCO2 <- nonCO2 %>%
    # Remove co2 emissions and transport emissions, then sum to sector 
    filter(ghg != "CO2",
           !stringr::str_detect(sector, "^trn_")) %>%
    # Add in transport nonCO2s and resource nonCO2s
    bind_rows(trn_nonco2, resource_nonCO2) %>%
    group_by(Units, scenario, region, sector, ghg, year) %>%
    summarise(value = sum(value)) %>%
    ungroup()
  
  # SLIGHT DIFFERENCE IN CO2 EMISSIONS - APPORTION to CO2 no bio emissions based on other ratio
  trn_co2_no_bio <- CO2 %>%
    filter(stringr::str_detect(sector, "^trn_")) %>%
    left_join(trn_co2, by = c("Units", "scenario", "region", "sector", "year")) %>%
    mutate(value = value * ratio,
           sector = subsector) %>%
    select(-subsector, -ghg, -ratio)
  
  CO2 <- CO2 %>%
    filter(!stringr::str_detect(sector, "^trn_")) %>%
    bind_rows(trn_co2_no_bio) %>%
    mutate(ghg = "CO2")
  #
  # Then combine all GHG emissions
  #
  
  ghg <- nonCO2 %>% 
    bind_rows(CO2)
  
  #
  # Add in GWPs and calculate CO2e
  #
  ghg <- ghg %>%
    left_join(GWP, by = c("ghg", "Units")) %>%
    mutate(value = value * GWP,
           Units = "MTCO2e") %>%
    na.omit() %>%
    select(-type, -GWP)
  
  #
  # Now need to start distributing direct emissions
  #
  ghg_rewrite <- ghg %>%
    left_join(sector_label, by = "sector") %>%
    # Remove delivered gas, delivered biomass, and wholesale gas
    filter(type != "eliminate") %>%
    # Sum by rewrite sector
    group_by(Units, scenario, region, year, type, rewrite, ghg) %>%
    summarise(value = sum(value)) %>%
    ungroup() %>%
    rename(direct = rewrite)
  
  # Enduse sectors are fine as is - just need to add passthrough and enduse columns
  enduse <- ghg_rewrite %>%
    filter(type == "enduse") %>%
    mutate(transformation = direct,
           enduse = direct) %>%
    select(-type)
  
  # Transformation sectors need to be distributed to enduse
  transform_division <- fuel_tracing %>%
    group_by(scenario, region, year, transformation, enduse) %>%
    summarise(ratio = sum(ratio_enduse_in_transformation)) %>%
    ungroup() %>%
    mutate(direct = transformation)
  
  transformation <- ghg_rewrite %>%
    left_join(transform_division, by = c("scenario", "region", "year", "direct")) %>%
    filter(type == "transformation") %>%
    mutate(value = value * ratio) %>%
    select(-ratio, -type)
  
  # Primary sectors need to be distributed to enduse
  primary_division <- fuel_tracing %>%
    mutate(primary = stringr::str_replace(primary, "total biomass", "biomass"),
           primary = stringr::str_replace(primary, "traded unconventional oil", "unconventional oil"),
           direct = primary,
           ratio = ratio_enduse_in_primary) %>%
    select(-ratio_enduse_in_primary, -ratio_enduse_in_transformation, -value) 
  
  primary <- ghg_rewrite %>%
    filter(type == "primary") %>%
    left_join(primary_division, by = c("scenario", "region", "year", "direct")) %>%
    mutate(value = value * ratio) %>%
    select(-ratio, -type, -primary)
  
  # Take care of LUC emissions
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
           Units = "MTCO2e") %>%
    select(-GWP, -type) 
  
  # Combine all emissions and add global region
  all_emissions <- bind_rows(primary, transformation, enduse, LUC_emissions) %>%
    filter(year > 1990) %>%
    select(scenario, region, year, direct, transformation, enduse, ghg, value, Units)
  
  global <- all_emissions %>%
    group_by(scenario, year, direct, transformation, enduse, ghg, Units) %>%
    summarise(value = sum(value)) %>%
    ungroup() %>%
    mutate(region = "Global")
  
  all_emissions <- bind_rows(all_emissions, global)

  original_emissions <- sum(filter(ghg, year > 1990)$value) + 
    sum(filter(LUC_emissions, year > 1990)$value)
  calculated_emissions <- filter(all_emissions, region != "Global")$value %>% sum(na.rm = T)
  
  if (round(original_emissions - calculated_emissions,0) != 0){
    print("Total emissions from 1990 to 2100 do NOT match.")
  } else {
    print("Total emissions from 1990 to 2100 match.")
  
  }
  
  if (wide == TRUE){
    all_emissions <- all_emissions %>% 
      pivot_wider(names_from = year, values_from = value, values_fill = list(value = 0))
  }
  
  return(all_emissions)
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



