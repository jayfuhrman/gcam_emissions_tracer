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

final_fuel_CO2_disag <- function(all_emissions){
  
  sectors <- read_csv('input/sector_label.csv')
  elec_gen_fuels <- read_csv('input/elec_generation.csv')
  
  
  CO2_sequestration_by_tech <- rgcam::getQuery(prj, 'CO2 sequestration by tech')
  
  
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
    select(-value,-Units) -> tmp_elec
  
  
  tmp_elec%>%
    left_join(elec_gen_fuels, by = c('sector')) %>%
    mutate(fuel = if_else(fuel == 'backup_electricity','natural gas',fuel)) %>%
    group_by(scenario,region,year,fuel) %>%
    summarize(normfrac = sum(normfrac)) %>%
    ungroup() -> temp_elec_fuels
  
  
  tmp_elec%>%
    left_join(CO2_emiss_elec, by = c('scenario','region','year')) %>%
    mutate(value = value * normfrac) %>%
    left_join(elec_gen_fuels, by = c('sector')) %>%
    #filter(year >= 2005) %>%
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
  
  
  ####
  #get electricity emissions intensity per unit generated as it is a fuel for certain other transform sectors (e.g., H2 production)
  
  
  outputs_by_subsector <- rgcam::getQuery(prj, 'outputs by subsector') %>%
    filter(sector == 'electricity') %>%
    group_by(scenario,region,year,Units) %>%
    summarize(value = sum(value)) %>%
    ungroup() %>%
    rename(elec_output = value) -> elec_outputs
  
  
  elec_CO2_no_bio_final %>%
    group_by(scenario,region,year,ghg,Units) %>%
    summarize(value = sum(value)) %>%
    ungroup() %>%
    rename(elec_emiss = value) -> tot_elec_emissions
  
  elec_emiss_intensity <- tot_elec_emissions %>%
    left_join(elec_outputs %>% select(-Units),by = c('scenario','region','year')) %>%
    mutate(value = elec_emiss / elec_output,
           Units = 'MtCO2-per-EJ') %>%
    select(scenario,region,year,ghg,Units,value)
  
  
  
  
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
    #filter(year >= 2005) %>%
    group_by(scenario,region,direct,transformation,enduse,year,ghg,Units) %>%
    summarize(value = sum(value)) %>%
    ungroup() -> trn_tailpipe_CO2_for_disag
  
  
  trn_tailpipe_CO2_for_disag %>%
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
  
  
  inputs_by_subsector %>%  
    filter((sector == 'refining') & (input == 'elect_td_ind')) %>%
    left_join(elec_emiss_intensity %>%
                select(-Units) %>%
                rename(emiss_intensity = value),by = c('scenario','region','year')) %>%
    mutate(value = value * emiss_intensity * 12 / 44, #convert to MtC
           Units = 'MTC') %>%
    select(-emiss_intensity)-> refining_elec_input
  
  temp_elec_fuels %>%
    left_join(refining_elec_input,by = c('scenario','region','year')) %>%
    mutate(c_input = value * normfrac,
           sum_seq = 0,
           emiss_no_bio = c_input - sum_seq) %>%
    select(scenario,region,year,fuel,c_input,sum_seq,emiss_no_bio) -> refining_elec_input_joined #disaggregate electricity CO2 emissions for refining
  
  #refining_emiss_by_fuel_no_bio_bind <- bind_rows(refining_emiss_by_fuel_no_bio,refining_elec_input_joined) %>%
  refining_emiss_by_fuel_no_bio_bind <- bind_rows(refining_emiss_by_fuel_no_bio) %>%
    mutate(emiss_no_bio = if_else(is.na(emiss_no_bio),0,emiss_no_bio)) %>%
    group_by(scenario,region,year,fuel) %>%
    summarize(emiss_no_bio = sum(emiss_no_bio)) %>%
    ungroup()
  
  refining_emiss_by_fuel_no_bio_bind %>%
    group_by(scenario,region,year) %>%
    mutate(emiss_no_bio = if_else(is.na(emiss_no_bio),0,emiss_no_bio),
           normfrac = emiss_no_bio/sum(emiss_no_bio),
           normfrac = if_else(is.na(normfrac),0,normfrac),
           transformation = 'refining',
           ghg = 'CO2') %>%
    select(-emiss_no_bio) -> refining_emiss_by_fuel_no_bio_norm
  
  
  refining_emiss_by_fuel_no_bio_norm %>%
    left_join(trn_tailpipe_CO2_for_disag %>% filter(transformation == 'refining'),by = c('scenario','region','year','transformation','ghg')) %>%
    mutate(direct = fuel,
           value = value*normfrac) %>%
    #filter(year >= 2005) %>%
    select(-fuel,-normfrac) %>%
    bind_rows(trn_tailpipe_CO2_for_disag %>% filter(transformation == 'gas processing'))-> trn_tailpipe_CO2_disag
  
  
  
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
    ungroup() %>%
    filter(!(fuel %in% c('elect_td_ind','elect_td_trn'))) -> H2_inputs_no_elec #need to bind back electricity emissions
  
  inputs_by_subsector %>%
    filter(sector == 'H2 central production' | sector == 'H2 forecourt production',
           input %in% c('elect_td_ind','elect_td_trn')) %>%
    group_by(scenario,region,year,Units) %>%
    summarize(value = sum(value)) %>%
    ungroup() %>%
    left_join(elec_emiss_intensity %>% 
                select(-Units) %>%
                rename(emiss_intensity = value),by = c('scenario','region','year')) %>%
    mutate(c_input = value * emiss_intensity * 12 /44, #need to convert back to MtC from MtCO2e
           fuel = 'electricity') %>%
    select(-emiss_intensity,-value) -> H2_grid_elec_inputs 
  
  H2_inputs <- bind_rows(H2_inputs_no_elec)
  
  
  CO2_sequestration_by_tech %>%
    filter(sector == 'H2 central production' | sector == 'H2 forecourt production') %>%
    mutate(fuel = if_else(subsector == 'gas','natural gas',subsector)) %>%
    rename(c_seq = value)-> H2_sequestration
  
  H2_sequestration %>%
    distinct(scenario,region,year) %>%
    mutate(fuel = 'electricity',
           c_seq = 0) -> H2_elec_seq
  
  H2_sequestration <- bind_rows(H2_sequestration) #,H2_elec_seq)
  
  H2_inputs %>%
    left_join(H2_sequestration %>%
                select(-sector,-subsector,-technology),by = c('scenario','region','year','fuel')) %>%
    mutate(c_seq = if_else(is.na(c_seq),0,c_seq),
           emiss_no_bio = c_input - c_seq,
           Units = 'MTC') %>% #mutate to H2 production (the actual transformation) occurs at the end
    group_by(scenario,region,year) %>%
    mutate(normfrac = emiss_no_bio / sum(emiss_no_bio)) %>%
    select(-Units) -> H2_inputs_joined 
  
  
  H2_inputs_joined %>%
    left_join(H2_CO2_emiss, by = c('scenario','region','year')) %>%
    mutate(direct = fuel,
           value = value * normfrac) %>%
    select(-c_input,-c_seq,-emiss_no_bio,-fuel,-normfrac) %>%
    filter(direct %in% c('electricity','biomass','coal','natural gas','crude oil')) %>%
    mutate(transformation = if_else(direct == 'electricity','H2 grid electrolysis',transformation)) -> H2_CO2_emiss_disag
  
  
  H2_CO2_emiss_disag %>%
    filter(direct != 'electricity') -> H2_CO2_emiss_no_elec
  
  H2_CO2_emiss_disag %>%
    filter(direct == 'electricity') -> H2_CO2_emiss_elec
  
  
  temp_elec_fuels %>%
    left_join(H2_CO2_emiss_elec,by = c('scenario','region','year')) %>%
    filter(!is.na(Units)) %>%
    mutate(value = normfrac * value) %>%
    select(-direct,-normfrac) %>%
    rename(direct = fuel) -> H2_elec_CO2_disag
  
  #H2_CO2_emiss_disag <- bind_rows(H2_CO2_emiss_no_elec,H2_elec_CO2_disag)
  H2_CO2_emiss_disag <- bind_rows(H2_CO2_emiss_no_elec)
  
  
  #deal with all remaining CO2 emissions
  #break out H2 and refining emissions to be dealt with downstream
  
  c_containing <- c('biomass','coal','natural gas','cement limestone','crude oil','airCO2')
  
  #all_emissions %>% select(scenario,region,direct,year,transformation,enduse,ghg,value,Units) %>%
  all_emiss_no_elec_or_trn_no_H2_CO2 %>%
    filter(!(direct %in% c_containing) & ghg == 'CO2')  -> remaining_industry_CO2
  
  #all_emissions %>% select(scenario,region,direct,year,transformation,enduse,ghg,value,Units) %>%
  all_emiss_no_elec_or_trn_no_H2_CO2 %>%
    filter(!(!(direct %in% c_containing) & ghg == 'CO2')) -> all_emiss_no_elec_or_trn_no_H2_CO2_final
  
  
  ## Filter to get fuels with tailpipe/smokestack emissions (i.e., natural gas + refined liquids)
  
  c_containing_ind_fuels <- c('delivered biomass','delivered coal','delivered gas','regional natural gas','unconventional oil','wholesale gas','traditional biomass',
                              'refined liquids industrial','refined liquids enduse','limestone','district heat','process heat cement','process heat dac','airCO2')
  
  transform_ind <- c('district heat','process heat cement','process heat dac')
  
  industrial_cseq_by_fuel <- CO2_sequestration_by_tech %>%
    #    filter(sector %in% remaining_industry_CO2$enduse) %>%
    left_join(ccoef_mapping %>% rename(technology = PrimaryFuelCO2Coef.name), by = c('technology')) %>%
    select(-PrimaryFuelCO2Coef,-subsector) %>%
    rename(enduse = sector,
           c_seq = value) 
  
  #first break out tailpipe/smokestack emissions (i.e,. where direct == transformation == enduse)
  remaining_industry_CO2 %>%
    filter(direct == transformation & transformation == enduse,
           direct != 'iron and steel') -> point_source_industrial_CO2
  
  #because iron and steel intakes multiple carbon-containing fuels and sequesters a blend of their associated emissions we need to deal with it separately
  remaining_industry_CO2 %>%
    filter(direct == transformation & transformation == enduse,
           direct == 'iron and steel') -> point_source_iron_steel
  
  iron_steel_capture_coef <- 0.9 # here we assume an equal share of all fuel carbon is sequestered for iron and steel
  
  iron_steel_emiss_disag <- rgcam::getQuery(prj,'inputs by tech') %>%
    filter(sector == 'iron and steel',
           !(input %in% c('elect_td_ind','H2 enduse','scrap'))) %>%
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
    select(-normfrac)
  
  
  upstream_industrial_CO2 <-remaining_industry_CO2  %>%
    anti_join(bind_rows(point_source_industrial_CO2,point_source_iron_steel),by = c('scenario','region','year','direct','transformation','enduse')) %>%
    filter(direct != 'district heat') 
  
  
  ind_transform_en_inputs_norm <- inputs_by_subsector %>%
    filter(sector %in% transform_ind) %>%
    rename(PrimaryFuelCO2Coef.name = input,
           transformation = sector) %>%
    group_by(scenario,region,year,transformation) %>%
    mutate(en_frac = value / sum(value)) %>%
    ungroup() %>%
    left_join(ccoef_mapping,by = c('PrimaryFuelCO2Coef.name'))
  
  
  
  ind_inputs_by_subsector_temp <- inputs_by_subsector %>%
    filter(sector %in% remaining_industry_CO2$enduse,
           input %in% c_containing_ind_fuels) %>%
    mutate(input = if_else(input == 'unconventional oil','crude oil',input),
           input = if_else(input == 'traditional biomass','biomass',input)) %>%
    rename(PrimaryFuelCO2Coef.name = input) #%>%
  #%>%
  
  ind_inputs_transform_for_disag <- ind_inputs_by_subsector_temp %>%
    filter(PrimaryFuelCO2Coef.name %in% transform_ind)
  
  district_heat_coef <- 1.25
  
  ind_transform_disag <- ind_transform_en_inputs_norm %>% 
    select(-value,-subsector) %>%
    left_join(ind_inputs_transform_for_disag %>% select(-subsector,-Units) %>%
                rename(transformation = PrimaryFuelCO2Coef.name),by = c('scenario','region','year','transformation')) %>%
    mutate(c_emiss = value * en_frac * PrimaryFuelCO2Coef,
           c_emiss = if_else(fuel == 'biomass',0,c_emiss),
           c_emiss = if_else(transformation == 'district heat', c_emiss * district_heat_coef, c_emiss)) %>%
    rename(enduse = sector) %>%
    left_join(industrial_cseq_by_fuel %>% rename(transformation = enduse) %>%
                select(-Units),by = c('scenario','region','year','transformation','fuel')) %>%
    mutate(c_seq = if_else(is.na(c_seq),0,c_seq),
           c_emiss = c_emiss - c_seq) %>%
    rename(direct = fuel) %>%
    select(scenario,region,year,direct,transformation,enduse,c_emiss) %>%
    rename(value = c_emiss)
  
  
  
  ind_inputs_by_subsector_no_transform <- ind_inputs_by_subsector_temp %>%
    filter(!(PrimaryFuelCO2Coef.name %in% transform_ind)) %>%
    left_join(ccoef_mapping,by = c('PrimaryFuelCO2Coef.name')) %>%
    mutate(value = value * PrimaryFuelCO2Coef,
           value = if_else(fuel == 'biomass',0,value),
           Units = 'MTC',
           transformation = sector,
           transformation = if_else(fuel == 'refining','refining',transformation)) %>%
    rename(enduse = sector) %>%
    left_join(industrial_cseq_by_fuel,by = c('scenario','region','year','enduse','fuel','Units')) %>%
    mutate(c_seq = if_else(is.na(c_seq),0,c_seq),
           c_emiss = value - c_seq) %>%
    rename(direct = fuel) %>%
    select(scenario,region,year,direct,transformation,enduse,c_emiss)  %>%
    rename(value = c_emiss) 

  
  
  ind_point_source_norm <- bind_rows(ind_inputs_by_subsector_no_transform,ind_transform_disag %>% filter(transformation == 'process heat cement')) %>%
    group_by(scenario,region,year,enduse) %>%
    mutate(normfrac = value / sum(value)) %>%
    ungroup() %>%
    select(-value) 
  
  ind_transform_disag %>% filter(transformation != 'process heat cement') -> ind_transform_disag_final
  
  
  ind_point_source_norm %>%
    left_join(point_source_industrial_CO2 %>% select(-direct,-transformation),by = c('scenario','region','year','enduse')) %>%
    mutate(value = value * normfrac,
           value = if_else(is.na(value),0,value)) %>%
    select(-normfrac) %>% #add back iron and steel disag
    bind_rows(iron_steel_emiss_disag) -> disag_point_source_ind_CO2
  
  remaining_industry_disag <- bind_rows(disag_point_source_ind_CO2,upstream_industrial_CO2,ind_transform_disag_final) %>%
    mutate(direct = if_else(direct == 'refining' & value > 0,'crude oil',
                            if_else(direct == 'refining' & value <= 0,'biomass',direct)),
           ghg = 'CO2',
           Units = 'MTCO2e')
  
  
  
  
  ## Final processing #
  
  #Fix cement emissions to separate out process heat (fossil fuel) from limestone-related emissions  
  df <- bind_rows(all_emiss_no_elec_or_trn_no_H2_CO2_final,
                  trn_tailpipe_CO2_disag,
                  elec_CO2_no_bio_final,
                  H2_CO2_emiss_disag,
                  remaining_industry_disag) %>%
    mutate(transformation = if_else(direct == 'limestone','calcination',transformation)) %>%
    mutate(direct = if_else((direct == 'gas processing') & (ghg == 'CO2'),'natural gas',direct),#assign all direct gas processing CO2 emissions to natural gas since we're using emissions no bio query and bio constitutes a very small fraction of gas processing anyway
           direct = if_else(direct == 'H2 enduse','H2 production',direct),
           transformation = if_else(transformation == 'H2 enduse','H2 production',transformation)) %>%
    group_by(scenario,region,direct,transformation,enduse,year,ghg,Units) %>%
    summarize(value = sum(value)) %>%
    ungroup() #%>% 
  return(df)
}

direct_aggregation <- function(all_emissions){
  non_energy <- c('limestone','electricity','other industrial processes','industrial processes','adipic acid','nitric acid','solvents','waste_incineration','wastewater treatment','comm cooling','resid cooling','urban processes')
  food_agriculture <- c('Wheat','Corn','SugarCrop','SheepGoat','Beef','Dairy','FiberCrop','FodderGrass','FodderHerb','MiscCrop','OilCrop','OtherGrain','PalmFruit','Pork','Poultry','Rice','RootTuber')
  
  
  all_emissions %>%
    mutate(direct = if_else(direct %in% non_energy,'Non-energy',direct)) %>%
    mutate(direct = if_else(direct == 'refined liquids','crude oil',direct)) %>%
    mutate(direct = if_else(direct == 'traditional biomass','biomass',direct)) %>%
    mutate(direct = if_else(direct == 'unconventional oil','crude oil',direct)) %>%
    mutate(direct = if_else(direct %in% food_agriculture,'Food and agriculture',direct)) %>%
    mutate(direct = if_else(ghg == 'LUC CO2','LULUCF',direct)) %>%
    mutate(direct = if_else(ghg == 'CO2' & direct %in% c('coal','crude oil','natural gas') & value < 0,'biomass CCS',direct)) %>%
    mutate(direct = if_else(ghg == 'CO2' & direct == 'biomass' & value <= 0,'biomass CCS',direct)) %>%
    mutate(direct = if_else(ghg == 'CO2' & (direct == 'biomass CCS' | direct == 'biomass') & value >= 0,'natural gas',direct)) %>% #due to numerical precision some small amount of biomass CO2 emissions come out as small positive numbers (and vise versa for )
    mutate(transformation = if_else(transformation == 'H2 grid electrolysis','H2 production',transformation)) %>%
    mutate(direct = if_else(enduse == 'UnmanagedLand','LULUCF',direct)) %>%
    mutate(direct = if_else(enduse == 'Coal','coal',direct)) %>%
    mutate(enduse = if_else(enduse == 'ces','direct air capture',enduse)) %>%
    mutate(transformation = if_else(transformation == 'ces','direct air capture',transformation)) %>%
    group_by(scenario,region,year,direct,transformation,enduse,ghg,Units) %>%
    summarize(value = sum(value)) %>%
    ungroup() -> all_emissions
  return(all_emissions)
}

final_fuel_nonCO2_disag <- function(all_emissions) {
  
  transport <- read_csv('input/transport.csv')
  
  all_emissions %>%
    filter(ghg %in% c('CH4','N2O') & direct == transformation & transformation == enduse) -> combustion_non_CO2_emiss
  
  all_emissions %>%
    filter(!(ghg %in% c('CH4','N2O') & direct == transformation & transformation == enduse)) -> all_other_emiss  #for mergeback
  
  
  #temporary until we can query directly on the cluster
  nonCO2_emissions_by_tech <- rgcam::getQuery(prj,'nonCO2 emissions by tech')
  
  #nonCO2_emissions_by_tech <- read_csv('nonCO2_emissions_by_tech.csv') %>% 
  #  pivot_longer(cols = '1990':'2100',names_to = 'year') %>%
  #  mutate(scenario = gsub("(.*),.*", "\\1", scenario)) %>%
  #  filter(sector != 'UnmanagedLand')
  
  
  nonCO2_combustion_emissions_by_tech <- nonCO2_emissions_by_tech %>%
    filter(ghg %in% c('CH4','N2O')) %>%
    mutate(sector = if_else(subsector %in% transport$transportation_subsector,subsector,sector),
           fuel = if_else(technology %in% c('Liquids','NG','Coal','biomass'),technology,subsector),
           fuel = if_else(fuel %in% c('gas','NG'),'natural gas',fuel),
           fuel = if_else(fuel %in% c('Liquids'),'refined liquids',fuel)) %>%
    filter(!(sector %in% c('H2 central production','H2 forecourt production','electricity','refining','district heat'))) %>% #filter out transformation sector as these will be dealt with separately
    group_by(scenario,region,sector,ghg,year) %>%
    mutate(normfrac = value / sum(value)) %>%
    ungroup() %>%
    mutate(year = as.numeric(year),
           normfrac = if_else(is.na(normfrac),0,normfrac)) %>%
    rename(enduse = sector)
  
  
  
  nonCO2_combustion_emissions_by_tech %>%
    select(-Units,-technology,-subsector,-value) %>%
    left_join(combustion_non_CO2_emiss, by = c('scenario','region','year','enduse','ghg')) %>%
    filter(Units != is.na(Units)) %>%
    mutate(value = value * normfrac,
           direct = fuel) %>%
    select(-normfrac,-fuel) -> combustion_non_CO2_emiss_disag
  
  
  other_emiss_transform_for_disag <- all_other_emiss %>%
    filter(direct %in% c('H2 production','electricity','refining','district heat') & ghg %in% c('CH4','N2O')) %>%
    rename(sector = direct)
  
  all_other_emiss_no_transform_combustion <- all_other_emiss %>%
    filter(!(direct %in% c('H2 production','electricity','refining','district heat') & ghg %in% c('CH4','N2O')))
  
  
  
  nonCO2_emissions_by_tech_transform <- nonCO2_emissions_by_tech %>%
    #    rename(ghg = GHG) %>%
    filter(ghg %in% c('CH4','N2O') & sector %in% c('H2 central production','H2 forecourt production','electricity','district heat','refining')) %>%
    mutate(sector = if_else(sector %in% c('H2 central production','H2 forecourt production'),'H2 production',sector),
           fuel = if_else(subsector %in% c('biomass','biomass liquids'),'biomass',
                          if_else(subsector %in% c('coal','coal to liquids'),'coal',
                                  if_else(subsector %in% c('gas','gas to liquids'),'natural gas',subsector)))) %>%
    group_by(scenario,region,sector,year,fuel,ghg) %>%
    summarize(value = sum(value)) %>%
    ungroup() %>%
    group_by(scenario,region,sector,year,ghg) %>%
    mutate(normfrac = value / sum(value)) %>%
    select(-value) %>%
    ungroup()
  
  nonCO2_emissions_by_tech_transform$year <- as.numeric(nonCO2_emissions_by_tech_transform$year)
  
  nonCO2_emissions_by_tech_transform_disag<- nonCO2_emissions_by_tech_transform %>%
    left_join(other_emiss_transform_for_disag,by = c('scenario','region','sector','year','ghg')) %>%
    filter(!is.na(normfrac)) %>%
    mutate(value = value * normfrac,
           direct = fuel) %>%
    select(scenario,region,year,direct,transformation,enduse,ghg,value,Units)
  
  
  
  all_other_emiss_disag <- bind_rows(nonCO2_emissions_by_tech_transform_disag,all_other_emiss_no_transform_combustion)
  all_emiss_w_nonCO2_comb_disag <- bind_rows(combustion_non_CO2_emiss_disag,all_other_emiss_disag) 
  
  all_emiss_w_nonCO2_comb_disag  %>%
    select(scenario,region,direct,transformation,enduse,ghg,year,value,Units) %>%
    group_by(scenario,region,direct,transformation,enduse,ghg,year,Units) %>%
    summarize(value = sum(value)) %>% #sum combustion and resource extraction emissions for some sectors
    ungroup() %>%
    arrange(year) %>%
    select(scenario,region,year,direct,transformation,enduse,ghg,value,Units) -> all_emiss_w_nonCO2_comb_disag_distinct
  
  return(all_emiss_w_nonCO2_comb_disag_distinct)
}

# Function to distribute co2 sequestration same as fuel tracer
co2_sequestration_distributor <- function(prj, fuel_tracing, primary_map, WIDE_FORMAT = TRUE){
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
  enduse_in_trans <- fuel_tracing %>%
    # filter(region == "China", year == 2050) %>% # TEMPORARY
    group_by(scenario, region, year, transformation, enduse) %>%
    summarise(value = sum(value)) %>%
    group_by(scenario, region, year, transformation) %>%
    mutate(ratio_enduse_in_trans = value / sum(value)) %>%
    ungroup() %>%
    filter(transformation %in% transf_sectors) %>%
    select(-value, enduse_map = enduse, enduse = transformation)
  
  # Fix iron and steel, expand direct
  ironsteel_inputs <- getQuery(prj, "inputs by tech") %>%
    # filter(region == "China", year == "2050") %>% # TEMPORARY
    filter(sector == "iron and steel",
           str_detect(technology, "CCS")) %>%
    # Get rid of electricity and scrap
    filter(!(input %in% c("elect_td_ind", "scrap"))) %>%
    # Fix input names
    mutate(input = str_replace(input, "delivered coal", "coal"),
           input = str_replace(input, "refined liquids industrial", "refined liquids"),
           input = str_replace(input, "wholesale gas", "gas")) %>%
    # Get percentage of input in total
    group_by(scenario, region, year, sector, subsector, technology) %>%
    mutate(ratio_input_in_tech = value / sum(value)) %>%
    ungroup() %>%
    select(scenario, region, year, technology, input, ratio_input_in_tech) 
  
  # Expand iron and steel from co2 sequestration
  iron_steel_replace <- getQuery(prj, "CO2 sequestration by tech") %>%
    # filter(region == "China", year == "2050") %>%
    filter(sector == "iron and steel") %>%
    left_join(ironsteel_inputs, by = c("scenario", "region", "year", "technology")) %>%
    mutate(input = if_else(is.na(input), "crude oil", input),
           ratio_input_in_tech = if_else(is.na(ratio_input_in_tech) & input == "crude oil", 
                                         1, ratio_input_in_tech),
           value = value * ratio_input_in_tech) %>%
    select(-subsector, -ratio_input_in_tech, subsector = input)
  
  # Carbon sequestration by subsector
  seq <- getQuery(prj, "CO2 sequestration by tech") %>%
    # filter(region == "China", year == "2050") %>% # TEMPORARY
    filter(sector != "iron and steel", year > 1990) %>%
    bind_rows(iron_steel_replace) %>%
    mutate(sector = if_else(str_detect(sector, "elec_"), "electricity", sector)) %>%
    group_by(Units, scenario, region, sector, subsector, year) %>%
    summarise(value = sum(value)) %>%
    ungroup() 
  
  seq2 <- seq %>%
    left_join(primary_map, by = "subsector") %>%
    mutate(transformation = sector,
           transformation = str_replace_all(transformation, 
                                            "H2 central production",
                                            "H2 enduse")) %>%
    left_join(primary_in_trans, 
              by = c("scenario", "region", "year", "primary" = "trans_map")) %>%
    # If join occurred, enduse should be old transform,
    # transform should be old primary
    # primary should be primary_map
    mutate(enduse = transformation,
           transformation = if_else(!is.na(ratio_primary_in_trans) & !(transformation %in% transf_sectors), 
                                    primary, transformation),
           primary = if_else(!is.na(ratio_primary_in_trans), 
                             primary_map, primary)) %>%
    # Multiply value by ratio of primary in transformation
    mutate(ratio_primary_in_trans = 
             if_else(is.na(ratio_primary_in_trans), 1, ratio_primary_in_trans),
           value = value * ratio_primary_in_trans) %>%
    select(-primary_map, -ratio_primary_in_trans)
  
  # Split out enduses that are actually transformations
  seq3 <- seq2 %>%
    left_join(enduse_in_trans, by = c("scenario", "region", "year", "enduse")) %>%
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
    mutate(direct = str_replace_all(primary, "traded unconventional oil", "crude oil"),
           direct = str_replace_all(direct, "total biomass", "biomass"),
           direct = if_else(direct == "atmospheric CO2","CO2 removal",direct),
           enduse = if_else(enduse == "ces","direct air capture",enduse),
           enduse = if_else(enduse == "process heat dac","direct air capture",enduse),
           transformation = if_else(transformation == 'ces','direct air capture',transformation)) %>%
    group_by(scenario, region, direct, transformation, enduse, year, ghg, Units) %>%
    summarise(value = sum(value)) %>%
    ungroup()
  
  global <- seq3 %>%
    group_by(scenario, year, direct, transformation, enduse, ghg, Units) %>%
    summarise(value = sum(value)) %>%
    ungroup() %>%
    mutate(region = "Global")
  
  seq3 <- bind_rows(seq3, global) %>% 
    filter(year >= 2005) %>%
    mutate(value = if_else(is.na(value),0,value))

  if (WIDE_FORMAT){
    seq3 <- seq3 %>%
      arrange(year) %>%
      pivot_wider(names_from = year, values_from = value) %>%
      arrange(region, direct) 
    
    seq3[is.na(seq3)] <- 0
  }
  return(seq3)
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
  
  #write.csv(all_emissions,'all_emissions.csv')
  #final fuel processing
  all_emissions <- final_fuel_CO2_disag(all_emissions)

  all_emissions <- final_fuel_nonCO2_disag(all_emissions) #disaggregate nonCO2 combustion emissions
  
  all_emissions <- direct_aggregation(all_emissions)
  
  global <- all_emissions %>%
    group_by(scenario, year, direct, transformation, enduse, ghg, Units) %>%
    summarise(value = sum(value)) %>%
    ungroup() %>%
    mutate(region = "Global")
  
  all_emissions <- bind_rows(all_emissions, global) %>% 
    filter(year >= 2005) %>%
    mutate(value = if_else(is.na(value),0,value))
  
  original_emissions <- sum(filter(ghg, year > 1990)$value) + 
    sum(filter(LUC_emissions, year > 1990)$value)
  #print(paste0('original emissions: ',original_emissions))

  calculated_emissions <- filter(all_emissions, region != "Global")$value %>% sum(na.rm = T)
  #print(paste0('calculated emissions: ',calculated_emissions))
  
  
  if (round(original_emissions - calculated_emissions,0) != 0){
    print("Total emissions from 1990 to 2100 do NOT match.")
    print("percent difference between raw GCAM output data and disaggregated emissions is:")
    print(100*(original_emissions - calculated_emissions)/original_emissions)
    #write.csv(ghg %>% filter(year > 1990),'original_ghg.csv')
    #write.csv(LUC_emissions %>% filter(year > 1990),'original_LUC.csv')
    #write.csv(filter(all_emissions, region != "Global"),'calculated_emissions.csv')
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



