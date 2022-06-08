from re import S
import pandas as pd

#######################
## world cities data ##
########################

world_cities = pd.read_csv("/Users/keenzarate/Documents/data/ds4a/new_raw_data/worldcities.csv")

# subset data to only countries we want 
sub_world_cities = world_cities[world_cities['country'].isin(['China', 'India','United States', 'Brazil'])]

# aggregate -- count number 
ven = sub_world_cities.groupby(['country'])['city'].nunique().reset_index(name='number_of_cities')

sum_pop = sub_world_cities.groupby(['country'])['population'].sum().reset_index(name='total_population')

country_info = pd.merge(ven, sum_pop, on = 'country')

######################
### cereal data    ###
######################

raw_cereal = pd.read_csv("/Users/keenzarate/Documents/data/ds4a/new_raw_data/cereal_new.csv")

# clean up column 
raw_cereal.columns = raw_cereal.columns.str.replace(' ','_')
raw_cereal.columns = raw_cereal.columns.str.lower()

# drop column we don't need so we can stack it with the rest of the weather data 
sub_raw_cereal = raw_cereal.drop(['year_code', 'flag', 'flag_description', 'domain_code', 'domain'], axis = 1)

# change usa to us to match other dataset 
sub_raw_cereal['area'] = sub_raw_cereal['area'].replace(['United States of America'],'United States')

# rename columns 
final_cereal_data = sub_raw_cereal.rename({'area_code_(fao)': 'country_id', 'area': 'country','item_code_(fao)': 'item_id'}, axis = 1)

######################
### greenhouse gas ###
######################

# read dataset
greenhouse_gas = pd.read_csv("/Users/keenzarate/Documents/data/ds4a/new_raw_data/historical_emissions.csv")

greenhouse_gas.columns = greenhouse_gas.columns.str.replace(' ','_')
greenhouse_gas.columns = greenhouse_gas.columns.str.lower()

long_greenhouse_gas = pd.DataFrame(pd.melt(greenhouse_gas, id_vars = ['country', 'data_source', 'sector', 'gas', 'unit'], value_name = 'measure_value', var_name = 'year'))

# drop column we don't need so we can stack it with the rest of the weather data 
final_greenhouse_gas = long_greenhouse_gas.drop(['data_source'], axis = 1)


##########################################
###     NOAA DATA CLEANING FUNCTION    ###
##########################################

# create function to clean temperature and precipitation data 
def clean_temp_precip(path_usa, path_bra, path_chn, path_ind, measure_name, unit):
    
    # read csv files 
    read_usa = pd.read_csv(path_usa, skiprows = 1)
    read_bra = pd.read_csv(path_bra, skiprows = 1)
    read_chn= pd.read_csv(path_chn,  skiprows = 1)
    read_ind = pd.read_csv(path_ind, skiprows = 1)

    # add a column country so we can have an indicator when data is stacked in the end 
    read_usa['country'] = 'United States'
    read_bra['country'] = 'Brazil'
    read_chn['country'] = 'China'
    read_ind['country'] = 'India'

    # update the unnamed column to "year" to make it more clear
    read_usa.rename( columns={'Unnamed: 0' :'year'}, inplace=True )
    read_bra.rename( columns={'Unnamed: 0' :'year'}, inplace=True )
    read_chn.rename( columns={'Unnamed: 0' :'year'}, inplace=True )
    read_ind.rename( columns={'Unnamed: 0' :'year'}, inplace=True )

    # make the data long so we can stack it later on our other datasets 
    long_usa = pd.DataFrame(pd.melt(read_usa, id_vars = ['year', 'country'], value_name = 'measure_value', var_name = 'location'))
    long_bra = pd.DataFrame(pd.melt(read_bra, id_vars = ['year', 'country'], value_name = 'measure_value', var_name = 'location'))
    long_chn = pd.DataFrame(pd.melt(read_chn, id_vars = ['year', 'country'], value_name = 'measure_value', var_name = 'location'))
    long_ind = pd.DataFrame(pd.melt(read_ind, id_vars = ['year', 'country'], value_name = 'measure_value', var_name = 'location'))

    # stack all of the individual country dataframes 
    stacked_data = pd.concat([long_usa, long_bra, long_chn, long_ind])

    # add a column measure name so we know what this data is about 
    stacked_data['measure_name'] = measure_name

    stacked_data['unit'] = unit

    # sort col so we can stack it later 
    stacked_data.sort_index(axis=1)

    return stacked_data

#################################
### CLEAN TEMPERATURE DATA    ###
#################################

# dataset paths
temp_usa = "/Users/keenzarate/Documents/data/ds4a/new_raw_data/tas_timeseries_annual_cru_1901-2020_USA.csv"
temp_bra = "/Users/keenzarate/Documents/data/ds4a/new_raw_data/tas_timeseries_annual_cru_1901-2020_BRA.csv"
temp_chn = "/Users/keenzarate/Documents/data/ds4a/new_raw_data/tas_timeseries_annual_cru_1901-2020_CHN.csv"
temp_ind = "/Users/keenzarate/Documents/data/ds4a/new_raw_data/tas_timeseries_annual_cru_1901-2020_IND.csv"

# clean mean temperature data 
clean_mean_temp = clean_temp_precip(temp_usa, temp_bra, temp_chn, temp_ind, 'Mean Temperature', 'Tonnes')


#################################
### CLEAN PRECIPITATION DATA ###
#################################

# load the dataset skip the extra headers cause we dont need it.
precip_usa = "/Users/keenzarate/Documents/data/ds4a/new_raw_data/pr_timeseries_annual_cru_1901-2020_USA.csv"
precip_bra = "/Users/keenzarate/Documents/data/ds4a/new_raw_data/pr_timeseries_annual_cru_1901-2020_BRA.csv"
precip_chn = "/Users/keenzarate/Documents/data/ds4a/new_raw_data/pr_timeseries_annual_cru_1901-2020_CHN.csv"
precip_ind = "/Users/keenzarate/Documents/data/ds4a/new_raw_data/pr_timeseries_annual_cru_1901-2020_IND.csv"


test = pd.read_csv(precip_usa)

# clean precipitation data
clean_precip = clean_temp_precip(precip_usa, precip_bra, precip_chn, precip_ind, 'Precipitation', 'Celcius')


clean_precip.loc[((clean_precip['location'] == 'Brazil') & (clean_precip['year'] == 2018))]


# stack mean temperature, carbon emissions, precipitation 
final_temp_precip = pd.concat([clean_mean_temp, clean_precip])


final_cereal_data.to_csv('/Users/keenzarate/Documents/data/ds4a/clean_data/clean_cereal_data.csv',   index = False)
final_temp_precip.to_csv('/Users/keenzarate/Documents/data/ds4a/clean_data/clean_climate_indicator_data.csv', index = False)
final_greenhouse_gas.to_csv('/Users/keenzarate/Documents/data/ds4a/clean_data/clean_greenhouse_data.csv', index = False)
country_info.to_csv('/Users/keenzarate/Documents/data/ds4a/clean_data/clean_country_info.csv', index = False)
