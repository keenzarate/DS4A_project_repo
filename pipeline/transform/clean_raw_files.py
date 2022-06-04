import pandas as pd
from sqlalchemy import null 

# clean MEAN TEMPERATURE data 

# read dataset 
temp_usa = pd.read_csv("/Users/keenzarate/Documents/data/ds4a/tas_timeseries_annual_cru_1901-2020_USA.csv", skiprows = 1)
temp_bra = pd.read_csv("/Users/keenzarate/Documents/data/ds4a/tas_timeseries_annual_cru_1901-2020_BRA.csv", skiprows = 1)
temp_chn= pd.read_csv("/Users/keenzarate/Documents/data/ds4a/tas_timeseries_annual_cru_1901-2020_CHN.csv",  skiprows = 1)
temp_ind = pd.read_csv("/Users/keenzarate/Documents/data/ds4a/tas_timeseries_annual_cru_1901-2020_IND.csv", skiprows = 1)

# add a column country so we can have an indicator when data is stacked in the end 
temp_usa['country'] = 'United States'
temp_bra['country'] = 'Brazil'
temp_chn['country'] = 'China'
temp_ind['country'] = 'India'

# update the unnamed column to "year" to make it more clear
temp_usa.rename( columns={'Unnamed: 0' :'year'}, inplace=True )
temp_bra.rename( columns={'Unnamed: 0' :'year'}, inplace=True )
temp_chn.rename( columns={'Unnamed: 0' :'year'}, inplace=True )
temp_ind.rename( columns={'Unnamed: 0' :'year'}, inplace=True )

# make the data long so we can stack it later on our other datasets 
long_usa = pd.DataFrame(pd.melt(temp_usa, id_vars = ['year', 'country'], value_name = 'measure_value', var_name = 'location'))
long_bra = pd.DataFrame(pd.melt(temp_bra, id_vars = ['year', 'country'], value_name = 'measure_value', var_name = 'location'))
long_chn = pd.DataFrame(pd.melt(temp_chn, id_vars = ['year', 'country'], value_name = 'measure_value', var_name = 'location'))
long_ind = pd.DataFrame(pd.melt(temp_ind, id_vars = ['year', 'country'], value_name = 'measure_value', var_name = 'location'))

# stack all of the individual country dataframes 
stacked_mean_temp = pd.concat([long_usa, long_bra, long_chn, long_ind])

# add a column measure name so we know what this data is about 
stacked_mean_temp['measure_name'] = 'mean_temperature'

# sort col so we can stack it later 
stacked_mean_temp.sort_index(axis=1)


# CLEAN PRECIPATION DATA

# the same format as the mean temperature one 

# load the dataset skip the extra headers cause we dont need it.
precip_usa = pd.read_csv("/Users/keenzarate/Documents/data/ds4a/pr_timeseries_annual_cru_1901-2020_USA.csv", skiprows = 1)
precip_bra = pd.read_csv("/Users/keenzarate/Documents/data/ds4a/pr_timeseries_annual_cru_1901-2020_BRA.csv", skiprows = 1)
precip_chn= pd.read_csv("/Users/keenzarate/Documents/data/ds4a/pr_timeseries_annual_cru_1901-2020_CHN.csv",  skiprows = 1)
precip_ind = pd.read_csv("/Users/keenzarate/Documents/data/ds4a/pr_timeseries_annual_cru_1901-2020_IND.csv", skiprows = 1)

# rename the unnamed column to year so we know what it is about 
precip_usa.rename( columns={'Unnamed: 0' :'year'}, inplace=True )
precip_bra.rename( columns={'Unnamed: 0' :'year'}, inplace=True )
precip_chn.rename( columns={'Unnamed: 0' :'year'}, inplace=True )
precip_ind.rename( columns={'Unnamed: 0' :'year'}, inplace=True )

# add a column called country so we have an indicator 
precip_usa['country'] = 'United States'
precip_bra['country'] = 'Brazil'
precip_chn['country'] = 'China'
precip_ind['country'] = 'India'

# make the data long so we can stack with the mean dataset 
long_usa_precip = pd.DataFrame(pd.melt(precip_usa, id_vars = ['year', 'country'], value_name = 'measure_value', var_name = 'location'))
long_bra_precip = pd.DataFrame(pd.melt(precip_bra, id_vars = ['year', 'country'], value_name = 'measure_value', var_name = 'location'))
long_chn_precip = pd.DataFrame(pd.melt(precip_chn, id_vars = ['year', 'country'], value_name = 'measure_value', var_name = 'location'))
long_ind_precip = pd.DataFrame(pd.melt(precip_ind, id_vars = ['year', 'country'], value_name = 'measure_value', var_name = 'location'))

# stack everything into one dataframe 
stacked_precip = pd.concat([long_usa_precip, long_bra_precip, long_chn_precip, long_ind_precip])

# add a column measure name 
stacked_precip['measure_name'] = 'precipitation_celsius'

# sort the column again 
stacked_precip.sort_index(axis=1)


# CLEAN WORLD BANK DATA 

# set to show all the columns of the data being loaded
pd.set_option('display.max_columns', None)

# list of countries we want to subset to 
countries = ['United States', 'India', 'China', 'Brazil']


# build small function to clean data from same source -- 
def clean_world_bank(countries:list, data_path, measure_name):

    # read the data 
    dt = pd.DataFrame(pd.read_csv(data_path))

    sub_dt = dt[dt['Country Name'].isin(countries)]

    #clean columns
    sub_dt.columns = sub_dt.columns.str.lower()

    # add underscore 
    sub_dt.columns = sub_dt.columns.str.replace(' ', '_')

    # make data long
    sub_dt_long = pd.melt(sub_dt, id_vars = ['series_name', 'series_code', 'country_name', 'country_code'], value_name = 'measure_value', var_name = 'year')

    # clean year column n
    sub_dt_long['year'] = sub_dt_long['year'].str.replace(r'_.*', '')

    # rename the column to "location"
    sub_dt_long = sub_dt_long.rename({'country_name': 'location'}, axis = 1)

    sub_dt_long['measure_name'] = measure_name

    final_dt = sub_dt_long.drop(['series_name', 'series_code', 'country_code'], axis = 1)

    # sort by country and year 
    final_dt = final_dt.sort_values(['location', 'year'])

    return final_dt


# clean cereal data
cereal = clean_world_bank(countries, '/Users/keenzarate/Documents/data/ds4a/cereal_production_world_bank.csv', 'cereal_prod_tons')



# clean CO2 emission data 

# load c02 emissions data 
# data download from https://ourworldindata.org/co2-emissions
carbon_emissions =  pd.read_csv('/Users/keenzarate/Documents/data/ds4a/annual-co2-emissions-per-country.csv')

carbon_emissions = carbon_emissions.rename({'Entity': 'location', 'Year': 'year', 'Annual CO2 emissions': 'annual_c02_emissions'}, axis = 1)

sub_carbon_emissions = carbon_emissions.drop(['Code'], axis = 1)

sub_carbon_emissions['country'] = sub_carbon_emissions['location']

long_carbon_emissions = pd.DataFrame(pd.melt(sub_carbon_emissions, id_vars = ['year', 'country', 'location'], value_name = 'measure_value', var_name = 'measure_name'))

# sort the column again 
long_carbon_emissions.sort_index(axis=1)

# stack mean temperature, carbon emissions, precipitation 
stacked_all = pd.concat([long_carbon_emissions, stacked_mean_temp, stacked_precip])

# make the data wide 
stacked_wide = stacked_all.pivot(index=['location', 'year'], columns='measure_name', values='measure_value').reset_index().rename_axis(None, axis=1)




# final data output 

# final weather data stacked 
final_weather_data = stacked_wide


# final cereal data 
final_cereal_data = cereal.pivot(index=['location', 'year'], columns='measure_name', values='measure_value').reset_index().rename_axis(None, axis=1)


final_cereal_data.to_csv('/Users/keenzarate/Documents/data/ds4a/clean_cereal_data.csv')
final_weather_data.to_csv('/Users/keenzarate/Documents/data/ds4a/clean_weather_data.csv')