import pandas as pd
import boto3

# set to show all the columns of the data being loaded
pd.set_option('display.max_columns', None)

##########################################
###  WORLD BANK DATA CLEANING FUNCTION ###
##########################################

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


##########################################
###     NOAA DATA CLEANING FUNCTION    ###
##########################################

# create function to clean temperature and precipitation data 
def clean_temp_precip(path_usa, path_bra, path_chn, path_ind, measure_name):
    
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

    # sort col so we can stack it later 
    stacked_data.sort_index(axis=1)

    return stacked_data

#################################
### CLEAN TEMPERATURE DATA    ###
#################################

# dataset paths
temp_usa = "/Users/keenzarate/Documents/data/ds4a/tas_timeseries_annual_cru_1901-2020_USA.csv"
temp_bra = "/Users/keenzarate/Documents/data/ds4a/tas_timeseries_annual_cru_1901-2020_BRA.csv"
temp_chn = "/Users/keenzarate/Documents/data/ds4a/tas_timeseries_annual_cru_1901-2020_CHN.csv"
temp_ind = "/Users/keenzarate/Documents/data/ds4a/tas_timeseries_annual_cru_1901-2020_IND.csv"

# clean mean temperature data 
clean_mean_temp = clean_temp_precip(temp_usa, temp_bra, temp_chn, temp_ind, 'mean_temperature_tons')

#################################
### CLEAN PRECIPITATION DATA ###
#################################

# load the dataset skip the extra headers cause we dont need it.
precip_usa = "/Users/keenzarate/Documents/data/ds4a/pr_timeseries_annual_cru_1901-2020_USA.csv"
precip_bra = "/Users/keenzarate/Documents/data/ds4a/pr_timeseries_annual_cru_1901-2020_BRA.csv"
precip_chn = "/Users/keenzarate/Documents/data/ds4a/pr_timeseries_annual_cru_1901-2020_CHN.csv"
precip_ind = "/Users/keenzarate/Documents/data/ds4a/pr_timeseries_annual_cru_1901-2020_IND.csv"

# clean precipitation data
clean_precip = clean_temp_precip(precip_usa, precip_bra, precip_chn, precip_ind, 'precipitation_celsius')



#################################
###     CLEAN CEREAL DATA     ###
#################################

# list of countries we want to subset to 
countries = ['United States', 'India', 'China', 'Brazil']

# clean cereal data
cereal = clean_world_bank(countries, '/Users/keenzarate/Documents/data/ds4a/cereal_production_world_bank.csv', 'cereal_prod_tons')


#######################################
###   CLEAN C02 EMISSION DATASET    ###
#######################################

# load c02 emissions data 
# data downloaded from https://ourworldindata.org/co2-emissions
carbon_emissions =  pd.read_csv('/Users/keenzarate/Documents/data/ds4a/annual-co2-emissions-per-country.csv')

# rename columns
carbon_emissions = carbon_emissions.rename({'Entity': 'location', 'Year': 'year', 'Annual CO2 emissions': 'annual_c02_emissions'}, axis = 1)

# drop column we don't need so we can stack it with the rest of the weather data 
sub_carbon_emissions = carbon_emissions.drop(['Code'], axis = 1)

# rename country column to location 
sub_carbon_emissions['country'] = sub_carbon_emissions['location']

# make the data long so we can stack later 
long_carbon_emissions = pd.DataFrame(pd.melt(sub_carbon_emissions, id_vars = ['year', 'country', 'location'], value_name = 'measure_value', var_name = 'measure_name'))

# sort the column again 
long_carbon_emissions.sort_index(axis=1)

# stack mean temperature, carbon emissions, precipitation 
stacked_all = pd.concat([long_carbon_emissions, clean_mean_temp, clean_precip])

# make the stacked data wide 
stacked_wide = stacked_all.pivot(index=['location', 'year'], columns='measure_name', values='measure_value').reset_index().rename_axis(None, axis=1)



#########################
###   FINAL DATASETS  ###
#########################

# final weather data stacked 
final_weather_data = stacked_wide

# final cereal data 
final_cereal_data = cereal.pivot(index=['location', 'year'], columns='measure_name', values='measure_value').reset_index().rename_axis(None, axis=1)


#############################
###   WRITE DATA TO CSV  ###
#############################

final_cereal_data.to_csv('/Users/keenzarate/Documents/data/ds4a/clean_cereal_data.csv',   index = False)
final_weather_data.to_csv('/Users/keenzarate/Documents/data/ds4a/clean_weather_data.csv', index = False)

# data 
cereal_data = '/Users/keenzarate/Documents/data/ds4a/clean_cereal_data.csv'
weather_data = '/Users/keenzarate/Documents/data/ds4a/clean_weather_data.csv'


#############################
###   LOAD DATA TO S3     ###
#############################

# create a client to access resources with
s3_client = boto3.client('s3', aws_access_key_id = "null", aws_secret_access_key = "null")

# upload the applications to the bucket created earlier
# we have a try/except clause to see if it fails
try:
    s3_client.upload_file(cereal_data, 'null' ,"clean/clean_cereal_data.csv")
    print('File successfully loaded')
except Exception as e:
    print(f'{e} Client error: file could not be uploaded to S3')

# load weather data to s3
try:
    s3_client.upload_file(weather_data, 'null' ,"clean/clean_weather_data.csv")
    print('File successfully loaded')
except Exception as e:
    print(f'{e} Client error: file could not be uploaded to S3')