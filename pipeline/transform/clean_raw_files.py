import pandas as pd 

# clean MEAN TEMPERATURE data 

temp_usa = pd.read_csv("/Users/keenzarate/Documents/data/ds4a/tas_timeseries_annual_cru_1901-2020_USA.csv", skiprows = 1)
temp_bra = pd.read_csv("/Users/keenzarate/Documents/data/ds4a/tas_timeseries_annual_cru_1901-2020_BRA.csv", skiprows = 1)
temp_chn= pd.read_csv("/Users/keenzarate/Documents/data/ds4a/tas_timeseries_annual_cru_1901-2020_CHN.csv",  skiprows = 1)
temp_ind = pd.read_csv("/Users/keenzarate/Documents/data/ds4a/tas_timeseries_annual_cru_1901-2020_IND.csv", skiprows = 1)

temp_usa.rename( columns={'Unnamed: 0' :'year'}, inplace=True )
temp_bra.rename( columns={'Unnamed: 0' :'year'}, inplace=True )
temp_chn.rename( columns={'Unnamed: 0' :'year'}, inplace=True )
temp_ind.rename( columns={'Unnamed: 0' :'year'}, inplace=True )

# make the data long 
long_usa = pd.DataFrame(pd.melt(temp_usa, id_vars = ['year'], value_name = 'measure_value', var_name = 'location'))
long_bra = pd.DataFrame(pd.melt(temp_bra, id_vars = ['year'], value_name = 'measure_value', var_name = 'location'))
long_chn = pd.DataFrame(pd.melt(temp_chn, id_vars = ['year'], value_name = 'measure_value', var_name = 'location'))
long_ind = pd.DataFrame(pd.melt(temp_ind, id_vars = ['year'], value_name = 'measure_value', var_name = 'location'))

# stack everything into one dataframe 
stacked_mean_temp = pd.concat([long_usa, long_bra, long_chn, long_ind])

# add a column measure name 
stacked_mean_temp['measure_name'] = 'mean_temperature'

stacked_mean_temp.sort_index(axis=1)

# CLEAN WORLD BANK DATA 

# set to show all the columns of the data being loaded
pd.set_option('display.max_columns', None)

# list of countries we want to subset to 
countries = ['United States', 'India', 'China', 'Brazil']


# build small function to clean data from same source -- 
def clean_world_bank(countries:list, data_path, measure_name):

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

    sub_dt_long = sub_dt_long.rename({'country_name': 'location'}, axis = 1)

    sub_dt_long['measure_name'] = measure_name

    final_dt = sub_dt_long.drop(['series_name', 'series_code', 'country_code'], axis = 1)

    # sort by country and year 
    final_dt = final_dt.sort_values(['location', 'year'])

    return final_dt


# clean cereal data
cereal = clean_world_bank(countries, '/Users/keenzarate/Documents/data/ds4a/cereal_production_world_bank.csv', 'cereal_prod_tons')

# load c02 emissions data 
carbon_emissions =  clean_world_bank(countries, '/Users/keenzarate/Documents/data/ds4a/co2_emissions_world_bank.csv', 'co2_emmissions')

# order column to match with mean temperature
carbon_emissions.sort_index(axis = 1)


# testing



# add precipitation data here:






# stack mean temperature, carbon emissions, precipitation 
stacked_all = pd.concat([carbon_emissions, stacked_mean_temp])