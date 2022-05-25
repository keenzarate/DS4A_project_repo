import pandas as pd

# set to show all the columns of the data being loaded
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

countries = ['United States', 'India', 'China', 'Brazil']


# build small function to clean data from same source -- 

def clean_world_bank(countries:list, data_path):

    dt = pd.DataFrame(pd.read_csv(data_path))

    sub_dt = dt[dt['Country Name'].isin(countries)]

    #clean columns
    sub_dt.columns = sub_dt.columns.str.lower()

    # add underscore 
    sub_dt.columns = sub_dt.columns.str.replace(' ', '_')

    # make data long
    sub_dt_long = pd.melt(sub_dt, id_vars = ['series_name', 'series_code', 'country_name', 'country_code'], value_name = 'cereal_prod_(tons)', var_name = 'year')

    # clean year column n
    sub_dt_long['year'] = sub_dt_long['year'].str.replace(r'_.*', '')

    # sort by country and year 
    sub_dt_final = sub_dt_long.sort_values(['country_name', 'year'])

    return sub_dt_final


#### change the data path to something else... pull from raw data in S3

# clean cereal data
cereal = clean_world_bank(countries, 'https://raw.githubusercontent.com/keenzarate/DS4A_project_repo/main/data/cereal.csv?token=GHSAT0AAAAAABTRESMJ372THZWUWIAFNEVCYTB5CQQ')

# load c02 emissions data 
carbon_emissions =  clean_world_bank(countries, 'https://raw.githubusercontent.com/keenzarate/DS4A_project_repo/main/data/co2_emissions.csv?token=GHSAT0AAAAAABTRESMIIVVGTWRNZGQETV5MYTB5C4Q')

# load and clean rainfall data
rainfall_dt = clean_world_bank(countries, 'https://raw.githubusercontent.com/keenzarate/DS4A_project_repo/main/data/precipitation.csv?token=GHSAT0AAAAAABTRESMIPXCNRV4PDPQIA44YYTB5DFQ')