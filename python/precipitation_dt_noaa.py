import pandas as pd 
import requests as r
import io
import csv
import tarfile
import re
import datetime

pd.set_option('display.max_colwidth', None)

# info: https://www.ncei.noaa.gov/products/land-based-station/global-historical-climatology-network-monthly (also has temperature data)
# data pulled from 'https://www.ncei.noaa.gov/data/ghcnm/v4beta/archive/'
# documentation of this lives here; https://www.ncei.noaa.gov/data/ghcnm/v4beta/doc/ (readme and the inventory file)

#documentation : https://www.ncei.noaa.gov/products/land-based-station/global-historical-climatology-network-daily
# all other data: https://www.ncei.noaa.gov/data/


# let's load this version so it's compressed with all the files 
folder = '/Users/keenzarate/Documents/data/ds4a/ghcn-m_v4.00.00_prcp_s16970101_e20220331_c20220407.tar.gz'


years = datetime.datetime.today().year
year_range =list(range(years, years-13, -1))

# create list of col names based on documentation
col_names = ['gcn_identifier','station_name','longitude', 'latitude', 'elevation_(meters)', 'year_month', 'precipitation_value', 'measurement_flag', 'qc_flag', 'source_flag', 'source_index']

# open tgz folder
tar = tarfile.open(folder, "r:gz")

# initalize an empty list
list_dt = []

# extract member of tar and read 
for member in tar:

    # convert to str so we can search
    str_var = str(member)

    if re.search('CH|BR|IN', str_var):
        # extract the member
        content = tar.extractfile(member).read()

        print(f'reading {member}')
        
        # open data 
        data = pd.read_csv(io.BytesIO(content), delimiter = ',', header = None, names = col_names, skipinitialspace=True)

        # append each dt into a list
        list_dt.append(data)

# stack list 
stacked_dt = pd.concat(list_dt)

# clean up columns 
stacked_dt['station_name'] = stacked_dt['station_name'].str.replace(' ', '')

stacked_dt['year_month']=stacked_dt['year_month'].astype('string')

stacked_dt['year'] = stacked_dt['year_month'].str.slice(0, 4).astype(int)

# subset to relevant years we need
sub_stacked = stacked_dt.loc[(stacked_dt['year'].isin(year_range))]


# look into one country
# and only D = precipitation total formed from four six-hour totals

#refer to this documentation for values https://www.ncei.noaa.gov/data/global-historical-climatology-network-daily/doc/GHCND_documentation.pdf

india_sub = sub_stacked.loc[(sub_stacked['gcn_identifier'].str.contains('IN\d{9}'))] 

india_sub.sort_values(by=['gcn_identifier', 'year'])

# count the number of years by gcn identifier, ideally we want at least 10!
num_years = india_sub.groupby(['gcn_identifier'])['year'].count().reset_index(name = 'counts')

# order by count
num_years.sort_values(by = ['counts'])

# look at the highest one -- looks like it's not really consistent by year.
india_sub.loc[india_sub['gcn_identifier'] == 'IN009021100']