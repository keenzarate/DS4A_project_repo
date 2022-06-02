import pandas as pd
import io
import tarfile
import boto3
import re
from tabulate import tabulate
import matplotlib.pyplot as plt
import geopandas as gpd


pd.set_option('display.max_colwidth', None)


# data downloaded from: http://climate.geog.udel.edu/~climate/html_pages/download.html
# Under "http://climate.geog.udel.edu/~climate/html_pages/download.html"
# Terrestrial Monthly Precipitation series (precip_2017.tar.gz (284MB))


folder = '/Users/keenzarate/Documents/data/ds4a/precipitation_climate_geo.tar.gz'

# create list of col names
col_names = ['longitude', 'latitude', 'jan', 'feb', 'march', 'april', 'may', 'june', 'july', 'august', 'september', 'october', 'november', 'december', 'overall']

# open tgz folder
tar = tarfile.open(folder, "r:gz")

list_dt = []

# extract member of tar and read 
for member in tar:

    str_var = str(member)

    if re.search('2010|2011|2012|2013|2014|2015|2016|2017|2018|2019', str_var):
        
        # extract the member
        content = tar.extractfile(member).read()

        print(f'reading {member}')
        
        # open data 
        data = pd.read_csv(io.BytesIO(content), sep="\s+", header = None, names = col_names)

        # store name of the file into a column
        data['filename'] = member

        # append each dt into a list
        list_dt.append(data)

# stack list 
stacked_dt = pd.concat(list_dt)

# convert data long to make it easier to read
stacked_dt_long = pd.melt(stacked_dt, id_vars = ['longitude', 'latitude', 'filename'], value_name = 'moisture_index')

# convert column to string so we can grab year -- this is so slow ..
stacked_dt_long['filename']=stacked_dt_long['filename'].astype('string')

# grab year so we can identify rows that belong to given year
stacked_dt_long['year'] = stacked_dt_long['filename'].str.slice(17, 21)

# subset data only annual values
sub_data = stacked_dt_long.loc[stacked_dt_long['variable'].isin(['overall'])]

# subset one coordinate just to test the map thing
test_sub = sub_data.loc[((sub_data['longitude'] == 78.75) & (sub_data['latitude'] == 20.75)) | ((sub_data['longitude'] == 104.25) & (sub_data['latitude'] == 35.25))]

test_sub.sort_values(by=['longitude', 'latitude', 'year'])


# # plot coordinates on map 

countries = gpd.read_file(gpd.datasets.get_path("naturalearth_lowres"))

countries.plot(color="lightgrey")

gdf_data = gpd.GeoDataFrame(test_sub, geometry=gpd.points_from_xy(test_sub.longitude, test_sub.latitude))

ax = countries.plot(facecolor='Grey', edgecolor='k',alpha=1,linewidth=1)

gdf_data.plot(ax=ax, color = 'red', markersize = 10)

plt.show()

