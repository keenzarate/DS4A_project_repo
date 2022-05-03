import pandas as pd
import io
import tarfile
import os
import re
from tabulate import tabulate
import matplotlib.pyplot as plt
import fiona
import geopandas as gpd


pd.set_option('display.max_colwidth', None)

folder = '/Users/keenzarate/Documents/data/ds4a/precip_2017.tar.gz'

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

# subset data only annual values
sub_data = stacked_dt_long.loc[stacked_dt_long['variable'].isin(['overall'])]

# convert column to string so we can grab year -- this is so slow ..
sub_data['filename']=sub_data['filename'].astype('string')

# grab year so we can identify rows that belong to given year
sub_data['year'] = sub_data['filename'].str.slice(17, 21)



# plot coordinates on map 

countries = gpd.read_file(gpd.datasets.get_path("naturalearth_lowres"))

countries.plot(color="lightgrey")

gdf_data = gpd.GeoDataFrame(sub_data, geometry=gpd.points_from_xy(sub_data.longitude, sub_data.latitude))

ax = countries.plot(color='white', edgecolor='black')

# We can now plot our ``GeoDataFrame``.
gdf_data.plot(ax=ax, markersize = 2, color = 'red')

plt.show()

