import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime,date
! pip install geopandas
import geopandas as gpd
from geopandas import GeoDataFrame as gdf
from google.colab import files
import folium
from folium import Choropleth, Circle, Marker
from folium.plugins import HeatMap, MarkerCluster

import warnings
warnings.filterwarnings('ignore')

path = '/content/drive/My Drive/AHEAD_BigData/taxiGPS.csv'

df = pd.read_csv(path)

df['passenger_id'].value_counts()

df['passenger_id'].nunique()

df['driver_id'].value_counts()

df['driver_id'].nunique()

city_location = [6.8691, 79.8915]

gps_data = gpd.GeoDataFrame(gps_data, geometry=gpd.points_from_xy(gps_data.pickup_long,gps_data.pickup_lat),crs='EPSG:4326')  #converting to GeoDataframe with Coordinate Reference system 4326
map =  folium.Map(location=city_location, tiles='openstreetmap', zoom_start=10)
for idx, row in gps_data.iterrows():
  Marker([row['pickup_lat'], row['pickup_long']]).add_to(map)

sf = '/content/drive/My Drive/AHEAD_BigData/TAZ_461.shp'

zones = gpd.read_file(sf)

zones['id'] = zones['RDA_Old']

ori_data = gpd.GeoDataFrame(trips, geometry=gpd.points_from_xy(trips.pickup_long,trips.pickup_lat),crs='EPSG:5234')

des_data = gpd.GeoDataFrame(trips, geometry=gpd.points_from_xy(trips.dropoff_long,trips.dropoff_lat),crs='EPSG:5234')

z_list = list(zones.id)
ori_df = pd.DataFrame().reindex_like(ori_data).dropna()

for z in z_list:
  pol = (zones.loc[zones.id == z])
  pol.reset_index(drop = True, inplace =True)
  pip_mask = ori_data.within(pol.loc[0, 'geometry'])
  pip_data = ori_data.loc[pip_mask].copy()
  pip_data['ori_zone']= z
  ori_df = ori_df.append(pip_data)

des_df = pd.DataFrame().reindex_like(des_data).dropna()

for z in z_list:
  pol = (zones.loc[zones.id == z])
  pol.reset_index(drop = True, inplace =True)
  pip_mask = des_data.within(pol.loc[0, 'geometry'])
  pip_data = des_data.loc[pip_mask].copy()
  pip_data['des_zone']= z
  des_df = des_df.append(pip_data)

des_df = des_df[['trip_id','des_zone',]]

taxi_od = pd.merge(left=ori_df, right=des_df,left_on='trip_id', right_on='trip_id')

taxi_od['drop_time'] = pd.to_datetime(taxi_od['drop_time']).dt.time

taxi_od['pickup_time'] = pd.to_datetime(taxi_od['pickup_time']).dt.time

taxi_od['duration'] = pd.Series(dtype='object')

for i in range(len(taxi_od)):
   taxi_od.at[i,'duration'] = datetime.combine(date.min,taxi_od.at[i,'drop_time']) - datetime.combine(date.min,taxi_od.at[i,'pickup_time'])

taxi_od['hour_of_day'] = list(map(lambda  x: x.hour, (taxi_od['pickup_time'])))

origin = list(range(1,462))

dest = list(range(1,462))

od_matrix = taxi_od.assign(count=1).pivot_table(index='ori_zone', columns ='des_zone',values='count',aggfunc='count',margins=True,margins_name= 'sum_total').fillna(0).astype(int)

od_matrix = od_matrix.sort_values(by=od_matrix.columns[0], ascending=False)

od_matrix = od_matrix.sort_values(by=od_matrix.index[0], axis=1, ascending=False)

od_matrix = taxi_od.assign(count=1).pivot_table(index='ori_zone', columns ='des_zone',values='count',aggfunc='count').fillna(0).astype(int)

od_matrix.reset_index(inplace=True)
od_matrix = od_matrix.rename(columns = {'index':'ori_zone'})

od_matrix.max().max()

def download_csv(data,filename):
  filename= filename + '.csv'
  data.to_csv(filename, encoding = 'utf-8-sig',index= False)
  files.download(filename)

download_csv(od_matrix,'od_matrix_sun')
