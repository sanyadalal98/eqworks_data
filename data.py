import pandas as pd
import io
import requests
import math
from datetime import datetime
import logging
logger = logging.getLogger(__name__)

# Question 1
try:
    url="https://raw.githubusercontent.com/EQWorks/ws-data-spark/master/data/DataSample.csv"
    data=pd.read_csv(url) #, index_col=0)
    #print(data)
except Exception as e:
    logger.error(str(e))
    logger.error('error occured while getting sample data')
try:
    data.drop_duplicates(subset=[' TimeSt', 'Latitude', 'Longitude'], keep="first", inplace=True)
    data.reset_index(drop=True, inplace=True)
    #print(data)
except Exception as e:
    logger.error(str(e))
    logger.error('error occured while filtering sample data')

# Queston 2
try:
    url2="https://raw.githubusercontent.com/EQWorks/ws-data-spark/master/data/POIList.csv"
    poi=pd.read_csv(url2)#, index_col=0)
    #print(poi)
except Exception as e:
    logger.error(str(e))
    logger.error('error occured while getting poi data')

def get_closest_POI(id, lat, long):
    dist = float('inf')
    p = 0
    for index, row in poi.iterrows():
        x = float(row[' Latitude']) - float(lat)
        y = float(row['Longitude']) - float(long)
        hyp = x * x + y * y
        if hyp < dist:
            dist = hyp
            p = index
    (poi.iat[p, 3]).append((id, dist))

try:
    poi_row_count = poi['POIID'].count()
    #requests will be list of type id * distance
    poi['requests'] = [[] for i in range(poi_row_count)]
    for index, row in data.iterrows():
        get_closest_POI(row['_ID'], row['Latitude'], row['Longitude'])
except Exception as e:
    logger.error(str(e))
    logger.error('error occured while calculating closest POI')


# Question 3 part 1
try:
    poi['mean'] = 0.0
    poi['std_dev'] = 0.0
    for index, row in poi.iterrows():
        if len(row['requests']) == 0:
            continue
        for request in row['requests']:
            id, dist = request
            poi.at[index, 'mean'] = poi.at[index, 'mean'] + float(dist)
        poi.at[index, 'mean'] = poi.at[index, 'mean']/len(row['requests'])
        for request in row['requests']:
            id, dist = request
            poi.at[index, 'std_dev'] = poi.at[index, 'std_dev'] + (dist - row['mean']) * (dist - row['mean'])
        poi.at[index, 'std_dev'] = math.sqrt(poi.at[index, 'std_dev']/len(row['requests']))
        #print(poi)
except Exception as e:
    logger.error(str(e))
    logger.error('error occured while calculating mean and standard deviation for POI')

# Question 3 part 2
try:
    poi['radius'] = 0.0
    poi['density'] = 0.0
    for index, row in poi.iterrows():
        if len(row['requests']) == 0:
            continue
        for request in row['requests']:
            id, dist = request
            if float(dist) > poi.at[index, 'radius']:
                poi.at[index, 'radius'] = float(dist)
        # from "https://gis.stackexchange.com/questions/142326/calculating-longitude-length-in-miles" we have that 1 deg in latitude ~ 69 miles && 1 deg in longitude ~ 55 miles
        # hence sqrt(1*1 + 1*1) = sqrt(2) deg in hypotenuse = sqrt(69 * 69 + 55 * 55) miles
        # hence by cross multiplication we have that 1 deg in hypotenuse ~ 62.39 miles
        #poi.at[index, 'radius'] = poi.at[index, 'radius'] * 62.39
        area = math.pi * (poi.at[index, 'radius']) * (poi.at[index, 'radius'])
        d = len(row['requests'])/area
        poi.at[index, 'density'] = d
except Exception as e:
    logger.error(str(e))
    logger.error('error occured while calculating radius and density for POI')

# Question 4 a
try:
    #reset radius and density so that we can recalculate it in a more precise manner
    poi['radius'] = 0.0
    poi['density'] = 0.0
    poi['popularity'] = 0
    largest_density = 0.0
    for index, row in poi.iterrows():
        if len(row['requests']) == 0:
            continue
        #count will keep count of all the requests that full the criterion outlined below
        count = 0
        for request in row['requests']:
            id, dist = request
            # discount all requests that are more than 2 standard deviations away from the mean when measuring popularity
            # more than 75% of requests should be less than 2 standard deviations away from the mean by Tchebysheff's theorem
            if (dist - row['mean']) <= (2 * row['std_dev']):
                count = count + 1
                if float(dist) > row['radius']:
                    poi.at[index, 'radius'] = float(dist)
        area = math.pi * (poi.at[index, 'radius']) * (poi.at[index, 'radius'])
        poi.at[index, 'density'] = count/area
        if poi.at[index, 'density'] > largest_density:
            largest_density = poi.at[index, 'density']
    slope = 20.0/largest_density
    for index, row in poi.iterrows():
        poi.at[index, 'popularity'] = -10 + slope * (poi.at[index, 'density'])
    print(poi)
except Exception as e:
    logger.error(str(e))
    logger.error('error occured while calculating popularity for POI')
