import geopandas as gpd
import pandas as pd
import numpy as np
import shapely.geometry as geo
import os, datetime, json
import fiona
import itertools
import tqdm
import errno

def configure(context):
    context.config("output_path")
    context.config("jtab_data_path")
    context.config("sampling_rate")
    context.config("jtab_output_path")
    
    context.stage("jtab_preparation.home_clustering")

def validate(context):
    output_path = context.config("output_path")

    if not os.path.isdir(output_path):
        raise RuntimeError("Output directory must exist: %s" % output_path)

def execute(context):
    output_dir    = context.config("output_path")
    jtab_data_dir = context.config("jtab_data_path")
    jtab_output = context.config("jtab_output_path")
    
    path = os.path.join(jtab_output, "spatial/")
    try:
        os.mkdir(path)
    except OSError as e:
        if e.errno == errno.EEXIST:
            print('Directory already created.')
        else:
            raise
    
    pop_per_zone = context.stage("jtab_preparation.home_clustering")
    
    map_cantons = gpd.read_file(jtab_data_dir + "/spatial/france_cantons_without_domtom.shp", crs = "EPSG:2154")
    map_cantons = map_cantons[["ref", "nom", "geometry"]]
    
    map_cantons = map_cantons.to_crs("EPSG:4326")
    map_cantons.to_file(path + "france_map_cantons.shp")
    
    map_cantons.columns = ["code", "city_name", "geometry"]
    map_cantons.loc[:, "city_id"] = range(len(map_cantons))
    map_cantons.loc[:, "country"] = "France"
    
    map_cantons["centroids"] = gpd.GeoSeries(map_cantons["geometry"].centroid)
    
    df_cities = gpd.GeoDataFrame(map_cantons, geometry = "centroids")
    
    df_cities["lng"]       = df_cities.geometry.x
    df_cities["lat"]       = df_cities.geometry.y
    
    print(df_cities.head())
    
    pop_per_zone = pop_per_zone[["city", "size"]]
    pop_per_zone = pop_per_zone.groupby(["city"])["size"].sum()
    pop_per_zone = pop_per_zone.reset_index()
    
    print(pop_per_zone)
    print(np.sum(pop_per_zone["size"]))
    
    df_cities = df_cities.merge(pop_per_zone, left_on = "city_name", right_on = "city", how = "left")
    df_cities = df_cities[["city_id", "code", "city_name", "lat", "lng", "country", "size", "centroids"]]
    df_cities.columns = ["city_id", "code", "city_name", "lat", "lng", "country", "population", "geometry"]
    df_cities = gpd.GeoDataFrame(df_cities, geometry = "geometry", crs = "EPSG:4326")
    
    df_cities.to_file(path +"france_map_centroids.shp")
    
    cities = df_cities = df_cities[["city_id", "code", "city_name", "lat", "lng", "country", "population"]]
    cities.to_csv(jtab_output + "/france.csv", index = False)    
    return df_cities
    

