import geopandas as gpd
import pandas as pd
import numpy as np
import shapely.geometry as geo
import os, datetime, json
import fiona
import itertools
import tqdm

def configure(context):
    context.config("output_path")
    context.config("jtab_data_path")
    context.config("sampling_rate")
    context.config("jtab_output_path")
    
    context.stage("jtab_preparation.cities")
    context.stage("jtab_preparation.activities")

def validate(context):
    output_path = context.config("output_path")

    if not os.path.isdir(output_path):
        raise RuntimeError("Output directory must exist: %s" % output_path)
    
def execute(context):
    output_dir    = context.config("output_path")
    jtab_data_dir = context.config("jtab_data_path")
    sampling_rate = context.config("sampling_rate")
    act           = context.stage("jtab_preparation.activities")[0]    
    jtab_output   = context.config("jtab_output_path")
    
    trips = gpd.read_file(output_dir + "/ile_de_france_trips.gpkg")
    trips = trips[(trips["preceding_purpose"].isin(act["activity_name"])) & (trips["following_purpose"].isin(act["activity_name"]))]    
    trips.loc[:, "activity_id"] = 0
    for index, row in act.iterrows():
        trips.loc[trips["following_purpose"] == row["activity_name"], "activity_id"] = row["activity_id"]
    
    map_cantons = gpd.read_file(jtab_data_dir + "/spatial/france_cantons_without_domtom.shp")
    map_cantons = map_cantons[["ref", "nom", "geometry"]]
    
    df_cities = context.stage("jtab_preparation.cities")
    map_cantons = map_cantons.merge(df_cities, left_on = "ref", right_on = "code")
    map_cantons = map_cantons[["city_id", "geometry"]]
    map_cantons = gpd.GeoDataFrame(map_cantons, geometry = "geometry", crs = "EPSG:2154")    
    
    print(map_cantons.head())
    
    trips = trips[["person_id", "trip_index", "activity_id", "geometry"]]
    
    print(trips.head())
    
    def linestring_to_points_start(line):
        return geo.Point(line.coords[0])
        
    def linestring_to_points_end(line):
        return geo.Point(line.coords[1])
        
    trips["from_point"] = trips.apply(lambda l: linestring_to_points_start(l['geometry']), axis=1)
    trips["to_point"]   = trips.apply(lambda l:   linestring_to_points_end(l['geometry']), axis=1)
    del trips["geometry"]
    print(trips.head())
    
    trips_from = gpd.GeoDataFrame(trips, geometry = "from_point", crs = "EPSG:2154")
    trips_to = gpd.GeoDataFrame(trips, geometry = "to_point", crs = "EPSG:2154")
    
    result_from = gpd.sjoin(trips_from[["person_id", "trip_index", "activity_id", "from_point"]], map_cantons, how="left")[["person_id", "trip_index", "activity_id", "city_id"]]
    result_to   = gpd.sjoin(trips_to[["person_id", "trip_index", "to_point"]],     map_cantons, how="left")[["person_id", "trip_index", "city_id"]]
    
    result_from.columns = ["person_id", "trip_index", "activity_id", "from_id"]
    result_to.columns   = ["person_id", "trip_index", "to_id"]
    
    od_disaggregate = pd.merge(result_from, result_to, on = ["person_id", "trip_index"], how = "inner")
    od_aggregate    = od_disaggregate.groupby(["from_id", "to_id", "activity_id"])["person_id"].size()
    od_aggregate    = od_aggregate.reset_index()
    od_aggregate.columns = ["from_id", "to_id", "activity_id", "count"]
    
    od_aggregate_zone = od_aggregate.groupby(["from_id", "activity_id"])["count"].sum().reset_index()
    od_aggregate_zone.columns = ["from_id", "activity_id", "total"]
    # The total here is defined by origin zone and activity type. The interpretations of the probabilities are thus like "I am currently in Paris and I want to perform a "holiday" activity. The proba of doing this activity in Marseille is 0.8 and 0.2 in Lyon" (-> to get a sum equal to 1, one must sum over the destination ids).
    
    od_aggregate = od_aggregate.merge(od_aggregate_zone, on = ["from_id", "activity_id"])
    od_aggregate.loc[:, "probability"] = [c/t for (c,t) in list(zip(od_aggregate["count"], od_aggregate["total"]))]
    od_aggregate    = od_aggregate.reset_index()
    print(od_aggregate.columns)
    
    od = od_aggregate[["from_id", "to_id", "activity_id", "probability"]]
    od = od.sort_values(["from_id", "to_id", "activity_id"], ascending = True)
    
    set_city_ids = list(set(map_cantons["city_id"]))
    fromtoids    = list(itertools.product(set_city_ids, set_city_ids, act["activity_id"].values.tolist()))
    
    df_empty = pd.DataFrame({"from_id":[l[0] for l in fromtoids], "to_id": [l[1] for l in fromtoids], "activity_id":[l[2] for l in fromtoids]})
    
    od = pd.merge(od, df_empty, how = "outer", on = ["from_id", "to_id", "activity_id"])
    od.loc[:, "probability"] = od["probability"].fillna(0)
    od = od.sort_values(["from_id", "activity_id", "to_id"], ascending = True)
            
    print(od)
    od.to_csv(jtab_output + "/destinationsProbDist_by_activity.csv", index = False)
    
    od = od.groupby(["from_id", "to_id"])["probability"].sum().reset_index()
    od = od.sort_values(["from_id","to_id"], ascending = True)
    od.to_csv(jtab_output + "/destinationsProbDist.csv", index = False)
    return od
