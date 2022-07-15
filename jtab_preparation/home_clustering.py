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
    
    context.stage("jtab_preparation.population_clustering")

def validate(context):
    output_path = context.config("output_path")

    if not os.path.isdir(output_path):
        raise RuntimeError("Output directory must exist: %s" % output_path)

def execute(context):
    output_dir    = context.config("output_path")
    jtab_data_dir = context.config("jtab_data_path")
    sampling_rate = context.config("sampling_rate")
    
    homes = gpd.read_file(output_dir + "/ile_de_france_homes.gpkg")
    
    map_cantons = gpd.read_file(jtab_data_dir + "/spatial/france_cantons_without_domtom.shp")
    map_cantons = map_cantons[["ref", "nom", "geometry"]]
    
    result = gpd.sjoin(homes, map_cantons, how="left")
    result = result[["household_id", "ref"]]
    
    
    population_df = context.stage("jtab_preparation.population_clustering")
    population_df = population_df[["household_id", "agent_id"]]
    
    agents_per_zone = pd.merge(population_df, result, on = "household_id", how = "left")
    
    agents_per_zone = agents_per_zone.groupby(["ref", "agent_id"])["household_id"].size()
    agents_per_zone = agents_per_zone.reset_index()
    agents_per_zone = agents_per_zone.sort_values(["ref", "agent_id"], ascending = True)
    
    agents_per_zone = pd.merge(agents_per_zone, map_cantons[["ref", "nom"]], on = "ref", how = "left")
    agents_per_zone.columns = ["code", "agent_id", "size", "city"]
    agents_per_zone         = agents_per_zone[["agent_id", "city", "size"]]
    
    agents_per_zone["size"] = agents_per_zone["size"]/ sampling_rate
    
    all_agents_id = range(40)
    all_cities    = np.unique(map_cantons["nom"])
    
    agentcities = list(itertools.product(all_agents_id, all_cities))
    df_empty = pd.DataFrame({"agent_id": [l[0] for l in agentcities], "city":[l[1] for l in agentcities]})
    
    agents_per_zone = pd.merge(agents_per_zone, df_empty, on = ["city", "agent_id"])
    agents_per_zone["size"] = agents_per_zone["size"].fillna(0)
    
    names = map_cantons[["ref", "nom"]].rename(columns = {"ref":"code", "nom":"city"})
    agents_per_zone = agents_per_zone.merge(names, how = "left", on = "city")
    agents_per_zone = agents_per_zone.sort_values(["code", "agent_id"], ascending = True)
    del agents_per_zone["code"]
    
    jtab_output = context.config("jtab_output_path")
    agents_per_zone.to_csv(jtab_output + "/residence.csv", index = False)
    
    return agents_per_zone
    
    
    
