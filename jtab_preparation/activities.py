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
    context.stage("jtab_preparation.population_clustering")
    
def validate(context):
    output_path = context.config("output_path")

    if not os.path.isdir(output_path):
        raise RuntimeError("Output directory must exist: %s" % output_path)
    
def execute(context):
    output_dir    = context.config("output_path")
    jtab_data_dir = context.config("jtab_data_path")
    sampling_rate = context.config("sampling_rate")    
    jtab_output = context.config("jtab_output_path")
    
    activities_purpose = ["home", "visits", "holiday"]
    act_purpose = pd.DataFrame({"activity_id":list(range(len(activities_purpose))), "activity_name":activities_purpose})
    act_purpose.to_csv(jtab_output + "/act.csv", index = False)
    
    cities = context.stage("jtab_preparation.cities")
    city_ids = cities["city_id"].values.tolist()
    
    city_activity = list(itertools.product(city_ids, act_purpose["activity_id"].values.tolist()))
    
    actcity = pd.DataFrame({"city_id":[l[0] for l in city_activity], "activity_id":[l[1] for l in city_activity], "activity_cost":[1 for l in city_activity]})
    actcity.to_csv(jtab_output + "/actCity.csv", index = False)
    
    plans = gpd.read_file(output_dir + "/ile_de_france_activities.gpkg")
    print(plans.columns)
    
    activities = plans[plans["purpose"].isin(act_purpose["activity_name"].values.tolist())]
    
    starts = activities["start_time"]
    starts = starts[~np.isnan(starts)]
    min_starts = np.min(starts)
    
    ends = activities["end_time"]
    ends = ends[~np.isnan(ends)]
    max_ends = np.max(ends)
    
    activities["end_time"] = activities["end_time"].fillna(max_ends)
    activities["start_time"] = activities["start_time"].fillna(min_starts)
    
    activities.loc[:, "time_duration"] = (activities["end_time"] - activities["start_time"])/60
    activities.loc[:, "activity_id"] = "home"
    
    for index, row in act_purpose.iterrows():
        activities.loc[activities["purpose"] == row["activity_name"], "activity_id"] = row["activity_id"]
    
    agents = context.stage("jtab_preparation.population_clustering")
    agents = agents[["person_id", "agent_id"]]
    
    activities = activities[["person_id", "activity_index", "activity_id", "time_duration"]]
    activities = activities.merge(agents, on = "person_id", how = "left")
    
    activities = activities.groupby(["agent_id", "activity_id"])["time_duration"].mean().reset_index()
    activities["time_duration"] = [round(t) for t in activities["time_duration"]]
    activities.loc[:, "perc_of_time_target"] = 0.1
    activities.loc[:, "duration_discomfort"] = 1
    
    print(activities)
    activities.to_csv(jtab_output + "/popActivities.csv", index = False)
    
    return act_purpose, activities
