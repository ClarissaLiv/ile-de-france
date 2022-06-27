import shapely.geometry as geo
import numpy as np
import pandas as pd
import geopandas as gpd

def configure(context):
    context.stage("data.bpe.cleaned")
    context.stage("data.spatial.municipalities")

def execute(context):
    df_locations = context.stage("data.bpe.cleaned")[[
        "enterprise_id", "activity_type", "commune_id", "geometry"
    ]].copy()
    df_locations["destination_id"] = np.arange(len(df_locations))

    # Attach attributes for activity types
    # For long distance trips, only purposes "vacation" and "visit" and MAYBE "business" are considered.
    # From the BPE, only "vacation" purposes are currently being extracted.
    # "Visit" will be assumed to take place at residential locations, aka at a "home" that is not the agent's home.
    # If "buisness" is considered, then it will be at work locations that are not the agent's work location. - cvl, 27.06.2022
    df_locations["offers_vacation"] = df_locations["activity_type"] == "vacation"
    df_locations["offers_other"] = ~(df_locations["offers_vacation"])

    # Define new IDs
    df_locations["location_id"] = np.arange(len(df_locations))
    df_locations["location_id"] = "sec_" + df_locations["location_id"].astype(str)

    return df_locations
