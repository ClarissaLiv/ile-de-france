import shapely.geometry as geo
import numpy as np
import pandas as pd
import geopandas as gpd

def configure(context):
    context.stage("data.bpe.cleaned_longDistance")
    context.stage("data.spatial.municipalities")
    context.stage("synthesis.locations.work")
    context.stage("synthesis.locations.home")

def execute(context):
    df_locations = context.stage("data.bpe.cleaned_longDistance")[[
        "enterprise_id", "activity_type", "commune_id", "geometry"
    ]].copy()
    df_locations["destination_id"] = np.arange(len(df_locations))

    # Attach attributes for activity types
    # For long distance trips, only purposes "vacation" and "visit" and MAYBE "business" are considered.
    # From the BPE, only "vacation" purposes are currently being extracted.
    # "Visit" will be assumed to take place at residential locations, aka at a "home" that is not the agent's home.
    # If "buisness" is considered, then it will be at work locations that are not the agent's work location. - cvl, 27.06.2022
    df_locations["offers_holiday"] = df_locations["activity_type"] == "holiday"
    df_locations["offers_school_trip"] = df_locations["activity_type"] == "Education"
    df_locations["offers_shop"] = df_locations["activity_type"] == "Shopping"
    df_locations["offers_other"] = ~(df_locations["offers_holiday"] | df_locations["offers_school_trip"]  | df_locations["offers_shop"] )

    df_locations["offers_visits"]  = False
    df_locations["offers_business"]  = False

    # Define new IDs
    df_locations["location_id"] = np.arange(len(df_locations))
    df_locations["location_id"] = "sec_" + df_locations["location_id"].astype(str)

    df_locations_work = context.stage("synthesis.locations.work")[[
        "location_id", "commune_id", "geometry"
    ]].copy()

    # TODO find a safer way to assign locations using max of already existing locations ids
    M = len(df_locations)
    df_locations_work["location_id"] = np.arange(M, M+len(df_locations_work))

    df_locations_work["offers_holiday"] = False
    df_locations_work["offers_other"]   = False
    df_locations_work["offers_visits"]  = False
    df_locations_work["offers_school_trip"]  = False
    df_locations_work["offers_shop"]  = False
    df_locations_work["offers_business"]  = True

    df_locations_home = context.stage("synthesis.locations.home")[[
        "location_id", "commune_id", "geometry"
    ]].copy()

    M =len(df_locations) + len(df_locations_work)
    df_locations_home["location_id"] = np.arange(M, M + len(df_locations_home))

    df_locations_home["offers_holiday"] = False
    df_locations_home["offers_other"] = False
    df_locations_home["offers_visits"] = True
    df_locations_home["offers_school_trip"]  = False
    df_locations_home["offers_shop"]  = False
    df_locations_home["offers_business"] = False

    df_locations = pd.concat([df_locations, df_locations_home, df_locations_work])
    df_locations.loc[df_locations["offers_shop"].isna(), "offers_shop"] = False
    df_locations.loc[df_locations["offers_other"].isna(), "offers_other"] = False
    df_locations.loc[df_locations["offers_visits"].isna(), "offers_visits"] = False
    df_locations.loc[df_locations["offers_holiday"].isna(), "offers_holiday"] = False
    df_locations.loc[df_locations["offers_business"].isna(), "offers_business"] = False
    df_locations.loc[df_locations["offers_school_trip"].isna(), "offers_school_trip"] = False

    return df_locations
