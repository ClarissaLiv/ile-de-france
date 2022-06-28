from tqdm import tqdm
import pandas as pd
import numpy as np
import data.hts.hts as hts

"""
This stage cleans the national HTS.
"""

def configure(context):
    context.stage("data.hts.entd.raw_long_distances")

INCOME_CLASS_BOUNDS = [400, 600, 800, 1000, 1200, 1500, 1800, 2000, 2500, 3000, 4000, 6000, 10000, 1e6]

PURPOSE_MAP = [
    ("1", "home"),
    ("1.11", "school_trip"),
    ("2", "shop"),
    ("3", "other"),
    ("4", "other"),
    ("5", "visits"),
    ("6", "other"),
    ("7", "holiday"),
    ("8", "holiday"),
    ("9", "business")
]

MODES_MAP = [
    ("1", "walk"),
    ("2", "car"), #
    ("2.20", "bike"), # bike
    ("2.23", "car_passenger"), # motorcycle passenger
    ("2.25", "car_passenger"), # same
    ("3", "car"),
    ("3.32", "car_passenger"),
    ("4", "pt_taxi"), # taxi
    ("5", "pt_regional"),
    ("6", "pt_LongDistanceTrains"),
    ("7", "pt_Airplane"), # Plane
    ("8", "pt_boat"), # Boat
#    ("9", "pt_other") # Other
]

def convert_time(x):
    if type(x) == str:
        #print(np.dot(np.array(x.split(":"), dtype = np.float), [3600.0, 60.0, 1.0]))
        return np.dot(np.array(x.split(":"), dtype = np.float), [3600.0, 60.0, 1.0])
    else:
        return 0
        
def fix_times(x):
    hour = int(x[11:13])
    day  = int(x[0:2])
    
    if hour >= 24:
        hour = hour - 24
        day = day + 1
        
    x = str(day) + x[2:11] + str(hour) + x[13:]
    return x
        
def convert_time_long_distances(df_trips, option = "departure"):
    column_start_day = "departure_day"
    
    if option == "departure":
        day_column  = "V2_OLDDEJ"
        time_column = "V2_OLDDEH"
        col_name    = "departure_time"
        
    if option == "arrival":
        day_column  = "V2_OLDARJ"
        time_column = "V2_OLDARH"
        col_name    = "arrival_time"
        
    df_trips[day_column] = df_trips[day_column].astype(str)   
    df_trips[time_column] = df_trips[time_column].astype(str) 
    
    df_trips.loc[df_trips[time_column] == "nan", time_column] = "00:00:01"
    df_trips.loc[df_trips[day_column] == "nan", day_column] = "01/01/1900"
    
    df_trips[time_column] = df_trips[time_column].str.rjust(8, "0")
    
    df_trips["time"] = df_trips[[day_column, time_column]].agg(' '.join, axis=1)
    
    df_trips["time"] = df_trips["time"].apply(fix_times)
    
    df_trips["time"] = pd.to_datetime(df_trips["time"], format = '%d/%m/%Y %H:%M:%S')
    
    df_trips["origin_day"] = pd.to_datetime(df_trips[column_start_day], format = '%d/%m/%Y')
    
    df_trips[col_name] = pd.to_timedelta(df_trips["time"] - df_trips["origin_day"], unit = "m")
    df_trips[col_name] = df_trips[col_name] / pd.Timedelta("1 second")
    
    del df_trips["time"]
    del df_trips["origin_day"]
    
    return df_trips

def execute(context):
    df_individu, df_tcm_individu, df_menage, df_tcm_menage,  df_ld_general, df_ld_detailed = context.stage("data.hts.entd.raw_long_distances")

    # Make copies
    df_persons         = pd.DataFrame(df_tcm_individu, copy = True)
    df_households      = pd.DataFrame(df_tcm_menage, copy = True)
    df_holiday_general = pd.DataFrame(df_ld_general, copy = True)
    df_holiday_det     = pd.DataFrame(df_ld_detailed, copy = True)

    # Get weights for persons that actually have trips
    df_persons = pd.merge(df_persons, df_holiday_det[["IDENT_IND", "POIDS_VOY13"]].drop_duplicates("IDENT_IND"), on = "IDENT_IND", how = "left")
    df_persons["is_kish"] = ~df_persons["POIDS_VOY13"].isna()
    df_persons["trip_weight"] = df_persons["POIDS_VOY13"].fillna(0.0)

    # Important: If someone did not have any trips on the reference day, ENTD asked
    # for another day. With this flag we make sure that we only cover "reference days".
    #f = df_trips["V2_MOBILREF"] == 1
    #df_trips = df_trips[f]
    #print("Filtering out %d non-reference day trips" % np.count_nonzero(~f))

    # Merge in additional information from ENTD
    df_households = pd.merge(df_households, df_menage[[
        "idENT_MEN", "V1_JNBVEH", "V1_JNBMOTO", "V1_JNBCYCLO", "V1_JNBVELOADT"
    ]], on = "idENT_MEN", how = "left")

    df_persons = pd.merge(df_persons, df_individu[[
        "IDENT_IND", "V1_GPERMIS", "V1_GPERMIS2R", "V1_ICARTABON"
    ]], on = "IDENT_IND", how = "left")

    # Transform original IDs to integer (they are hierarchichal)
    df_persons["entd_person_id"] = df_persons["IDENT_IND"].astype(np.int)
    df_persons["entd_household_id"] = df_persons["IDENT_MEN"].astype(np.int)
    df_households["entd_household_id"] = df_households["idENT_MEN"].astype(np.int)
    df_holiday_det["entd_person_id"] = df_holiday_det["IDENT_IND"].astype(np.int)

    # Construct new IDs for households, persons and trips (which are unique globally)
    df_households["household_id"] = np.arange(len(df_households))

    df_persons = pd.merge(
        df_persons, df_households[["entd_household_id", "household_id"]],
        on = "entd_household_id"
    )
    df_persons["person_id"] = np.arange(len(df_persons))
    
    df_trips = pd.merge(
        df_holiday_det, df_holiday_general, how = "left", on = ["IDENT_VOY"]
    )

    df_trips = pd.merge(
        df_trips, df_persons[["entd_person_id", "person_id", "household_id"]],
        on = ["entd_person_id"]
    )
    df_trips["trip_id"] = np.arange(len(df_trips))
    
    # In long distance trips: defining first day and last day of the trip
    df_trips.loc[:, "departure_day"] = df_trips["V2_OLDDEBJ"]
    df_trips.loc[:, "return_day"]    = df_trips["V2_OLDFINJ"]
    
    # Define vacation id
    df_trips.loc[:, "vacation_id"] = df_trips["IDENT_VOY"]
    
    # Departure and arrival time
    #df_trips["departure"]      = df[['V2_OLDDEJ', 'V2_OLDDEH']].agg('-'.join, axis=1)
    #df_trips["arrival"]        = df[['V2_OLDARJ', 'V2_OLDARH']].agg('-'.join, axis=1)
    
    #df_trips["departure_time"] = df_trips["departure"].apply(convert_time).astype(np.float)
    #df_trips["arrival_time"]   = df_trips["arrival"].apply(convert_time).astype(np.float)
    
    df_trips = convert_time_long_distances(df_trips, "departure")
    df_trips = convert_time_long_distances(df_trips, "arrival")

    # Weight
    df_persons["person_weight"] = df_persons["PONDV1"].astype(np.float)
    df_households["household_weight"] = df_households["PONDV1"].astype(np.float)

    # Clean age
    df_persons.loc[:, "age"] = df_persons["AGE"]

    # Clean sex
    df_persons.loc[df_persons["SEXE"] == 1, "sex"] = "male"
    df_persons.loc[df_persons["SEXE"] == 2, "sex"] = "female"
    df_persons["sex"] = df_persons["sex"].astype("category")

    # Household size
    df_households["household_size"] = df_households["NPERS"]

    # Clean departement
    df_households["departement_id"] = df_households["DEP"].fillna("undefined").astype("category")
    df_persons["departement_id"] = df_persons["DEP"].fillna("undefined").astype("category")

    df_trips["origin_departement_id"]      = df_trips["DEP"].fillna("undefined").astype("category")
    df_trips["destination_departement_id"] = df_trips["V2_OLDVDEP"].fillna("undefined").astype("category")

    # Clean employment
    df_persons["employed"] = df_persons["SITUA"].isin([1, 2])

    # Studies
    # Many < 14 year old have NaN
    df_persons["studies"] = df_persons["ETUDES"].fillna(1) == 1
    df_persons.loc[df_persons["age"] < 5, "studies"] = False

    # Number of vehicles
    df_households["number_of_vehicles"] = 0
    df_households["number_of_vehicles"] += df_households["V1_JNBVEH"].fillna(0)
    df_households["number_of_vehicles"] += df_households["V1_JNBMOTO"].fillna(0)
    df_households["number_of_vehicles"] += df_households["V1_JNBCYCLO"].fillna(0)
    df_households["number_of_vehicles"] = df_households["number_of_vehicles"].astype(np.int)

    df_households["number_of_bikes"] = df_households["V1_JNBVELOADT"].fillna(0).astype(np.int)

    # License
    df_persons["has_license"] = (df_persons["V1_GPERMIS"] == 1) | (df_persons["V1_GPERMIS2R"] == 1)

    # Has subscription
    df_persons["has_pt_subscription"] = df_persons["V1_ICARTABON"] == 1

    # Household income
    df_households["income_class"] = -1
    df_households.loc[df_households["TrancheRevenuMensuel"].str.startswith("Moins de 400"), "income_class"] = 0
    df_households.loc[df_households["TrancheRevenuMensuel"].str.startswith("De 400"), "income_class"] = 1
    df_households.loc[df_households["TrancheRevenuMensuel"].str.startswith("De 600"), "income_class"] = 2
    df_households.loc[df_households["TrancheRevenuMensuel"].str.startswith("De 800"), "income_class"] = 3
    df_households.loc[df_households["TrancheRevenuMensuel"].str.startswith("De 1 000"), "income_class"] = 4
    df_households.loc[df_households["TrancheRevenuMensuel"].str.startswith("De 1 200"), "income_class"] = 5
    df_households.loc[df_households["TrancheRevenuMensuel"].str.startswith("De 1 500"), "income_class"] = 6
    df_households.loc[df_households["TrancheRevenuMensuel"].str.startswith("De 1 800"), "income_class"] = 7
    df_households.loc[df_households["TrancheRevenuMensuel"].str.startswith("De 2 000"), "income_class"] = 8
    df_households.loc[df_households["TrancheRevenuMensuel"].str.startswith("De 2 500"), "income_class"] = 9
    df_households.loc[df_households["TrancheRevenuMensuel"].str.startswith("De 3 000"), "income_class"] = 10
    df_households.loc[df_households["TrancheRevenuMensuel"].str.startswith("De 4 000"), "income_class"] = 11
    df_households.loc[df_households["TrancheRevenuMensuel"].str.startswith("De 6 000"), "income_class"] = 12
    df_households.loc[df_households["TrancheRevenuMensuel"].str.startswith("10 000"), "income_class"] = 13
    df_households["income_class"] = df_households["income_class"].astype(np.int)

    # Trip purpose
    df_trips["following_purpose"] = "other"
    #df_trips["preceding_purpose"] = "other"

    for prefix, activity_type in PURPOSE_MAP:
        df_trips.loc[
            df_trips["V2_OLDMOT"].astype(np.str).str.startswith(prefix), "following_purpose"
        ] = activity_type

        #df_trips.loc[
        #    df_trips["V2_MMOTIFORI"].astype(np.str).str.startswith(prefix), "preceding_purpose"
        #] = activity_type

    df_trips["following_purpose"] = df_trips["following_purpose"].astype("category")
    #df_trips["preceding_purpose"] = df_trips["preceding_purpose"].astype("category")

    # Trip mode
    df_trips["mode"] = "pt"

    for prefix, mode in MODES_MAP:
        df_trips.loc[
            df_trips["V2_OLDMT1S"].astype(np.str).str.startswith(prefix), "mode"
        ] = mode

    df_trips["mode"] = df_trips["mode"].astype("category")

    # Further trip attributes
    df_trips["routed_distance"] = df_trips["V2_OLDKM"] * 1000.0
    df_trips["routed_distance"] = df_trips["routed_distance"].fillna(0.0) # This should be just one within Île-de-France

    # Only leave weekday trips
    #f = df_trips["V2_TYPJOUR"] == 1
    #print("Removing %d trips on weekends" % np.count_nonzero(~f))
    #df_trips = df_trips[f]

    # Only leave one day per person
    #initial_count = len(df_trips)

    #df_first_day = df_trips[["person_id", "IDENT_JOUR"]].sort_values(
    #    by = ["person_id", "IDENT_JOUR"]
    #).drop_duplicates("person_id")
    #df_trips = pd.merge(df_trips, df_first_day, how = "inner", on = ["person_id", "IDENT_JOUR"])

    final_count = len(df_trips)
    #print("Removed %d trips for non-primary days" % (initial_count - final_count))

    # Trip flags
    df_trips = hts.compute_first_last_long_distances(df_trips)  
    df_trips = hts.fix_trip_times(df_trips)

    # Trip times
    #print(df_trips["V2_OLDDEH"].head(5).apply(convert_time))
    #exit()

    # Durations
    df_trips["trip_duration"] = df_trips["arrival_time"] - df_trips["departure_time"]
    hts.compute_activity_duration(df_trips)

    # Add weight to trips
    df_trips["trip_weight"] = df_trips["POIDS_VOY13"]

    # Chain length
    df_persons = pd.merge(
        df_persons, df_trips[["person_id", "NBD"]].drop_duplicates("person_id").rename(columns = { "NBD": "number_of_trips" }),
        on = "person_id", how = "left"
    )
    df_persons["number_of_trips"] = df_persons["number_of_trips"].fillna(-1).astype(np.int)
    df_persons.loc[(df_persons["number_of_trips"] == -1) & df_persons["is_kish"], "number_of_trips"] = 0

    # Passenger attribute
    df_persons["is_passenger"] = df_persons["person_id"].isin(
        df_trips[df_trips["mode"] == "car_passenger"]["person_id"].unique()
    )

    # Calculate consumption units
    hts.check_household_size(df_households, df_persons)
    df_households = pd.merge(df_households, hts.calculate_consumption_units(df_persons), on = "household_id")

    # Socioprofessional class
    df_persons["socioprofessional_class"] = df_persons["CS24"].fillna(80).astype(int) // 10
    
    # Holiday main mode and main purpose
    df_trips.loc[:, "vacation_main_purpose"] = df_trips["V2_OLDMOTPR"]
    
    for prefix, activity_type in PURPOSE_MAP:
        df_trips.loc[
            df_trips["vacation_main_purpose"].astype(np.str).str.startswith(prefix), "vacation_main_purpose"
        ] = activity_type
        
    df_trips.loc[:, "vacation_main_mode"] = df_trips["V2_OLDMTPP"]
    
    for prefix, mode in MODES_MAP:
        df_trips.loc[
            df_trips["vacation_main_mode"].astype(np.str).str.startswith(prefix), "vacation_main_mode"
        ] = mode
    
    # Fixing preceding purpose
    df_trips.loc[: , "purpose_previous_trip"] = ["  "] + df_trips["following_purpose"][:len(df_trips)-1].values.tolist()
    df_trips.loc[:, "preceding_purpose"] = df_trips["purpose_previous_trip"]
    df_trips.loc[df_trips["is_first_trip"], "preceding_purpose"] = "home"
    
    # Removing vacations with NAN times (ie departure time == 1 second)
    f = (df_trips["departure_time"]%10 == 1) | (df_trips["arrival_time"]%10 == 1) | df_trips["departure_time"].isna() | (df_trips["departure_time"] < 0)
    vacation_ids_filter = np.unique(df_trips[f]["vacation_id"])
    
    print(len(df_trips))
    df_trips = df_trips[~df_trips["vacation_id"].isin(vacation_ids_filter)]
    print(len(df_trips))
    
    # Cleaning df_trips
    df_trips = df_trips[[
        "entd_person_id", "person_id", "household_id", "vacation_id",
        "trip_weight", "trip_id", "is_first_trip", "is_last_trip",
        "departure_day", "return_day", "departure_time", "arrival_time", "trip_duration", "activity_duration",
        "routed_distance", "mode", "preceding_purpose", "following_purpose", 
        "origin_departement_id", "destination_departement_id",
        "vacation_main_purpose", "vacation_main_mode"
    ]]
    
    df_trips.to_csv("trips_entd_ld.csv", index = False)

    return df_households, df_persons, df_trips

def calculate_income_class(df):
    assert "household_income" in df
    assert "consumption_units" in df

    return np.digitize(df["household_income"], INCOME_CLASS_BOUNDS, right = True)
