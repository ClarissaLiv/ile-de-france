import geopandas as gpd
import pandas as pd
import numpy as np
import shapely.geometry as geo
import os, datetime, json

AGE_BOUNDARIES = [-1,24,44,64,100]
GENDER         = ["female", "male"]                        
INCOME         = [-1, 8293, 10389, 11666, 13266, 25957]

def configure(context):
    context.config("output_path")
    context.config("jtab_data_path")
    context.config("jtab_output_path")

def validate(context):
    output_path = context.config("output_path")

    if not os.path.isdir(output_path):
        raise RuntimeError("Output directory must exist: %s" % output_path)

def execute(context):
    output_dir    = context.config("output_path")
    jtab_data_dir = context.config("jtab_data_path")
    jtab_output_dir = context.config("jtab_output_path")
    
    persons = pd.read_csv(output_dir + "/ile_de_france_persons.csv", sep = ";")
    hhl     = pd.read_csv(output_dir + "/ile_de_france_households.csv", sep = ";")
    
    persons = persons.merge(hhl, how = "left", on = "household_id")
    
    persons.loc[:, "age"]    = [min(a, 100) for a in persons["age"]]
    persons.loc[:, "income"] = [round(i) for i in persons["income"]]
    persons.loc[:, "income"] = [min(i, 25957) for i in persons["income"]]
    
    persons.loc[:, "age_class"]    = np.digitize(persons["age"], AGE_BOUNDARIES, right = True)
    persons.loc[:, "income_class"] = np.digitize(persons["income"], AGE_BOUNDARIES, right = True)
    
    persons["age_class"] = persons["age_class"].astype(str)
    persons["income_class"] = persons["income_class"].astype(str)
    
    persons.loc[:, "definition"]   = persons[["sex", "age_class", "income_class"]].agg("-".join, axis = 1)
    
    # Agent id definition
    agents = pd.read_csv(jtab_data_dir + "/agents.csv")
    agents.to_csv(jtab_output_dir + "/agents.csv", index = False)
    agents.loc[:, "age_class"]    = [(l == 0) * "1" + (l == 25) * "2" + (l == 45) * "3" + (l == 65) * "4" for l in agents["l_age"]]
    agents.loc[:, "income_class"] = [(l == 0) * "1" + (l == 8924) * "2" + (l == 10390) * "3" + (l == 11667) * "4" + (l == 13267) * "5" for l in agents["l_income"]]
    agents.loc[:, "gender_class"] = [(l) * "male" + (not l) * "female" for l in agents["gender"]]
    
    
    agents.loc[:, "definition"]   = agents[["gender_class", "age_class", "income_class"]].agg("-".join, axis = 1)
    agents = agents[["agent_id", "definition"]]
    
    persons = persons.merge(agents, how = "left", on = ["definition"])
    del persons["definition"]   
    
    print(persons.head())
    
    return persons
