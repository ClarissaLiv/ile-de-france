import geopandas as gpd
import pandas as pd
import numpy as np
import shapely.geometry as geo
import os, datetime, json

def configure(context):
    context.config("output_path")
    
    context.stage("jtab_preparation.population_clustering")
    context.stage("jtab_preparation.origin_destination_probability")

def validate(context):
    output_path = context.config("output_path")

    if not os.path.isdir(output_path):
        raise RuntimeError("Output directory must exist: %s" % output_path)

def execute(context):
    odmatrix =  context.stage("jtab_preparation.origin_destination_probability")
    agents   =  context.stage("jtab_preparation.population_clustering")
    return odmatrix
