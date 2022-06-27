import simpledbf
from tqdm import tqdm
import pandas as pd
import os

"""
This stage loads the raw data of the French HTS (ENTD).
"""

Q_MENAGE_COLUMNS = [
    "DEP", "idENT_MEN", "PONDV1", "RG",
    "V1_JNBVELOADT",
    "V1_JNBVEH", "V1_JNBMOTO", "V1_JNBCYCLO"
]

Q_TCM_MENAGE_COLUMNS = [
    "NPERS", "PONDV1", "TrancheRevenuMensuel",
    "DEP", "idENT_MEN", "RG"
]

Q_INDIVIDU_COLUMNS = [
    "IDENT_IND", "idENT_MEN",
    "RG", "V1_GPERMIS", "V1_ICARTABON",
    "V1_GPERMIS2R"
]

Q_TCM_INDIVIDU_COLUMNS = [
    "AGE", "ETUDES", "IDENT_IND", "IDENT_MEN",
    "PONDV1", "CS24", "SEXE", "DEP", "SITUA",
]

K_DEPLOC_COLUMNS = [
    "IDENT_IND",                                      # Individual ID                                          -> OK
    "V2_MMOTIFDES",                                   # Destination purpose                                    -> OK
    "V2_MMOTIFORI",                                   # Origin purpose                                         -> OK (always home I guess)                      
    "V2_TYPJOUR",                                     # Used to remove weekends, probably not relevant here    -> Leave it for now
    "V2_MORIHDEP",                                    # Departure hour                                         -> ?
    "V2_MDESHARR",                                    # Arrival hour                                           -> ?
    "V2_MDISTTOT",                                    # Total distance                                         -> OK
    "IDENT_JOUR",                                     # IDENT_JOUR -> to keep only one day per person.         -> See below
    "V2_MTP",                                         # MTP = mode                                             -> OK
    "V2_MDESDEP",                                     # Destination department                                 -> OK
    "V2_MORIDEP",                                     # Origin department                                      -> OK
    "NDEP",                                           # Number of trips                                        -> OK
    "V2_MOBILREF",                                    # Reference day (?)                                      -> Probably not relevant here
    "PONDKI"                                          # weight                                                 -> OK, see below
]

K_VOYAGE_COLUMNS = [                                    # General description of the long-distance trips
    #"IDENT_IND",                                        # Some individuals (IDENT_IND) reported more than one long-distance trip. Should we proceed as with K_DEPLOC and keep only the first one? If not, use "IDENT_VOY" instead
    "IDENT_VOY",
    #"POIDS_VOY13",                                      # Weight of the holiday/excursion. There is also a "yearly weight" (column name: poids_annuel)
    "V2_DVO_DSV",                                       # Crowfly distance from the residence to the main trip destination
    "V2_OLDMOTPR",                                      # Main travel purpose. Then the origin purpose should always be home.
    "V2_OLDMTPP",                                       # Main mode
    "DEP",                                              # Residence department
    "V2_OLDVDEP",                                       # Destination department
    "Nbdep",                                            # Number of trips done during the holidays?
    "V2_OLDDEBJ",                                       # First day
    "V2_OLDFINJ"                                        # Last day
]

K_VOYAGE_DET_COLUMNS = [                                # Detailed description of what happens during one holiday "trip"
    "IDENT_IND",                                        # Individual identifier. See above.
    "IDENT_VOY",
    "POIDS_VOY13",                                      # Weight of the holiday/excursion. There is also a "yearly weight" (column name: poids_annuel)
    "OLDI",                                             # Trip number inside the tour
    "NBD",                                              # Number of trips inside the tour
    "V2_OLDKM",                                         # Travelled distance
    "V2_DVO_ODV",                                       # Crowfly distance
    "V2_OLDDEH",                                        # Departure hour
    "V2_OLDARH",                                        # Arrival hour
    "V2_OLDMOT",                                        # Purpose
    "V2_OLDMT1S",                                       # Mode
]

def configure(context):
    context.config("data_path")

def execute(context):
    df_individu = pd.read_csv(
        "%s/entd_2008/Q_individu.csv" % context.config("data_path"),
        sep = ";", encoding = "latin1", usecols = Q_INDIVIDU_COLUMNS,
        dtype = { "DEP": str }
    )

    df_tcm_individu = pd.read_csv(
        "%s/entd_2008/Q_tcm_individu.csv" % context.config("data_path"),
        sep = ";", encoding = "latin1", usecols = Q_TCM_INDIVIDU_COLUMNS,
        dtype = { "DEP": str }
    )

    df_menage = pd.read_csv(
        "%s/entd_2008/Q_menage.csv" % context.config("data_path"),
        sep = ";", encoding = "latin1", usecols = Q_MENAGE_COLUMNS,
        dtype = { "DEP": str }
    )

    df_tcm_menage = pd.read_csv(
        "%s/entd_2008/Q_tcm_menage_0.csv" % context.config("data_path"),
        sep = ";", encoding = "latin1", usecols = Q_TCM_MENAGE_COLUMNS,
        dtype = { "DEP": str }
    )

    df_deploc = pd.read_csv(
        "%s/entd_2008/K_deploc.csv" % context.config("data_path"),
        sep = ";", encoding = "latin1", usecols = K_DEPLOC_COLUMNS,
        dtype = { "DEP": str }
    )
    
    df_ld_general = pd.read_csv(
        "%s/entd_2008/K_voyage.csv" % context.config("data_path"),
        sep = ";", encoding = "latin1", usecols = K_VOYAGE_COLUMNS,
        dtype = { "DEP": str }
    )
    
    df_ld_detailed = pd.read_csv(
        "%s/entd_2008/K_voydepdet.csv" % context.config("data_path"),
        sep = ";", encoding = "latin1", usecols = K_VOYAGE_DET_COLUMNS#,
        #dtype = { "DEP": str }
    )

    return df_individu, df_tcm_individu, df_menage, df_tcm_menage, df_ld_general, df_ld_detailed

def validate(context):
    for name in ("Q_individu.csv", "Q_tcm_individu.csv", "Q_menage.csv", "Q_tcm_menage_0.csv", "K_deploc.csv", "K_voyage.csv", "K_voydepdet.csv"):
        if not os.path.exists("%s/entd_2008/%s" % (context.config("data_path"), name)):
            raise RuntimeError("File missing from ENTD: %s" % name)

    return [
        os.path.getsize("%s/entd_2008/Q_individu.csv" % context.config("data_path")),
        os.path.getsize("%s/entd_2008/Q_tcm_individu.csv" % context.config("data_path")),
        os.path.getsize("%s/entd_2008/Q_menage.csv" % context.config("data_path")),
        os.path.getsize("%s/entd_2008/Q_tcm_menage_0.csv" % context.config("data_path")),
        os.path.getsize("%s/entd_2008/K_deploc.csv" % context.config("data_path"))
    ]
