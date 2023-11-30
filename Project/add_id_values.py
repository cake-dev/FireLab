import os
import tempfile
import warnings
from os.path import join as pjoin

import dask
import dask.dataframe as dd
import dask_geopandas as dgpd
import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from scipy.fft import dst
import tqdm
import xarray as xr
from dask.diagnostics import ProgressBar
from rasterio.crs import CRS

from raster_tools import Raster, Vector, open_vectors, clipping, zonal
from raster_tools.dtypes import F32, U8, U16

import matplotlib.pyplot as plt

# change pandas max col display
pd.set_option('display.max_columns', 500)

# Location for temporary storage
TMP_LOC = "/home/jake/FireLab/Project/data/temp/"
DATA_LOC = "/home/jake/FireLab/Project/data/"

STATE = "OR"

# Location of clipped DEM files
DEM_DATA_DIR = pjoin(TMP_LOC, "dem_data")

# location of feature data files
FEATURE_DIR = pjoin(DATA_LOC, "FeatureData")
EDNA_DIR = pjoin(DATA_LOC, "terrain")
MTBS_DIR = pjoin(DATA_LOC, "MTBS_Data")

mtbs_df_path = pjoin(TMP_LOC, f"{STATE}_mtbs.parquet/")
mtbs_df_parquet_path = pjoin(TMP_LOC, f"{STATE}_mtbs_new.parquet")
mtbs_df_temp_path = pjoin(TMP_LOC, f"{STATE}_mtbs_temp.parquet/")
checkpoint_1_path = pjoin(TMP_LOC, "check1")
checkpoint_2_path = pjoin(TMP_LOC, "check2")

PATHS = {
    "states": pjoin(EDNA_DIR, "state_borders/cb_2018_us_state_5m.shp"),
    "dem": pjoin(EDNA_DIR, "us_orig_dem/us_orig_dem/orig_dem/hdr.adf"),
    "dem_slope": pjoin(EDNA_DIR, "us_slope/us_slope/slope/hdr.adf"),
    "dem_aspect": pjoin(EDNA_DIR, "us_aspect/aspect/hdr.adf"),
    "dem_flow_acc": pjoin(EDNA_DIR, "us_flow_acc/us_flow_acc/flow_acc/hdr.adf"),
    "gm_srad": pjoin(FEATURE_DIR, "gridmet/srad_1986_2020_weekly.nc"),
    "gm_vpd": pjoin(FEATURE_DIR, "gridmet/vpd_1986_2020_weekly.nc"),
    "aw_mat": pjoin(FEATURE_DIR, "adaptwest/Normal_1991_2020_MAT.tif"),
    "aw_mcmt": pjoin(FEATURE_DIR, "adaptwest/Normal_1991_2020_MCMT.tif"),
    "aw_mwmt": pjoin(FEATURE_DIR, "adaptwest/Normal_1991_2020_MWMT.tif"),
    "aw_td": pjoin(FEATURE_DIR, "adaptwest/Normal_1991_2020_TD.tif"),
    "dm_tmax": pjoin(FEATURE_DIR, "daymet/tmax_1986_2020.nc"),
    "dm_tmin": pjoin(FEATURE_DIR, "daymet/tmin_1986_2020.nc"),
    "biomass_afg": pjoin(
        FEATURE_DIR, "biomass/biomass_afg_1986_2020_{}.nc".format(STATE)
    ),
    "biomass_pfg": pjoin(
        FEATURE_DIR, "biomass/biomass_pfg_1986_2020_{}.nc".format(STATE)
    ),
    "landfire_fvt": pjoin(
        FEATURE_DIR, "landfire/LF2020_FVT_200_CONUS/Tif/LC20_FVT_200.tif"
    ),
    "landfire_fbfm40": pjoin(
        FEATURE_DIR, "landfire/LF2020_FBFM40_200_CONUS/Tif/LC20_F40_200.tif"
    ),
    "ndvi": pjoin(FEATURE_DIR, "ndvi/access/weekly/ndvi_1986_2020_weekavg.nc"),
    "mtbs_root": pjoin(MTBS_DIR, "MTBS_BSmosaics/"),
    "mtbs_perim": pjoin(MTBS_DIR, "mtbs_perimeter_data/mtbs_perims_DD.shp"),
}
YEARS = list(range(2016, 2021))
GM_KEYS = list(filter(lambda x: x.startswith("gm_"), PATHS))
AW_KEYS = list(filter(lambda x: x.startswith("aw_"), PATHS))
DM_KEYS = list(filter(lambda x: x.startswith("dm_"), PATHS))
BIOMASS_KEYS = list(filter(lambda x: x.startswith("biomass_"), PATHS))
LANDFIRE_KEYS = list(filter(lambda x: x.startswith("landfire_"), PATHS))
NDVI_KEYS = list(filter(lambda x: x.startswith("ndvi"), PATHS))
DEM_KEYS = list(filter(lambda x: x.startswith("dem"), PATHS))

mtbs_df_2018_2020 = dgpd.read_parquet(checkpoint_2_path)

# load mtbs perimeters
print("Loading MTBS perimeters")
mtbs_perim = gpd.read_file(PATHS["mtbs_perim"])
mtbs_perim['Ig_Date'] = pd.to_datetime(mtbs_perim['Ig_Date'])
mtbs_perim.columns

# extract only the columns we need (Event_ID where startswith OR, Ig_Date, and geometry)
mtbs_perim = mtbs_perim[["Event_ID", "Ig_Date", "geometry"]]
mtbs_perim = mtbs_perim[mtbs_perim.Event_ID.str.startswith("OR")]
# drop rows where Ig_Date before 1986 or after 2020
mtbs_perim = mtbs_perim[mtbs_perim.Ig_Date.dt.year.between(2018, 2020)]
mtbs_perim.reset_index(drop=True, inplace=True)
mtbs_perim = mtbs_perim.to_crs(mtbs_df_2018_2020.crs)

def spatial_join(partition, mtbs_perim):
    # Convert the Dask partition to a GeoDataFrame
    gdf = gpd.GeoDataFrame(partition, geometry='geometry', crs=mtbs_perim.crs)

    # Perform the spatial join
    joined = gpd.sjoin(gdf, mtbs_perim, how="inner", predicate="intersects")

    # Filter by date if needed
    joined = joined[joined['ig_date'] == joined['Ig_Date']]
    # print(joined)

    return joined

result = mtbs_df_2018_2020.map_partitions(spatial_join, mtbs_perim, align_dataframes=False)
print(len(result))

with ProgressBar():
    final_result = result.compute()

final_result.drop(columns=['index_right', 'Ig_Date'], inplace=True)

# drop null values
final_result.dropna(inplace=True)
# adds unique id to each pixel
final_result.reset_index(inplace=True)
final_result['index'] = final_result.index
# rename index to unique_id
final_result.rename(columns={'index': 'unique_id'}, inplace=True)

with ProgressBar():
    final_result.to_parquet(mtbs_df_parquet_path)