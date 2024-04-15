import os
import time
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

# Filter out warnings from dask_geopandas and dask
warnings.filterwarnings(
    "ignore", message=".*initial implementation of Parquet.*"
)
warnings.filterwarnings(
    "ignore", message=".*Slicing is producing a large chunk.*"
)


# Location for temporary storage
TMP_LOC = "/home/jake/FireLab/Project/data/temp/"
TMP_LOC2 = "/home/jake/FireLab/Project/data/temp2/"
TMP_LOC3 = "/home/jake/FireLab/Project/data/temp3/"
DATA_LOC = "/home/jake/FireLab/Project/data/"

STATE = "OR"

# Location of clipped DEM files
DEM_DATA_DIR = pjoin(TMP_LOC, "dem_data")

# location of feature data files
FEATURE_DIR = pjoin(DATA_LOC, "FeatureData")
EDNA_DIR = pjoin(DATA_LOC, "terrain")
MTBS_DIR = pjoin(DATA_LOC, "MTBS_Data")
VIIRS_DIR = pjoin(DATA_LOC, "viirs_data")

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
    "viirs_root": VIIRS_DIR,
    "viirs_perim": pjoin(VIIRS_DIR, "viirs_perims_shapefile.shp"),
}

YEARS = list(range(2018, 2021))
GM_KEYS = list(filter(lambda x: x.startswith("gm_"), PATHS))
AW_KEYS = list(filter(lambda x: x.startswith("aw_"), PATHS))
DM_KEYS = list(filter(lambda x: x.startswith("dm_"), PATHS))
BIOMASS_KEYS = list(filter(lambda x: x.startswith("biomass_"), PATHS))
LANDFIRE_KEYS = list(filter(lambda x: x.startswith("landfire_"), PATHS))
NDVI_KEYS = list(filter(lambda x: x.startswith("ndvi"), PATHS))
DEM_KEYS = list(filter(lambda x: x.startswith("dem"), PATHS))
# drop dem from DEM_KEYS
DEM_KEYS = [x for x in DEM_KEYS if x != "dem"]

# NC_KEYSET = set(GM_KEYS + DM_KEYS + BIOMASS_KEYS + NDVI_KEYS)
NC_KEYSET = [DM_KEYS, GM_KEYS, BIOMASS_KEYS, NDVI_KEYS]
TIF_KEYSET = [AW_KEYS, LANDFIRE_KEYS] 

MTBS_DF_PATH = pjoin(TMP_LOC, f"{STATE}_mtbs.parquet")
MTBS_DF_PARQUET_PATH_NEW = pjoin(TMP_LOC, f"{STATE}_mtbs_new.parquet")
MTBS_DF_TEMP_PATH = pjoin(TMP_LOC, f"{STATE}_mtbs_temp.parquet")
MTBS_DF_TEMP_PATH_2 = pjoin(TMP_LOC, f"{STATE}_mtbs_temp_2.parquet")
CHECKPOINT_1_PATH = pjoin(TMP_LOC, "check1")
CHECKPOINT_2_PATH = pjoin(TMP_LOC, "check2")
CHECKPOINT_3_PATH = pjoin(TMP_LOC, "check3")
CHECKPOINT_4_PATH = pjoin(TMP_LOC, "check4")

def get_state_dem_path(dem_key, state):
    return pjoin(DEM_DATA_DIR, f"{state}_{dem_key}.tif")

def clip_and_save_dem_rasters(keys, paths, feature, state):
    feature = feature.compute()
    for k in tqdm.tqdm(keys, ncols=80, desc="DEM Clipping"):
        path = paths[k]
        out_path = get_state_dem_path(k, state)
        if os.path.exists(out_path):
            continue
        rs = Raster(path)
        (bounds,) = dask.compute(feature.to_crs(rs.crs).total_bounds)
        crs = clipping.clip_box(rs, bounds)
        crs.save(out_path)

def extract_fire_data(fire_perimeters_path, dem_path, output_path):
    """
    Extracts data from fire perimeters and DEM, creating a parquet file.

    Args:
        fire_perimeters_path (str): Path to the fire perimeters shapefile.
        dem_path (str): Path to the DEM raster file.
        output_path (str): Path to save the output parquet file.
    """
    # Load fire perimeters and ensure point geometry
    fire_perimeters = gpd.read_file(fire_perimeters_path)
    if not fire_perimeters.geom_type.eq("Point").all():
        raise ValueError("Fire perimeters must have point geometry.")
    
    # Load DEM raster
    dem_raster = Raster(dem_path)
    
    # Ensure CRS match
    if fire_perimeters.crs != dem_raster.crs:
        fire_perimeters = fire_perimeters.to_crs(dem_raster.crs)

    # Extract DEM values for each fire point
    fire_vector = Vector(fire_perimeters)
    extracted_data = zonal.extract_points_eager(fire_vector, dem_raster)

    # Create final dataframe
    final_df = fire_perimeters[["fire_id", "ig_date", "geometry"]]
    final_df["dem"] = extracted_data["extracted"]

    # Save as parquet
    final_df.to_parquet(output_path)

# Example usage
# State borders
print("Loading state borders")
stdf = open_vectors(PATHS["states"], 0).data.to_crs("EPSG:5071")
states = {st: stdf[stdf.STUSPS == st].geometry for st in list(stdf.STUSPS)}
state_shape = states[STATE]
states = None
stdf = None
# MTBS Perimeters
# print("Loading MTBS perimeters")
# perimdf = open_vectors(PATHS["mtbs_perim"]).data.to_crs("EPSG:5071")
print("Loading VIIRS perimeters")
perimdf = open_vectors(PATHS["viirs_perim"]).data.to_crs("EPSG:5071")
# perimdf = dgpd.read_parquet(DATA_LOC + "viirs_perims.parquet").compute().to_crs("EPSG:5071")
# perimdf = perimdf.rename(columns={"t": "Ig_Date"})
state_fire_perims = perimdf.clip(state_shape.compute())
state_fire_perims = (
    state_fire_perims.assign(
        Ig_Date=lambda frame: dd.to_datetime(
            frame.Ig_Date, format="%Y-%m-%d"
        )
    )
    .sort_values("Ig_Date")
    .compute()
)
state_fire_perims = state_fire_perims[state_fire_perims.Ig_Date.dt.year.between(2018, 2020)]
year_to_perims = {
    y: state_fire_perims[state_fire_perims.Ig_Date.dt.year == y]
    for y in YEARS
}
state_fire_perims = None
# get fire from 2018 w/ longest duration
fire = year_to_perims[2018]
fire = fire[fire.duration == fire.duration.max()]
print(fire)
# convert fire to point geometry
fire = gpd.GeoDataFrame(fire, geometry=gpd.points_from_xy(fire.lon, fire.lat))
fire = Vector(fire)
print(fire)
# fire_perimeters_path = "path/to/fire_perimeters.shp"
# print("Clipping DEMs")
# clip_and_save_dem_rasters(DEM_KEYS, PATHS, state_shape, STATE)
# dem_path = "path/to/dem.tif"
# output_path = "fire_data.parquet"
# extract_fire_data(fire_perimeters_path, dem_path, output_path)