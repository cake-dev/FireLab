import os
from os.path import join as pjoin

import dask
import dask_geopandas as dgpd
import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from raster_tools import Raster, Vector, open_vectors, clipping, zonal

# Location for temporary storage
TMP_LOC = "/home/jake/FireLab/Project/data/temp/"
DATA_LOC = "/home/jake/FireLab/Project/data/"
STATE = "OR"

# Location of clipped DEM files
DEM_DATA_DIR = pjoin(TMP_LOC, "dem_data")

# Paths to data
PATHS = {
    "states": pjoin(DATA_LOC, "terrain/state_borders/cb_2018_us_state_5m.shp"),
    "dem": pjoin(DATA_LOC, "terrain/us_orig_dem/us_orig_dem/orig_dem/hdr.adf"),
    "mtbs_perim": pjoin(DATA_LOC, "MTBS_Data/mtbs_perimeter_data/mtbs_perims_DD.shp"),
}

def get_state_dem_path(dem_key, state):
    return pjoin(DEM_DATA_DIR, f"{state}_{dem_key}.tif")

def extract_dem_data(df, key):
    state = df.state.values[0]
    path = get_state_dem_path(key, state)
    rs = Raster(path)
    if type(df) == pd.DataFrame:
        df = gpd.GeoDataFrame(df)
    feat = Vector(df, len(df))
    rdf = (
        zonal.extract_points_eager(feat, rs, skip_validation=True)
        .drop(columns=["band"])
        .compute()
    )
    df[key].values[:] = rdf.extracted.values
    return df

# MAIN 
if __name__ == "__main__":
    # State borders
    print("Loading state borders")
    stdf = open_vectors(PATHS["states"], 0).data.to_crs("EPSG:5071")
    states = {st: stdf[stdf.STUSPS == st].geometry for st in list(stdf.STUSPS)}
    state_shape = states[STATE]

    # MTBS Perimeters
    print("Loading MTBS perimeters")
    perimdf = open_vectors(PATHS["mtbs_perim"]).data.to_crs("EPSG:5071")
    state_fire_perims = perimdf.clip(state_shape.compute())
    
    # Clip DEM to state boundary and save (if not already clipped)
    dem_path = PATHS["dem"]
    dem_out_path = get_state_dem_path("dem", STATE)
    if not os.path.exists(dem_out_path):
        print("Clipping DEM to state boundary")
        rs = Raster(dem_path)
        (bounds,) = dask.compute(state_shape.total_bounds)
        clipped_dem = clipping.clip_box(rs, bounds)
        clipped_dem.save(dem_out_path)

    # Extract DEM data for each point in the perimeters DataFrame
    print("Extracting DEM data for points")
    df = state_fire_perims.assign(dem=np.nan)
    df = df[["geometry", "dem", "Event_ID"]]
    df = df.map_partitions(extract_dem_data, "dem")
    df = df.compute()

    # Now you have the point DataFrame with geometry and DEM columns
    print(df.head())