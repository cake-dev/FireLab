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
import tqdm
import xarray as xr
from dask.diagnostics import ProgressBar

from raster_tools import Raster, Vector, open_vectors, clipping, zonal
from raster_tools.dtypes import F32, U8

# Filter out warnings from dask_geopandas and dask
warnings.filterwarnings(
    "ignore", message=".*initial implementation of Parquet.*"
)
warnings.filterwarnings(
    "ignore", message=".*Slicing is producing a large chunk.*"
)

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

PATHS = {
    "states": pjoin(EDNA_DIR, "state_borders/cb_2018_us_state_5m.shp"),
    "dem": pjoin(EDNA_DIR, "us_orig_dem/us_orig_dem/orig_dem/hdr.adf"),
    # "dem_slope": pjoin(EDNA_DIR, "us_slope/us_slope/slope/hdr.adf"),
    # "dem_aspect": pjoin(EDNA_DIR, "us_aspect/aspect/hdr.adf"),
    # "dem_flow_acc": pjoin(EDNA_DIR, "us_flow_acc/us_flow_acc/flow_acc/hdr.adf"),
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
YEARS = [2020]
GM_KEYS = list(filter(lambda x: x.startswith("gm_"), PATHS)) 
AW_KEYS = list(filter(lambda x: x.startswith("aw_"), PATHS)) 
DM_KEYS = list(filter(lambda x: x.startswith("dm_"), PATHS)) 
BIOMASS_KEYS = list(filter(lambda x: x.startswith("biomass_"), PATHS)) 
LANDFIRE_KEYS = list(filter(lambda x: x.startswith("landfire_"), PATHS)) 
NDVI_KEYS = list(filter(lambda x: x.startswith("ndvi"), PATHS)) 
DEM_KEYS = list(filter(lambda x: x.startswith("dem"), PATHS)) 


def get_gridmet_var_name(ds):
    # Find the data variable in a gridmet xarray.Dataset
    var_name = list(set(ds.keys()) - set(["crs", "day_bnds"]))[0]
    return var_name

def extract_date_from_fire_id_part(df):
    # Assuming 'fire_id' is the column with timestamp data
    df['ig_date'] = df['fire_id'].str[-8:]
    df['ig_date'] = dd.to_datetime(df['ig_date'], format='%Y%m%d')
    df['ig_date'] = df['ig_date'].dt.date
    return df


def netcdf_to_raster(path, date):
    # This produces a Dataset. We need to grab the DataArray inside that
    # contains the data of interest.
    gridmet_ds = xr.open_dataset(path, chunks={"day": 1})
    gridmet_ds = gridmet_ds.rio.write_crs(gridmet_ds.crs.spatial_ref)
    # Find variable name
    var_name = get_gridmet_var_name(gridmet_ds)
    # Extract
    var_da = gridmet_ds[var_name]
    var_da = var_da.sel(day=date, method="nearest")
    xrs = xr.DataArray(
        var_da.data, dims=("y", "x"), coords=(var_da.lat.data, var_da.lon.data)
    ).expand_dims("band")
    xrs["band"] = [1]
    # Set CRS in raster compliant format
    xrs = xrs.rio.write_crs(gridmet_ds.crs.spatial_ref)
    return Raster(xrs)


def extract_gridmet_data(df, gm_name):
    assert df.ig_date.unique().size == 1
    # print(f"{gm_name}: {df.columns = }, {len(df) = }")
    date = df.ig_date.values[0]
    # print(f"{gm_name}: starting {date}")
    rs = netcdf_to_raster(PATHS[gm_name], date)
    bounds = gpd.GeoSeries(df.geometry).to_crs(rs.crs).total_bounds
    rs = clipping.clip_box(rs, bounds)
    if type(df) == pd.DataFrame:
        df = gpd.GeoDataFrame(df)
    feat = Vector(df, len(df))
    rdf = (
        zonal.extract_points_eager(feat, rs, skip_validation=True)
        .drop(columns=["band"])
        .rename(columns={"extracted": gm_name})
        .compute()
    )
    df[gm_name].values[:] = rdf[gm_name].values
    # print(f"{gm_name}: finished {date}")
    return df


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
        zonal.extract_points_eager(feat, rs, skip_validation=True) # COULD BE SCRAMBLERD HERE CHECK ORDER
        .drop(columns=["band"])
        .compute()
    )
    df[key].values[:] = rdf.extracted.values
    return df


def partition_extract_gridmet(df, key):
    # This func wraps extract_gridmet_data. It groups the partition in to sub
    # dataframes with the same date and then applies extract_gridmet_data to
    # each and reassembles the results into an output dataframe.
    parts = []
    for group in df.groupby("ig_date", sort=True):
        _, gdf = group
        parts.append(extract_gridmet_data(gdf, key))
    return pd.concat(parts)


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


def build_mtbs_year_df(path, perims_df, state_label):
    rs = Raster(path)
    dfs = []
    # for grp in perims_df.groupby("Event_ID"):
    #     event_id, perim = grp
    #     df = (
    #         clipping.clip(perim, rs)
    #         .to_vector()
    #         .rename(columns={"value": "mtbs"})
    #         .drop(columns=["band", "row", "col"])
    #         .assign(state=state_label, fire_id=event_id)
    #         .astype({"mtbs": U8})
    #     )
    #     dfs.append(df)
    # return dd.concat(dfs)
    for grp in perims_df.groupby("Ig_Date"):
        date, perim = grp
        df = (
            clipping.clip(perim, rs)
            .to_vector()
            .rename(columns={"value": "mtbs"})
            .drop(columns=["band", "row", "col"])
            .assign(state=state_label, ig_date=date)
            .astype({"mtbs": U8})
        )
        dfs.append(df)
    return dd.concat(dfs)


def _build_mtbs_df(
    years, year_to_mtbs_file, year_to_perims, state, working_dir
):
    dfs = []
    it = tqdm.tqdm(years, ncols=80, desc="MTBS")
    for y in it:
        mtbs_path = year_to_mtbs_file[y]
        if not os.path.exists(mtbs_path):
            it.write(f"No data for {y}")
            continue
        perims = year_to_perims[y]
        ydf = build_mtbs_year_df(mtbs_path, perims, state)
        ypath = pjoin(working_dir, str(y))
        ydf.compute().to_parquet(ypath)
        ydf = dgpd.read_parquet(ypath)
        dfs.append(ydf)
    return dd.concat(dfs)


def build_mtbs_df(
    years, year_to_mtbs_file, year_to_perims, state, out_path, tmp_loc=TMP_LOC
):
    print("Building mtbs df")
    with tempfile.TemporaryDirectory(dir=tmp_loc) as working_dir:
        df = _build_mtbs_df(
            years, year_to_mtbs_file, year_to_perims, state, working_dir
        )
        with ProgressBar():
            df.to_parquet(out_path)
    return dgpd.read_parquet(out_path)


def add_columns_to_df(
    df,
    columns,
    part_func,
    out_path,
    col_type=F32,
    col_default=np.nan,
    part_func_args=(),
    tmp_loc=TMP_LOC,
    parallel=True,
):
    print(f"Adding columns: {columns}")
    # Add columns
    expanded_df = df.assign(**{c: col_type.type(col_default) for c in columns})
    with tempfile.TemporaryDirectory(dir=tmp_loc) as working_dir:
        # Save to disk before applying partition function. to_parquet() has a
        # chance of segfaulting and that chance goes WAY up after adding
        # columns and then mapping a function to partitions. Saving to disk
        # before mapping keeps the odds low.
        path = pjoin(working_dir, "expanded")
        expanded_df.to_parquet(path)

        expanded_df = dgpd.read_parquet(path)
        meta = expanded_df._meta.copy()
        for c in columns:
            expanded_df = expanded_df.map_partitions(
                part_func, c, *part_func_args, meta=meta
            )

        if parallel:
            with ProgressBar():
                expanded_df.to_parquet(out_path)
        else:
            # Save parts in serial and then assemble into single dataframe
            with tempfile.TemporaryDirectory(dir=tmp_loc) as part_dir:
                dfs = []
                for i, part in enumerate(expanded_df.partitions):
                    # Save part i
                    part_path = pjoin(part_dir, f"part{i:04}")
                    with ProgressBar():
                        part.compute().to_parquet(part_path)
                    # Save paths for opening with dask_geopandas later. Avoid
                    # opening more dataframes in this loop as doing so will
                    # likely cause a segfault. I have no idea why.
                    dfs.append(part_path)
                dfs = [dgpd.read_parquet(p) for p in dfs]
                # Assemble and save to final output location
                expanded_df = dd.concat(dfs)
                with ProgressBar():
                    expanded_df.to_parquet(out_path)
    return dgpd.read_parquet(out_path)



if __name__ == "__main__":

    # State borders
    stdf = open_vectors(PATHS["states"], 0).data.to_crs("EPSG:5071")
    states = {st: stdf[stdf.STUSPS == st].geometry for st in list(stdf.STUSPS)}
    state_shape = states[STATE]
    states = None
    stdf = None

    # MTBS Perimeters
    perimdf = open_vectors(PATHS["mtbs_perim"]).data.to_crs("EPSG:5071")
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
    year_to_perims = {
        y: state_fire_perims[state_fire_perims.Ig_Date.dt.year == y]
        for y in YEARS
    }
    state_fire_perims = None

    year_to_mtbs_file = {
        y: pjoin(PATHS["mtbs_root"], f"mtbs_{STATE}_{y}.tif")
        for y in YEARS
    }

    mtbs_df_path = pjoin(TMP_LOC, f"{STATE}_mtbs_NEW.parquet")
    mtbs_df_temp_path = pjoin(TMP_LOC, f"{STATE}_mtbs_temp.parquet")
    checkpoint_1_path = pjoin(TMP_LOC, "check1")
    checkpoint_2_path = pjoin(TMP_LOC, "check2")
    checkpoint_3_path = pjoin(TMP_LOC, "check3")

    if 1:
        df = build_mtbs_df(
            YEARS,
            year_to_mtbs_file,
            year_to_perims,
            STATE,
            out_path=mtbs_df_temp_path,
        )
        df = add_columns_to_df(
            df, GM_KEYS, partition_extract_gridmet, checkpoint_1_path
        )
        df = df.repartition(partition_size="100MB").reset_index(drop=True)
        print("Repartitioning")
        with ProgressBar():
            df.to_parquet(checkpoint_2_path)

    if 1:
        df = dgpd.read_parquet(checkpoint_2_path)
        # clip_and_save_dem_rasters(DEM_KEYS, PATHS, state_shape, STATE)
        df = add_columns_to_df(
            df,
            DEM_KEYS,
            extract_dem_data,
            checkpoint_3_path,
            # Save results in serial to avoid segfaulting. Something about the
            # dem computations makes segfaults extremely likely when saving
            # The computations require a lot of memory which may be what
            # triggers the fault.
            parallel=False,
        )
        df = df.repartition(partition_size="100MB").reset_index(drop=True)
        print("Repartitioning")
        with ProgressBar():
            df.to_parquet(mtbs_df_path)