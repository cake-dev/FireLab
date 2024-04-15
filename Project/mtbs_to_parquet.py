import os
import tempfile
import warnings
from os.path import join as pjoin
import dask.dataframe as dd
import dask_geopandas as dgpd
import tqdm
from dask.diagnostics import ProgressBar

from raster_tools import Raster, Vector, open_vectors, clipping, zonal
from raster_tools.dtypes import U8

# Filter out warnings from dask_geopandas and dask
warnings.filterwarnings(
    "ignore", message=".*initial implementation of Parquet.*"
)
warnings.filterwarnings(
    "ignore", message=".*Slicing is producing a large chunk.*"
)

# Location for temporary storage
TMP_LOC = "/home/jake/FireLab/Project/data/temp/"

STATE = "OR"
YEARS = [2020]

# location of data files
DATA_LOC = "/home/jake/FireLab/Project/data/"
EDNA_DIR = pjoin(DATA_LOC, "terrain")
MTBS_DIR = pjoin(DATA_LOC, "MTBS_Data")

OUT_PATH = pjoin(DATA_LOC, "mtbs_output")

PATHS = {
    "states": pjoin(EDNA_DIR, "state_borders/cb_2018_us_state_5m.shp"),
    "mtbs_root": pjoin(MTBS_DIR, "MTBS_BSmosaics/"),
    "mtbs_perim": pjoin(MTBS_DIR, "mtbs_perimeter_data/mtbs_perims_DD.shp"),
}

def build_mtbs_year_df(path, perims_df, state_label):
    rs = Raster(path)
    dfs = []
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
        print("built df: ", df.head())
        with ProgressBar():
            df.to_parquet(out_path)
    return dgpd.read_parquet(out_path)

if __name__ == "__main__":

    if 1:
        # State borders
        print("Loading state borders")
        stdf = open_vectors(PATHS["states"], 0).data.to_crs("EPSG:5071")
        states = {st: stdf[stdf.STUSPS == st].geometry for st in list(stdf.STUSPS)}
        state_shape = states[STATE]
        states = None
        stdf = None

        # MTBS Perimeters
        print("Loading MTBS perimeters")
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
        state_fire_perims = state_fire_perims[state_fire_perims.Ig_Date.dt.year.between(2018, 2020)]
        year_to_perims = {
            y: state_fire_perims[state_fire_perims.Ig_Date.dt.year == y]
            for y in YEARS
        }
        state_fire_perims = None

        year_to_mtbs_file = {
            y: pjoin(PATHS["mtbs_root"], f"mtbs_{STATE}_{y}.tif")
            for y in YEARS
        }

    if 1:
        # code below creates the parquet file from the mtbs data
        df = build_mtbs_df(
            YEARS,
            year_to_mtbs_file,
            year_to_perims,
            STATE,
            out_path=pjoin(OUT_PATH, f"{STATE}_mtbs.parquet")
        )