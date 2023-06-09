import subprocess

# Set the desired area of interest
# Change the values below to match the desired area of interest (i reccommend something like http://bboxfinder.com/ to get the coordinates)
STATE = "Oregon"
LONG_MIN = -124.85
LONG_MAX = -116.33
LAT_MIN = 41.86
LAT_MAX = 46.23

# Loop through years 2020 to 1986 to download biomass data
years = list(range(2020, 1985, -1))

for year in years:
    # Download biomass data
    subprocess.run(["gdal_translate", "-co", "compress=lzw", "-co", "tiled=yes", "-co", "bigtiff=yes",
                    f"/vsicurl/http://rangeland.ntsg.umt.edu/data/rap/rap-vegetation-biomass/v3/vegetation-biomass-v3-{year}.tif",
                    "-projwin", str(LONG_MIN), str(LAT_MAX), str(LONG_MAX), str(LAT_MIN),
                    f"out{year}_{STATE}.tif"], check=True)

    # Convert to netCDF format
    subprocess.run(["gdal_translate", "-of", "netCDF", "-co", "FORMAT=NC4",
                    f"out{year}_{STATE}.tif", f"{year}_biomass_{STATE}.nc"], check=True)

    # Remove temporary files
    subprocess.run(["rm", "*.tif"], check=True)