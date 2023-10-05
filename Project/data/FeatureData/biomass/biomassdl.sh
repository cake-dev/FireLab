# filename: biomass_dl.sh
# Description: Download biomass data from the Rangeland Assessment Program (RAP) website and convert to netCDF
# Change the values below to match the desired area of interest (i recommend something like http://bboxfinder.com/ to get the coordinates)
STATE=OR
LONG_MIN=-124.85
LONG_MAX=-116.33
LAT_MIN=41.86
LAT_MAX=46.23

# loop through years 2020 to 1986 to download biomass data

for year in 2020 2019 2018; do # 2017 2016 2015 2014 2013 2012 2011 2010 2009 2008 2007 2006 2005 2004 2003 2002 2001 2000 1999 1998 1997 1996 1995 1994 1993 1992 1991 1990 1989 1988 1987 1986; do
  gdal_translate -co compress=lzw -co tiled=yes -co bigtiff=yes /vsicurl/http://rangeland.ntsg.umt.edu/data/rap/rap-vegetation-biomass/v3/vegetation-biomass-v3-${year}.tif -projwin LONG_MIN LAT_MAX LONG_MAX LAT_MIN out${year}_${STATE}.tif
  gdal_translate -of netCDF -co "FORMAT=NC4" -co "ZLEVEL=5" out${year}_${STATE}.tif ${year}_biomass_${STATE}.nc
  rm *.tif
  cdo -f nc4 -z zip5 copy ${year}_biomass_${STATE}.nc ${year}_biomass_${STATE}_compressed.nc
  # rm ${year}_biomass_${STATE}.nc
done