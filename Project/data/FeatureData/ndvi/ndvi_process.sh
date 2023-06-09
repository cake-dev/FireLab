# This script concatenates the daily NDVI files into a single yearly file and then calculates the weekly average NDVI for each year.  It also removes unneccesary variables. 
# After that, it combines all of the years into a single file.
for year in 2020 2019 2018 2017 2016 2015 2014 2013 2012 2011 2010 2009 2008 2007 2006 2005 2004 2003 2002 2001 2000 1999 1998 1997 1996 1995 1994 1993 1992 1991 1990 1989 1988 1987 1986; do
    cd ${year}/
    cdo cat *.nc ${year}_ndvi_daily.nc
    cdo -f nc4 -z zip -timselmean,7 ${year}_ndvi_daily.nc ${year}_ndvi_weeklyavg.nc
    rm ${year}_ndvi_daily.nc
    ncks -x -v TIMEOFDAY,QA ${year}_ndvi_weeklyavg.nc ${year}_ndvi_weeklyavg.nc
    mv ${year}_ndvi_weeklyavg.nc ../weekly
done

cd ../weekly
cdo cat *.nc ndvi_1986_2020_weeklyavg.nc