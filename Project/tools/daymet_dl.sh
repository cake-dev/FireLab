# filename: daymet_dl.sh
#for year in 2020 2019 2018 2017 2016 2015 2014 2013 2012 2011 2010 2009 2008 2007 2006 2005 2004 2003 2002 2001 2000 1999 1998 1997 1996 1995 1994 1993 1992 1991 1990 1989 1988 1987 1986;
for year in 2009 2008 2007 2006 2005 2004 2003 2002 2001 2000 1999 1998 1997 1996 1995 1994 1993 1992 1991 1990 1989 1988 1987 1986;
do
    wget -nc -c -nd https://thredds.daac.ornl.gov/thredds/fileServer/ornldaac/2131/daymet_v4_tmax_monavg_na_${year}.nc
    wget -nc -c -nd https://thredds.daac.ornl.gov/thredds/fileServer/ornldaac/2131/daymet_v4_tmin_monavg_na_${year}.nc
done

#daymet
for var in tmax tmin;
do
    cdo -f nc4 -z zip cat daymet_v4_${var}_monavg_na_*.nc ${var}_1986_2020.nc
done
