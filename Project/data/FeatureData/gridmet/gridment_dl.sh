# filename: gridmet_dl.sh
for year in 2020 2019 2018 2017 2016 2015 2014 2013 2012 2011 2010 2009 2008 2007 2006 2005 2004 2003 2002 2001 2000 1999 1998 1997 1996 1995 1994 1993 1992 1991 1990 1989 1988 1987 1986;
do
    wget -nc -c -nd http://www.northwestknowledge.net/metdata/data/vpd_${year}.nc
    wget -nc -c -nd http://www.northwestknowledge.net/metdata/data/srad_${year}.nc
    wget -nc -c -nd http://www.northwestknowledge.net/metdata/data/pdsi_${year}.nc
done

#gridmet
for var in vpd srad pdsi;
do
    cdo -f nc4 -z zip cat ${var}_*.nc ${var}_1986-2020.nc
    cdo -f nc4 -z zip -timselmean,7 ${var}_1986-2020.nc ${var}_1986_2020_weekly.nc
done