# filename: ndvi_dl.sh
# download ndvi data from NOAA (CONUS)
for year in 2020 2019 2018 2017 2016 2015 2014 2013 2012 2011 2010 2009 2008 2007 2006 2005 2004 2003 2002 2001 2000 1999 1998 1997 1996 1995 1994 1993 1992 1991 1990 1989 1988 1987 1986; do
  wget -erobots=off -nv -m -np -nH --cut-dirs=2 --reject "index.html*" https://www.ncei.noaa.gov/data/land-normalized-difference-vegetation-index/access/${year}/
done