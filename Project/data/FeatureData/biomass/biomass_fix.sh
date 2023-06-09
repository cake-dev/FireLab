# filename: biomass_fix.sh
# Description: This script fixes the dates and bands of the biomass data, and splits the data into individual bands (AFG and PFG)

STATE=Oregon

for year in 2020 2019 2018 2017 2016 2015 2014 2013 2012 2011 2010 2009 2008 2007 2006 2005 2004 2003 2002 2001 2000 1999 1998 1997 1996 1995 1994 1993 1992 1991 1990 1989 1988 1987 1986; do
    if [ $(($year % 4)) -eq 0 ]; then
        year_days = 366
        cdo settaxis,${year}-01-01,00:00,${year_days}days ${year}_biomass_${STATE}.nc ${year}_biomass_${STATE}_fixed.nc 
        cdo splitvar ${year}_biomass_${STATE}_fixed.nc ${STATE}_${year}_biomass_
        rm ${year}_biomass_${STATE}.nc
        rm ${year}_biomass_${STATE}_fixed.nc
    else
        year_days = 365
        cdo settaxis,${year}-01-01,00:00,${year_days}days ${year}_biomass_${STATE}.nc ${year}_biomass_${STATE}_fixed.nc 
        cdo splitvar ${year}_biomass_fixed.nc ${STATE}_${year}_biomass_
        rm ${year}_biomass_${STATE}.nc
        rm ${year}_biomass_${STATE}_fixed.nc
    fi
done

#(note:(b1 = afg, b2 = pfg))
mkdir b1
mkdir b2
mv *Band1.nc b1/
mv *Band2.nc b2/
cd b1
cdo -f nc4 -z zip cat *.nc biomass_afg_1986_2020_${STATE}.nc 
cd ../b2
cdo -f nc4 -z zip cat *.nc biomass_pfg_1986_2020_${STATE}.nc