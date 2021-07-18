#!/bin/bash

#== example for commandline arguments:
#if test "$#" -ne 1; then
#    echo -e "\nMust give year for GSOD weather data download as single command# line argument."
#    exit
#fi
#YEAR=$1

##################
# raw file paths #
##################
#=================================
# Soucy and Berry's Covid19Canada
#=================================
#
# https://opencovid.ca and https://github.com/ccodwg/Covid19Canada
#
# cases and deaths (daily and cumulative) by health region (no hr-uid)
#    ["province","health_region","date_report","cases","cumulative_cases"
# e.g.
#    ["Ontario","Toronto","25-01-2020",1,1]
#
address_covid19canada_cases=https://raw.githubusercontent.com/ccodwg/Covid19Canada/master/timeseries_hr/cases_timeseries_hr.csv
address_covid19canada_deaths=https://raw.githubusercontent.com/ccodwg/Covid19Canada/master/timeseries_hr/mortality_timeseries_hr.csv
#======================
# NYTimes Covid19 data
#======================
# https://github.com/nytimes/covid-19-data
#
# cumulative data (cases,deaths) by date for counties (w/ combined FIPS)
#    [date,county,state,fips,cases,deaths]
# e.g.,
#    [2020-03-13,Saratoga,New York,36091,3,0]
#
address_nytimes_counties=https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv
#==================
# JHU Covid19 data
#==================
# https://github.com/CSSEGISandData/COVID-19
# dashboard: https://www.arcgis.com/apps/opsdashboard/index.html#/bda7594740fd40299423467b48e9ecf6
#
# cumulative cases and deaths by county
#    [UID,iso2,iso3,code3,FIPS,Admin2,Province_State,Country_Region,
#     Lat,Long_,Combined_Key,Population,1/22/20,1/23/20,...,[today]]
# e.g.
#    [84036091,US,USA,840,36091.0,Saratoga,New York,US,
#     43.10904162,-73.86653895,"Saratoga, New York, US",229863,0,...]
# 
address_jhu_cases=https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_US.csv
address_jhu_deaths=https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_US.csv


filearr=(
    $address_covid19canada_cases $address_covid19canada_deaths
    $address_nytimes_counties
    $address_jhu_cases $address_jhu_deaths
)

doy=`date +%j`
doymod5=`expr ${doy} % 5`
count=0
for site in "${filearr[@]}"
do
    # set directory name
    if [ $count -lt 2 ]
    then
	dir=covid19canada
    elif [ $count -eq 2 ]
    then
       dir=nytimes
    else
	dir=jhu
    fi

    # get filename
    filename=${site##*/}

    # save copy of old file
    echo mv ${dir}/${filename} ${dir}/old
    mv ${dir}/${filename} ${dir}/old

    # download file to directory
    echo wget -P ${dir} ${site}
    wget -P ${dir} ${site}    
    
    # increment counter
    let count=count+1
done
