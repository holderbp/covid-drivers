import pandas as pd
import numpy as np
import geopandas as gpd

##########################
# Parameters and Options #
##########################
do_fips_dma_collection = False # (set to True to re-run; see pars/files in function)
outfilename_statecounty_fips = "UScounty_fips_dma.csv"


##################################################################
# Collect and output the County/State FIPS, and DMA (Metro) data #
##################################################################
def output_fips_dma_file():
    #=== File paths and output file name
    #       Use same source for FIPS as PWPD and
    #       same source for DMAs as Google Trends
    pwpd_dir = "../pwpd/"
    UScounty_shape_dir = "data/shapefiles/UScounties/"
    UScounty_shape_filepath = \
        pwpd_dir + UScounty_shape_dir \
        + "tl_2019_us_county/tl_2019_us_county.shp"
    USstate_fips_filepath = \
        pwpd_dir + UScounty_shape_dir \
        + "US-state_fips-codes.csv"
    trends_dir = "../trends/"
    countyDMA_filename = \
        trends_dir + "data/dma/county_dma_sood-gaurav-harvard-dataverse_edited.csv"
    #=== Load the shapes file and keep only FIPS information
    #    (these code snippets mostly taken from pwpd.py)
    print("=== Loading shapefile...")
    counties_df = gpd.read_file(UScounty_shape_filepath)
    counties_df = counties_df[['STATEFP', 'COUNTYFP', 'NAME', 'NAMELSAD']]
    counties_df.columns = \
        ['fips_state', 'fips_county', 'county', 'countylong']
    #=== Make FIPS codes integer-valued
    counties_df['fips_state'] = counties_df['fips_state'].astype(int)
    counties_df['fips_county'] = counties_df['fips_county'].astype(int)
    #=== Create a five-digit fips code
    counties_df['fips'] = \
        counties_df['fips_state'].astype(str).str.zfill(2) \
        + counties_df['fips_county'].astype(str).str.zfill(3)
    counties_df['fips'] = counties_df['fips'].astype(int)
    #=== Add the state name and abbreviation for each county
    #    using the file of US state FIPS codes
    print("=== Getting State names...")
    counties_df['state'] = ""
    counties_df['stateabb'] = ""
    statefips_df = pd.read_csv(USstate_fips_filepath)
    statefips_df['fips'] = statefips_df['fips'].astype(int)
    for index,row in counties_df.iterrows():
        fips = row.fips_state
        thestate = (statefips_df['fips'] == fips)
        counties_df.at[index,'state'] = \
            statefips_df[thestate]['name'].to_list()[0]
        counties_df.at[index,'stateabb'] = \
            statefips_df[thestate]['abb'].to_list()[0]
    #=== Add metro area
    #    (code copied mostly from get-trends-by-county.py)
    print("=== Getting Metro Areas...")
    counties_df['dma'] = 0
    counties_df['dmaname'] = ""
    DMA_df = pd.read_csv(countyDMA_filename)
    for index, row in counties_df.iterrows():
        #--- get fips data 
        sfips = row.fips_state
        cfips = row.fips_county
        name = row.county + "," + row.stateabb
        #print(sfips, cfips, name)
        #--- select out dma entry for that county
        cdma = DMA_df[(DMA_df.STATEFP == sfips)
                      & (DMA_df.CNTYFP == cfips)]
        if (cdma.size == 0):
            print(   "***Error not found***")
            dma = -1
            sabb = ''
        else:
            # find the DMA value
            dma = cdma['DMAINDEX'].to_numpy()[0]
            dmaname = cdma['shortDMA'].to_numpy()[0]
        #--- place into dataframe
        counties_df.at[index,'dma'] = dma
        counties_df.at[index,'dmaname'] = dmaname
    #=== Rearrange the columns
    counties_df = \
        counties_df[['fips_state', 'fips_county', 'fips', 'state',
                     'stateabb', 'county', 'countylong', 'dma', 'dmaname']]
    #=== Create new FIPS codes to align with NYT/JHU import data
    newfipdicts = [
        {
            'fips_state': 29,
            'fips_county': 901,
            'fips': 29901,
            'state': 'Missouri',
            'stateabb': 'MO',
            'county': 'Kansas City',
            'countylong': 'Kansas City (not a real FIPS)',
            'dma': 33,
            'dmaname': 'Kansas City'
        },
        {
            'fips_state': 36,
            'fips_county': 901,
            'fips': 36901,
            'state': 'New York',
            'stateabb': 'NY',
            'county': 'New York City',
            'countylong': 'New York City (not a real FIPS)',
            'dma': 1,
            'dmaname': 'New York'
        },
        {
            'fips_state': 2,
            'fips_county': 901,
            'fips': 2901,
            'state': 'Alaska',
            'stateabb': 'AK',
            'county': 'Bristol Bay plus Lake and Peninsula',
            'countylong': 'Bristol Bay plus Lake and Peninsula (not a real FIPS)',
            'dma': 156,
            'dmaname': 'Anchorage'
        },
        {
            'fips_state': 2,
            'fips_county': 902,
            'fips': 2902,
            'state': 'Alaska',
            'stateabb': 'AK',
            'county': 'Yakutat plus Hoonah-Angoon',
            'countylong': 'Yakutat plus Hoonah-Angoon (not a real FIPS)',
            'dma': 206,
            'dmaname': 'Juneau'
        },
        {
            'fips_state': 49,
            'fips_county': 901,
            'fips': 49601,
            'state': 'Utah',
            'stateabb': 'UT',
            'county': 'HD Bear River',
            'countylong': 'Bear River Health District [3,5,33] (not a real FIPS)',
            'dma': 36,
            'dmaname': 'Salt Lake City'
        },        
        {
            'fips_state': 49,
            'fips_county': 902,
            'fips': 49902,
            'state': 'Utah',
            'stateabb': 'UT',
            'county': 'HD Central',
            'countylong': 'Central Health District [23,27,31,39,41,55] (not a real FIPS)',
            'dma': 36,
            'dmaname': 'Salt Lake City'
        },
        {
            'fips_state': 49,
            'fips_county': 903,
            'fips': 49903,
            'state': 'Utah',
            'stateabb': 'UT',
            'county': 'HD Southeast',
            'countylong': 'Southeast Health District [7,15,19] (not a real FIPS)',
            'dma': 36,
            'dmaname': 'Salt Lake City'
        },
        {
            'fips_state': 49,
            'fips_county': 904,
            'fips': 49904,
            'state': 'Utah',
            'stateabb': 'UT',
            'county': 'HD Southwest',
            'countylong': 'Southwest Health District [1,7,21,25,53] (not a real FIPS)',
            'dma': 36,
            'dmaname': 'Salt Lake City'
        },
        {
            'fips_state': 49,
            'fips_county': 905,
            'fips': 49905,
            'state': 'Utah',
            'stateabb': 'UT',
            'county': 'HD TriCounty',
            'countylong': 'TriCounty Health District [9,13,47] (not a real FIPS)',
            'dma': 36,
            'dmaname': 'Salt Lake City'
        },
        {
            'fips_state': 49,
            'fips_county': 906,
            'fips': 49906,
            'state': 'Utah',
            'stateabb': 'UT',
            'county': 'HD Weber-Morgan',
            'countylong': 'Weber-Morgan Health District [29,57] (not a real FIPS)',
            'dma': 36,
            'dmaname': 'Salt Lake City'
        },
        {
            'fips_state': 60,
            'fips_county': 0,
            'fips': 60000,
            'state': 'American Samoa',
            'stateabb': 'AS',
            'county': 'All',
            'countylong': 'American Samoa --- All (not a real FIPS)',
            'dma': 500,
            'dmaname': 'American Samoa'
        },
        {
            'fips_state': 69,
            'fips_county': 0,
            'fips': 69000,
            'state': 'Northern Mariana Islands',
            'stateabb': 'MP',
            'county': 'All',
            'countylong': 'Northern Mariana Islands --- All (not a real FIPS)',
            'dma': 520,
            'dmaname': 'N Mariana Islands'
        },
        {
            'fips_state': 78,
            'fips_county': 0,
            'fips': 78000,
            'state': 'Virgin Islands',
            'stateabb': 'VI',
            'county': 'All',
            'countylong': 'Virgin Islands --- All (not a real FIPS)',
            'dma': 530,
            'dmaname': 'Virgin Islands'
        },
        {
            'fips_state': 26,
            'fips_county': 901,
            'fips': 26901,
            'state': 'Michigan',
            'stateabb': 'MI',
            'county': 'MDOC',
            'countylong': 'Michigan DOC Facilities (not a real FIPS)',
            'dma': None,
            'dmaname': None
        }
    ]
    counties_df = counties_df.append(newfipdicts, ignore_index=True, sort=False)
    #=== Sort and output
    counties_df = counties_df.sort_values(['fips_state', 'fips_county'])
    counties_df.to_csv(outfilename_statecounty_fips, index=False)

#########################################################
# Load and arrange NYT and JHU Covid19 Cases and Deaths #
#########################################################
def load_nyt_jhu_covid():
    # Filenames of raw data
    #
    #     * NYTimes is cumulative [cases,deaths] with form
    #           [date, county <name>, state <name>,
    #            fips <5-digit>, cases <cum>, deaths <cum>]
    #
    #     * JHU is cases and deaths in separate files, each with form
    #           [UID, iso2, iso3, code3, FIPS <5-digit>,
    #            Admin2 <county name>, Province_State <state name>,
    #            Country_Region, Lat, Long_, Combined_Key <full name>,
    #            1/22/20, ... <all dates> ..., today]
    #
    # - Need to deal with the oddball entries of each.
    #   See "Geographic Exceptions" on their github page
    # - Need to take NYTimes cum->daily.
    # - Need to "transpose" the JHU file and take from cum->daily
    #
    # Want output form:
    #
    #   [sfips, cfips, date, cases, cases_cum, deaths, deaths_cum]
    #
    filename_nyt_raw = "rawdata/nytimes/us-counties.csv"
    filename_jhu_cases_raw = "rawdata/jhu/time_series_covid19_confirmed_US.csv"
    filename_jhu_deaths_raw = "rawdata/jhu/time_series_covid19_deaths_US.csv"

    #=== Read in NYTimes data
    nytraw_df = pd.read_csv(filename_nyt_raw)
    #=== Deal with the Kansas City entries
    #      NYTimes lists Kansas City, MO separately with no FIPS
    # ---> Create new FIPS 29901  (i.e., create in the FIPS/DMA collection section, above)
    print("=== NYTimes raw data: adjusting Kansas City")
    for index, row in \
        nytraw_df[(nytraw_df['county'] == "Kansas City")
                  & (nytraw_df['state'] == "Missouri")].iterrows():
        #print(row['date'])
        nytraw_df.at[index,'fips'] = 29901
    #=== Deal with the Joplin entries
    #      NYTimes lists Joplin, MO separately with no FIPS (starting 2020-06-25)
    # Move to Jasper County (29097) (southern part in Newton, but oh well)
    print("=== NYTimes raw data: adjusting Joplin")
    for index, row in \
        nytraw_df[(nytraw_df['county'] == "Joplin")
                  & (nytraw_df['state'] == "Missouri")].iterrows():
        #print(row['date'])
        nytraw_df.at[index, 'fips'] = 29097
    #=== Deal with the NYC entries
    #      NYTimes lists all five boroughs under "New York City" with no FIPS
    # ---> Create new FIPS 36901
    print("=== NYTimes raw data: adjusting NYC")    
    for index, row in \
        nytraw_df[(nytraw_df['county'] == "New York City")
                  & (nytraw_df['state'] == "New York")].iterrows():
        #print(row['date'])
        nytraw_df.at[index, 'fips'] = 36901
    #=== Deal with the Guam entries
    #    NYTimes lists the county for Guam as "Unknown" with no FIPS
    # ---> Change county to "Guam" and FIPS to its only one: 66010
    for index, row in \ 
        nytraw_df[nytraw_df['state'] == "Guam"].iterrows():
        nytraw_df.at[index, 'county'] = "Guam"
        nytraw_df.at[index, 'fips'] = 66010
    #=== Deal with Alaska combined-county entries
    #    NYTimes combines the following counties and gives them
    #    fake FIPS codes:
    #      * Bristol Bay Borough (2060) + Lake and Peninsula Borough (2164)
    #                ---> 2997
    #      * Yakutat City and Borough (2290) + Hoonah-Angoon Census Area (2105)
    #                ---> 2998
    # ---> Create new FIPS entries 2901 and 2901, respectively
    for index, row in \ 
        nytraw_df[(nytraw_df['state'] == "Alaska")
                  & (nytraw_df['fips'] == 2997)].iterrows():
        nytraw_df.at[index, 'fips'] = 2901
    for index, row in \ 
        nytraw_df[(nytraw_df['state'] == "Alaska")
                  & (nytraw_df['fips'] == 2998)].iterrows():
        nytraw_df.at[index, 'fips'] = 2902
    #=== Deal with the other "Unknown" county entries
    #
    #    NYTimes has many "unknown" cases/deaths in each state (no fips)
    #    *** See individual state files in rawdata/nytimes/unknown/ ***
    #    ***       created by: parse_unknown-nytimes.py             ***
    #
    #    These "Unknowns" don't seem to rise monotonically (i.e., Unknown
    #    is not being considered a cumulative entry like all other counties).
    #    Often, Unknown will be a large percent of the cases/deaths for a
    #    few early days, but then drop off to zero.  With a few exceptions,
    #    I think the best thing to do is just drop the "Unknown entries.  So,
    #    I'll ignore places where the proportion of Unknown deaths remain
    #    below 1% at late times (just drop all Unknown entries). But I'll
    #    note here the places where there is a problem, suggest and implement
    #    the fix, or note that it is unfixable:
    #
    #    Guam: All are Unknown
    #        ---> dealt with above
    #
    #    IL: Unknowns are 3% of deaths from 2020-06-08 to 2020-11-05, but
    #        then drop to zero and rise again to 1% by 2021-03-05.
    #        ---> Unfixable, drop "Unknown" entries
    #
    #    KS: After some early spikes, percent of deaths in Unknown rises
    #        inconsistently-monotonically from 2020-10-01 to present, going
    #        from 1% to 20%!!!  Comparison with JHU data shows that the NYT
    #        vastly undercounts many counties every few days, placing the
    #        remainder in "Unknown" (no idea why they would do this). So
    #        these Unknown are not real or un-accounted for.
    #        ---> Unfixable, drop "Unknown" entries
    #             ... and hope that KS values get fixed by a later
    #             procedure to ensure monotonic increase of cumulative.
    #
    #    LA: From 2020-04-22 to 2020-11-12 some 3-4% of cases and deaths
    #        are placed in Unknown.  Then they basically drop to zero.
    #        ---> Unfixable, drop "Unknown" entries
    #
    #    MD: Up until 2020-06-01, a large percentage of deaths are in
    #        Unknown (20% before mid-April, then 10% down to 2%)
    #        ---> Unfixable, drop "Unknown"  (***bad***)
    #
    #    MA: Unknown cases rise to >6% by April 2021.
    #        ---> Unfixable, drop "Unknown" entries
    #
    #    MN: Few days of very large % of death in Unknown in first week of
    #        April 2020, then about 2-3% of death in Unknown for second
    #        half of 2020, down to 1% in first half of 2021
    #        ---> Unfixable, drop "Unknown" entries
    #
    #    NY: 5-15% of deaths in 29Mar-5Apr2020 are in Unknown
    #        ---> Unfixable, drop "Unknown" entries
    #
    #    ND: 5% of deaths in Unknown 2020-05-27 through 2020-06-17,
    #        then 10% of deaths in late June/early-July, then 3-4% in
    #        summer 2020, 0.5-2% in Fall 2020, and back up to 2% in
    #        the first half of 2021.
    #        ---> Unfixable, drop "Unknown" entries
    #
    #    MP: 100% of cases and deaths in Unknown up until 2020-07-14.
    #        Then 0% of deaths afterwards, with 1-20% of cases sporadically.
    #        It looks like 2020-07-14 was the day of the first case on Tinian,
    #        so I should just move the earlier Unknowns to Saipan (69110),
    #        and remove other entries
    #        ---> Change "Unknown" to "Saipan" for entries prior to 2020-07-14
    #        ---> Drop "Unknown" entries after 2020-07-14
    for index, row in \ 
        nytraw_df[(nytraw_df['state'] == "Northern Mariana Islands")
                  & (nytraw_df['county'] == "Unknown")
                  & nytraw_df['date'].between("2020-01-01", "2020-07-13")].iterrows():
        nytraw_df.at[index, 'county'] = "Saipan"
        nytraw_df.at[index, 'fips'] = 69110
    #    PR: 100% of deaths are Unknown, and increase monotonically,
    #              as expected (these *are* cumulative numbers)
    #        100% of cases prior to 2020-05-05 are Unknown, then dropping
    #              down to 3-5% afterwards
    #        3% of cases are Unknown, but do not increase monotonically
    #        ---> Change "Unknown" to "All" with county FIPS 000
    for index, row in \ 
        nytraw_df[(nytraw_df['state'] == "Puerto Rico")
                  & (nytraw_df['county'] == "Unknown")].iterrows():
        nytraw_df.at[index, 'county'] = "All"
        nytraw_df.at[index, 'fips'] = 72000
    #    RI: 100% of cases and deaths are Unknown before 2020-03-25. A
    #        significant fraction (50-100%) of deaths remain Unknown through
    #        2020-05-02.  Then the percentage drops to 10% through the end of
    #        June2020, and is 1-5% after that ... no idea what is going on.
    #        ---> Before 2020-05-03: Change "Unknown" to "All" with county FIPS 000
    #        ---> Otherwise: Unfixable, drop "Unknown" entries    
    for index, row in \ 
        nytraw_df[(nytraw_df['state'] == "Rhode Island")
                  & (nytraw_df['county'] == "Unknown")
                  & nytraw_df['date'].between("2020-01-01", "2020-05-02")].iterrows():
        nytraw_df.at[index, 'county'] = "All"
        nytraw_df.at[index, 'fips'] = 44000
    #    TN: Couple high percentages in early days, 1% later
    #        ---> Unfixable, drop "Unknown" entries
    #
    #    UT: High percentages 2020-03-27 through 2020-04-15, then drop off
    #        to 3-6% until 2020-07-16, then approx zero percent unknown
    #        ---> Before 2020-04-16: Change "Unknown" to "All" with county FIPS 000
    #        ---> Otherwise: Unfixable, drop "Unknown" entries    
    for index, row in \ 
        nytraw_df[(nytraw_df['state'] == "Utah")
                  & (nytraw_df['county'] == "Unknown")
                  & nytraw_df['date'].between("2020-01-01", "2020-04-15")].iterrows():
        nytraw_df.at[index, 'county'] = "All"
        nytraw_df.at[index, 'fips'] = 49000
    #    VT: High percentages before 2020-04-08, then death percentage unknown
    #        drops to zero (cases at 0.5%).
    #        ---> Before 2020-04-08: Change "Unknown" to "All" with county FIPS 000
    #        ---> Otherwise: Unfixable, drop "Unknown" entries
    for index, row in \ 
        nytraw_df[(nytraw_df['state'] == "Vermont")
                  & (nytraw_df['county'] == "Unknown")
                  & nytraw_df['date'].between("2020-01-01", "2020-04-07")].iterrows():
        nytraw_df.at[index, 'county'] = "All"
        nytraw_df.at[index, 'fips'] = 50000
    #    VI: All Virgin Islands deaths are listed under "Unknown" before 2020-07-23,
    #        then everything is properly assigned them to the correct "county". There
    #        are four more "Unknown" entries from random days in 2021.
    #        ---> Before 2020-07-23: Change "Unknown" to "All" with county FIPS 000
    #        ---> Otherwise: do same... there's only 4 other entries with 1 case/death
    for index, row in \ 
        nytraw_df[(nytraw_df['state'] == "Virgin Islands")
                  & (nytraw_df['county'] == "Unknown")].iterrows():
        nytraw_df.at[index, 'county'] = "All"
        nytraw_df.at[index, 'fips'] = 78000
    #
    #   VA: Unknown deaths make up significant percentage before 2020-04-21, then
    #       basically zero.
    #        ---> Before 2020-04-08: Change "Unknown" to "All" with county FIPS 000
    #        ---> Otherwise: Unfixable, drop "Unknown" entries
    for index, row in \ 
        nytraw_df[(nytraw_df['state'] == "Virginia")
                  & (nytraw_df['county'] == "Unknown")
                  & nytraw_df['date'].between("2020-01-01", "2020-04-07")].iterrows():
        nytraw_df.at[index, 'county'] = "All"
        nytraw_df.at[index, 'fips'] = 51000
    #   WI: 6-8% of cases from 2020-06-10 to 2020-09-03 are Unknown, but less than
    #       1% of deaths
    #        ---> Unfixable, drop "Unknown" entries
    #
    # Dropping all Unknown entries...
    nytraw_df = nytraw_df[(nytraw_df['county'] != 'Unknown')] # delete all unknown
    #=== Make columns for state and county fips
    nytraw_df['cfips'] = nytraw_df['fips'].astype(int) % 1000
    nytraw_df['sfips'] = nytraw_df['fips'] // 1000 
    #=== [After putting NYT and JHU in same format will create additional entries:
    #     (UT health districts, "all" for all states/territories, metro regions)
    
    #=== output dataframe
    nytraw_df.to_csv("junk.csv", index=False)

    #### JHU
    #           [UID, iso2, iso3, code3, FIPS <5-digit>,
    #            Admin2 <county name>, Province_State <state name>,
    #            Country_Region, Lat, Long_, Combined_Key <full name>,
    #            1/22/20, ... <all dates> ..., today]
    print("=== Loading JHU raw data")
    jhuraw_c_df = pd.read_csv(filename_jhu_cases_raw)
    jhuraw_d_df = pd.read_csv(filename_jhu_deaths_raw)
    #=== Deal with the Kansas City entry
    #      JHU lists Kansas City, MO separately with no FIPS
    # ---> Create new FIPS 29901
    print("=== JHU raw data: adjusting Kansas City")
    for index, row in \
        jhuraw_c_df[(jhuraw_c_df['Admin2'] == "Kansas City")
                  & (jhuraw_c_df['Province_State'] == "Missouri")].iterrows():
        jhuraw_c_df.at[index,'FIPS'] = 29901
    for index, row in \
        jhuraw_d_df[(jhuraw_d_df['Admin2'] == "Kansas City")
                  & (jhuraw_d_df['Province_State'] == "Missouri")].iterrows():
        jhuraw_d_df.at[index,'FIPS'] = 29901
    #=== Deal with the NYC entries
    #      JHU lists the five borough counties separately with their FIPS
    # Leave them (can display just JHU data),
    # but create a new 'New York City' entry (36901) of their sum
    thedates = jhuraw_c_df.columns[11:]
    print(thedates)

    #=== Deal with Dukes and Nantucket entry
    #      JHU has two entries for this MA county:
    #         one is blank (w/ fips), one is actual (w/o fips)
    # Split using current ratio from NYTimes (N/D = 1462/1223)
    
    #=== Deal with Bear River Utah entry 
    #      JHU has a single entry for this health region (w/o fips),
    #      which incorporates the counties:
    #        Box Elder, Cache, Rich
    #      all of which have their own entries (w/ fips) which are zeros
    #          (actually Cache has some data in the first couple months...
    #           then all zeros???!)
    # *** For all Utah Health Departments ***
    #    - leave them as separate entries (JHU will give health dept, NYT county)
    #    - all of Utah is in same DMA: metro area SLC
    # ---> Create new FIPS: 49901

    #=== Deal with Central Utah entry
    #      JHU has a single entry for this health region (w/o fips),
    #      which incorporates the counties:
    #        Juab, Millard, Piute, Sanpete, Sevier, Wayne
    #      all of which have their own entries (w/ fips) which are zeros
    # ---> Create new FIPS: 49902    

    #=== Deal with Southeast Utah entry
    #      JHU has a single entry for this health region (w/o fips),
    #      which incorporates the counties:
    #        Grand, Emery, Carbon
    #      all of which have their own entries (w/ fips) which are zeros
    # ---> Create new FIPS: 49903
    
    #=== Deal with Southwest Utah entry
    #      JHU has a single entry for this health region (w/o fips),
    #      which incorporates the counties:
    #        Beaver, Garfield, Iron, Kane, Washington
    #      all of which have their own entries (w/ fips) which are zeros
    #          (actually Washington has some data in the first couple months...
    #           then all zeros???!)
    # ---> Create new FIPS: 49904    

    #=== Deal with TriCounty Utah entry
    #      JHU has a single entry for this health region (w/o fips),
    #      which incorporates the counties:
    #         Uintah, Duchesne, Daggett
    #      all of which have their own entries (w/ fips) which are zeros
    #          (actually Duchesne has some data in the first couple months...
    #           then all zeros???!)
    # ---> Create new FIPS: 49905
    
    #=== Deal with Weber-Morgan Utah entry
    #      JHU has a single entry for this health region (w/o fips),
    #      which incorporates the counties:
    #          Weber, Morgan
    #      both of which have their own entries (w/ fips) which are zeros
    # ---> Create new FIPS: 49906    

    #=== Deal with American Samoa entry
    #      JHU lists AS as single entry but without a fips
    # ---> Create new FIPS: 60000 (American Samoa -- All)
    
    #=== Deal with Guam entry
    #      JHU lists Guam as single entry but without a fips
    # Give it the Guam FIPS: 66010

    #=== Deal with Northern Mariana Islands entry
    #      JHU lists NMI as single entry but without a fips
    # ---> Create new FIPS: 69000 (NMI -- All)
    
    #=== Deal with Virgin Islands entry
    #      JHU lists VI as single entry but without a fips
    # ---> Create new FIPS: 78000 (VI -- All)    
    
    #=== Deal with prisons
    #      JHU has (no fips) entries for:
    #          "Federal Correctional Institution (FCI)" (michigan)
    #          "Michigan Department of Corrections (MDOC)"
    # Add FCI to Washtenaw County: 26161
    # ---> Create new FIPS: 26901   (there are 31 prisons all over the state)

    #=== Create entry to match the NYT "New York City"
    

################################################################
# Combine NYT and JHU to single file, compute averages, output #
################################################################
def output_covid_cases_deaths():
    pass


#############
# Main Code # 
#############

# If not done, create the basic county FIPS DMA file
if do_fips_dma_collection:
    output_fips_dma_file()

# Load the NYTimes and JHU cases and deaths files and arrange
[nyt_df, jhu_df] = load_nyt_jhu_covid()

# Combine NYT and JHU data into single dataframe
#     compute their average, and running averages (7d, 14d), and output
#output_covid_cases_deaths()

