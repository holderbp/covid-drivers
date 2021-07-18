import copy
import pandas as pd
import numpy as np
import geopandas as gpd
import datetime as dt

##########################
# Parameters and Options #
##########################
# Set this to True to re-run the collection of all
# county/DMA/state FIPS (see pars/files in function)
do_fips_dma_collection = True
# Set this to False when de-bugging, so the (2-minute-long)
# loading/cleaning can be skipped
do_load_and_clean_nytjhu = True
# Probably set these to True 
delete_jhu_cruise_entries = True
delete_jhu_prison_entries = True

#############
# Filenames #
#############
outfilename_statecounty_fips = "UScounty_fips_dma.csv"
filename_nyt_raw = "rawdata/nytimes/us-counties.csv"
filename_jhu_cases_raw = "rawdata/jhu/time_series_covid19_confirmed_US.csv"
filename_jhu_deaths_raw = "rawdata/jhu/time_series_covid19_deaths_US.csv"
nyt_c_cleaned_output_file = "nyt_c_cleaned.csv"
nyt_d_cleaned_output_file = "nyt_d_cleaned.csv"    
jhu_c_cleaned_output_file = "jhu_c_cleaned.csv"
jhu_d_cleaned_output_file = "jhu_d_cleaned.csv"    
nyt_c_daily_output_file = "nyt_c_daily.csv"
nyt_d_daily_output_file = "nyt_d_daily.csv"    
jhu_c_daily_output_file = "jhu_c_daily.csv"
jhu_d_daily_output_file = "jhu_d_daily.csv"    

#####################################################################
# Helper scripts for loading and cleaning NYT and JHU raw data sets #
#####################################################################
def msg_to_usr(section, msg):
    print(section + "\t" + msg)
                
def nytjhu_change_data(df, fips=None, county=None, state=None,
                       newfips=None, newcounty=None, newstate=None,
                       date_begin=None, date_end=None):
    if ( (fips is None) & (county is None) & (state is None) ):
        print("***Error nytjhu_change_data needs at least one piece of data")
        exit(0)
    elif ( (fips is None) & (county is None) ):        
        # only the state is given
        thecondition = (df['state'] == state)
    elif ((fips is None) & (state is None) ):
        # only the county is given        
        thecondition = (df['county'] == county)
    elif ((county is None) & (state is None) ):
        # only the FIPS is given        
        thecondition = (df['fips'] == fips)        
    elif (fips is None):
        # county and state are given
        thecondition = ( (df['state'] == state)
                         & (df['county'] == county) )
    if ( (date_begin is not None) & (date_end is not None) ):
        # This will only work for the original NYT raw dataframe
        thecondition = ( (thecondition)
                         & (df['date'].between(date_begin, date_end)) )
    # no need for anything else... if you have a unique FIPS no need to provide other
    for index, row in df[thecondition].iterrows():
            if (newfips is not None):
                df.at[index, 'fips'] = newfips
            if (newcounty is not None):
                df.at[index, 'county'] = newcounty
            if (newstate is not None):
                df.at[index, 'state'] = newstate

def nytjhu_create_composite_alls(df):
    # give the existing "All" entries XX999 fips values
    to_drop = []
    for index, row in df.iterrows():
        sfips = int(row['fips'] // 1000)
        cfips = int(row['fips'] % 1000)
        if (cfips == 0):
            newfips = int(f"{sfips:02d}999")
            to_drop.append(newfips)
            nytjhu_change_data(df, fips=row['fips'], newfips=newfips)
    # create new "All" entries for each state
    tempdf = df.copy(deep=True)
    tempdf['sfips'] = tempdf['fips'] // 1000
    statefips = np.unique(tempdf['sfips'].to_list())
    for s in statefips:
        onestate_df =  tempdf[tempdf['sfips'] == s]
        fipslist = onestate_df['fips'].to_list()
        statename = onestate_df['state'].to_list()[0]
        df = create_composite_entry(df, fipslist,
                                    s*1000, "All", statename,
                                    dropall=False)
    # delete the old "All" entries
    drop_by_fips(df, to_drop)
    # return dataframe
    return df

def nytjhu_create_composite_dmas(df, datatype, dmalist):
    for d in dmalist:
        if (d in [156, 206, 36, 6, 1]):
            # use the composite counties for these places:
            #
            #     Bristol Bay etc (dma-156), Yakutat etc (dma-206),
            #     Chugach + Copper River (dma-156),
            #     Utah HD (dma-36), Dukes and ACK (dma-6),
            #     New York City (dma-1)
            #
            onedma_df = \
                counties_df[(counties_df['dma'] == d)
                            & ( (counties_df['county_type'] == "regular")
                                | (counties_df['county_type'] == "composite") )].copy()
        else:
            # otherwise just grab the regular counties
            onedma_df = \
                counties_df[(counties_df['dma'] == d)
                            & (counties_df['county_type'] == "regular") ].copy()
        dmaname = onedma_df['dmaname'].to_list()[0]
        fipslist = onedma_df['fips'].to_list()
        if ( (datatype == "cases") & (d in [500, 520]) ):
            # For Northern Mariana and American Samoa, cases and deaths in "All"            
            allfips = (fipslist[0] // 1000) * 1000            
            df = create_composite_entry(df, [allfips], int(f"99{d:03d}"), dmaname, "",
                                       dropall=False)    
        elif ( (datatype == "deaths") & (d in [500, 510, 520]) ):
            # PR deaths are also in "All" (though cases are in each county)
            allfips = (fipslist[0] // 1000) * 1000
            df = create_composite_entry(df, [allfips], int(f"99{d:03d}"),
                                        dmaname, "", dropall=False)    
        else:
            df = create_composite_entry(df, fipslist, int(f"99{d:03d}"), dmaname, "",
                                       dropall=False)    
    return df

def nyt_move_city_to_county(df, cityname):
    if (cityname == "Kansas City"):
        county="Jackson"
        state="Missouri"
    elif (cityname == "Joplin"):
        county="Jasper"
        state="Missouri"
    tempdf = df[(df['county'] == county) 
                & (df['state'] == state)]
    ind_to_drop = []
    for index, row in \
        df[(df['county'] == cityname)
           & (df['state'] == state)].iterrows():
        thedate = row['date']
        cases = row['cases']
        deaths = row['deaths']
        county_ind = tempdf.index[ tempdf['date'] == thedate].to_list()[0]
        df.at[county_ind, 'cases'] = df.at[county_ind, 'cases'] + cases
        df.at[county_ind, 'deaths'] = df.at[county_ind, 'deaths'] + deaths
        ind_to_drop.append(index)
    df.drop(ind_to_drop, inplace=True)    

def nyt_make_fips_dicts(df, fips, first_date, last_date):
    # Grab the subset of date-rows with this fips value
    dfnew = df[df['fips'] == fips].copy()
    # Grab column names
    colnames = dfnew.columns.to_list()
    # make the date a datetime
    dfnew['date'] = pd.to_datetime(dfnew['date'], format="%Y-%m-%d")
    # Extend the dates from [first_date:last_date]
    daterng = pd.date_range(first_date, last_date)
    dfnew = dfnew.set_index('date').reindex(daterng).reset_index()
    # Fill Nan entries
    dfnew.fillna(method="ffill", inplace=True)
    dfnew.fillna(0, inplace=True)
    # reset column names
    dfnew.columns = colnames
    # create dictionary entry like a JHU row
    newdict_c = {}
    newdict_d = {}    
    newdict_c['fips'] = fips
    newdict_c['county'] = dfnew['county'].to_list()[-1]
    newdict_c['state'] = dfnew['state'].to_list()[-1]
    newdict_d['fips'] = fips
    newdict_d['county'] = dfnew['county'].to_list()[-1]
    newdict_d['state'] = dfnew['state'].to_list()[-1]
    for index, row in dfnew.iterrows():
        # use the JHU date format
        thedate = row['date'].strftime("%m/%d/%y")
        newdict_c[thedate] = row['cases']
        newdict_d[thedate] = row['deaths']        
    return [newdict_c, newdict_d]

def create_composite_entry(df, fips_list, newfips, newcounty, newstate,
                           dropall=False):
    """
    Works for both JHU and the JHU-type-adjusted NYT dataframes
    """
    target_rows = df[df['fips'].isin(fips_list)]
    if dropall:
        ind_to_drop = target_rows.index.to_list()
    new_row = target_rows.sum()
    new_row['fips'] = newfips
    new_row['county'] = newcounty
    new_row['state'] = newstate
    new_row.name = df.index[-1] + 1
    newdf = df.append([new_row])
    if dropall:
        newdf.drop(ind_to_drop, inplace=True)
    return newdf

def jhu_drop_out_of_state(df):
    ind_to_drop = []
    for index, row in df.iterrows():
        # The Puerto Rico entry has fips 72888, all others are 80XXX
        if ( ((row.fips // 1000) == 80)
             | (row.fips == 72888) ):
            ind_to_drop.append(index)
    df.drop(ind_to_drop, inplace=True)

def jhu_drop_unassigned(df):
    ind_to_drop = []
    for index, row in df.iterrows():
        # The Puerto Rico entry has fips 72999, all others are 80XXX
        if ( ((row.fips // 1000) == 90)
             | (row.fips == 72999) ):
            ind_to_drop.append(index)
    df.drop(ind_to_drop, inplace=True)

def drop_by_fips(df, fips_list):
    target_rows = df[df['fips'].isin(fips_list)]
    ind_to_drop = target_rows.index.to_list()
    df.drop(ind_to_drop, inplace=True)

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
    #=== Add entries for each DMA
    DMA_fips = np.unique(counties_df['dma'].to_list())
    newfipdicts = []
    for d in DMA_fips:
        dmaname = counties_df[counties_df['dma'] == d]\
            ['dmaname'].to_list()[0]
        newentry = {
            'fips_state': 99,
            'fips_county': d,
            'fips': int(f"99{d:03d}"),
            'state': "",
            'stateabb': "",
            'county': dmaname,
            'countylong': dmaname + " --- DMA (not a real FIPS)",
            'dma': d,
            'dmaname': dmaname
        }
        newfipdicts.append(newentry)
    counties_df = counties_df.append(newfipdicts, ignore_index=True, sort=False)        
    #=== Add entries for each state
    states_fips = np.unique(counties_df['fips_state'].to_list())
    # remove the DMA "state"
    states_fips = np.delete(states_fips, np.where(states_fips == 99))
    newfipdicts = []
    for s in states_fips:
        statename = counties_df[counties_df['fips_state'] == s]\
            ['state'].to_list()[0]
        stateabb =  counties_df[counties_df['fips_state'] == s]\
            ['stateabb'].to_list()[0]
        newentry = {
            'fips_state': s,
            'fips_county': 0,
            'fips': s*1000,
            'state': statename,
            'stateabb': stateabb,
            'county': 'All',
            'countylong': statename + " --- All (not a real FIPS)",
            'dma': -1,
            'dmaname': ""
        }
        newfipdicts.append(newentry)
    counties_df = counties_df.append(newfipdicts, ignore_index=True, sort=False)    
    #=== Add a "county-type" variable
    #
    #      'state' = whole state ('All' FIPS)
    #      'regular' = regular county (and not part of composite)
    #      'part-of-composite' = regular county but part of a composite
    #      'composite' = composite of multiple counties
    #      'metro' = metro area composite
    #      'other' = something else (e.g., prisons)
    #
    # when doing analysis, we can choose to use one of the following coverings:
    #
    #         'regular' + 'composite'          (using both NYT and JHU data)
    #         'regular' + 'part of composite'  (e.g., only using JHU nyc data)
    #         'metro'
    #         'state'
    # 
    # set the default value
    counties_df['county_type'] = "regular"
    # set for whole state entries
    counties_df.loc[counties_df['fips_county'] == 0, 'county_type'] = "state"
    # set for DMA entries
    counties_df.loc[counties_df['fips_state'] == 99, 'county_type'] = "metro"
    # set for counties within a composite
    # nyc [36047 kings, 36081 queens, 36085 richmond, 36005 bronx, 36061 New York]
    fipslist = [36047, 36081, 36085, 36005, 36061]
    counties_df.loc[counties_df['fips'].isin(fipslist), 'county_type'] = \
        "part-of-composite"
    # utah health districts
    fipslist = [49001, 49003, 49005, 49007, 49009, 49013,
                49015, 49017, 49019, 49021, 49023, 49025,
                49027, 49029, 49031, 49033, 49039, 49041,
                49047, 49053, 49055, 49057]
    counties_df.loc[counties_df['fips'].isin(fipslist), 'county_type'] = \
        "part-of-composite"
    # AK (two county pairs)
    fipslist = [2060, 2164, 2282, 2105]    
    counties_df.loc[counties_df['fips'].isin(fipslist), 'county_type'] = \
        "part-of-composite"    
    # MA (Dukes and Nantucket)
    fipslist = [25019, 25007]
    counties_df.loc[counties_df['fips'].isin(fipslist), 'county_type'] = \
        "part-of-composite"        
    #=== Rearrange the columns
    counties_df = \
        counties_df[['fips_state', 'fips_county', 'fips', 'county_type', 'state',
                     'stateabb', 'county', 'countylong', 'dma', 'dmaname']]
    #=== Create new composite FIPS codes to align with NYT/JHU import data
    newfipdicts = [
        {
            'fips_state': 36,
            'fips_county': 901,
            'fips': 36901,
            'county_type': "composite",
            'state': 'New York',
            'stateabb': 'NY',
            'county': 'New York City',
            'countylong': 'New York City (not a real FIPS)',
            'dma': 1,
            'dmaname': 'New York'
        },
        {
            'fips_state': 25,
            'fips_county': 901,
            'fips': 25901,
            'county_type': "composite",            
            'state': 'Massachusetts',
            'stateabb': 'MA',
            'county': 'Dukes and Nantucket',
            'countylong': 'Dukes and Nantucket Counties (not a real FIPS)',
            'dma': 6,
            'dmaname': 'Boston'
        },
        {
            'fips_state': 2,
            'fips_county': 901,
            'fips': 2901,
            'county_type': "composite",            
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
            'county_type': "composite",            
            'state': 'Alaska',
            'stateabb': 'AK',
            'county': 'Yakutat plus Hoonah-Angoon',
            'countylong': 'Yakutat plus Hoonah-Angoon (not a real FIPS)',
            'dma': 206,
            'dmaname': 'Juneau'
        },
        {
            'fips_state': 2,
            'fips_county': 63,
            'fips': 2063,
            'county_type': 'part-of-composite',            
            'state': 'Alaska',
            'stateabb': 'AK',
            'county': 'Chugach',
            'countylong': 'Chugach Census Area',
            'dma': 156,
            'dmaname': 'Anchorage'
        },
        {
            'fips_state': 2,
            'fips_county': 66,
            'fips': 2066,
            'county_type': 'part-of-composite',            
            'state': 'Alaska',
            'stateabb': 'AK',
            'county': 'Copper River',
            'countylong': 'Copper River Census Area',
            'dma': 156,
            'dmaname': 'Anchorage'
        },
        {
            'fips_state': 2,
            'fips_county': 903,
            'fips': 2903,
            'county_type': "composite",            
            'state': 'Alaska',
            'stateabb': 'AK',
            'county': 'Chugach plus Copper River',
            'countylong': 'Chugach plus Copper River (not a real FIPS, formerly Valdez-Cordova (261))',
            'dma': 156,
            'dmaname': 'Anchorage'
        },
        {
            'fips_state': 49,
            'fips_county': 901,
            'fips': 49901,
            'county_type': "composite",            
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
            'county_type': "composite",            
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
            'county_type': "composite",            
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
            'county_type': "composite",            
            'state': 'Utah',
            'stateabb': 'UT',
            'county': 'HD Southwest',
            'countylong': 'Southwest Health District [1,17,21,25,53] (not a real FIPS)',
            'dma': 36,
            'dmaname': 'Salt Lake City'
        },
        {
            'fips_state': 49,
            'fips_county': 905,
            'fips': 49905,
            'county_type': "composite",            
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
            'county_type': "composite",            
            'state': 'Utah',
            'stateabb': 'UT',
            'county': 'HD Weber-Morgan',
            'countylong': 'Weber-Morgan Health District [29,57] (not a real FIPS)',
            'dma': 36,
            'dmaname': 'Salt Lake City'
        },
        {
            'fips_state': 26,
            'fips_county': 901,
            'fips': 26901,
            'county_type': "other",            
            'state': 'Michigan',
            'stateabb': 'MI',
            'county': 'MiDOC',
            'countylong': 'Michigan DOC Facilities (not a real FIPS)',
            'dma': None,
            'dmaname': None
        },
        {
            'fips_state': 26,
            'fips_county': 902,
            'fips': 26902,
            'county_type': "other",                        
            'state': 'Michigan',
            'stateabb': 'MI',
            'county': 'MiFCI',
            'countylong': 'Michigan Federal Correctional Institution (not a real FIPS)',
            'dma': None,
            'dmaname': None
        }
    ]
    counties_df = counties_df.append(newfipdicts, ignore_index=True, sort=False)
    #=== Sort and output
    counties_df = counties_df.sort_values(['fips_state', 'fips_county'])
    counties_df.to_csv(outfilename_statecounty_fips, index=False)
    #=== Return the counties dataframe
    return counties_df

##################################################################
#  Load and arrange NYT and JHU Covid19 Cases and Deaths         #
#                                                                #
#     Basic principles:                                          #
#                                                                #
#       * For counties that are combined in JHU/NYTimes, e.g.,   #
#             - New York City (NYT)                              #
#             - two pairs of counties in Alaska (one pair JHU)   #
#             - Utah Health Districts (JHU)                      #
#         create multi-county entries for both, but retain       #
#         county data if it exists in one source, and delete     #
#         county data if it is zero in one source.               #
#                                                                #
#       * When making new fake FIPS codes, start at county fips  #
#         901.  And delete fake fips codes from NYT/JHU          #
#                                                                #
#       * Move multi-county cities to single county (i.e., in    #
#         NYT, move Joplin, MO to Jasper County and KC, MO to    #
#         Jackson County; and delete the city                    #
#                                                                #
#       * Generally delete "unknown" (NYT) or "out of <state>"   #
#         and "unassigned" data (JHU) entries, unless they are   #
#         both a large number/fraction of cases/deaths AND       #
#         they represent a true cumulative count.  In that case  #
#         move them to an "all" county with 000 county fips.     #
#                                                                #
#       * Create all multi-county DMAs in each data set          #
#                                                                #
#       * Give FIPS to those that are missing (e.g., Guam in     #
#         NYT)                                                   #              
#                                                                #
##################################################################
def load_nyt_jhu_covid():
    # Filenames of raw data
    #
    #     * NYTimes is cumulative [cases,deaths] with the form:
    #           [date, county <name>, state <name>,
    #            fips <5-digit>, cases <cum>, deaths <cum>]
    #
    #          https://github.com/nytimes/covid-19-data
    #
    #     * JHU is cumulative cases and deaths in separate files,
    #       each with form:
    #
    #           [UID, iso2, iso3, code3, FIPS <5-digit>,
    #            Admin2 <county name>, Province_State <state name>,
    #            Country_Region, Lat, Long_, Combined_Key <full name>,
    #            1/22/20, ... <all dates> ..., today]
    #
    #          https://github.com/CSSEGISandData/COVID-19
    #          https://doi.org/10.1016/S1473-3099(20)30120-1
    #
    # - Need to deal with the oddball entries of each.
    #   See "Geographic Exceptions" on their github page
    #
    #
    # Output form:
    #
    #   [fips, county, state, <cases/deaths on date1>, <... date2>, ...]
    #
    
    #===============================
    #==== Read in NYTimes data =====
    #===============================
    nytraw_df = pd.read_csv(filename_nyt_raw)
    #=========================================================
    #==== Assign (temporary) fake-FIPS for KC and Joplin =====
    #=========================================================
    # these will be removed later when added into their respective counties
    msg_to_usr("NYT-raw", "Assigning temporary fake-FIPS to Kansas City")
    nytjhu_change_data(nytraw_df, county="Kansas City", state="Missouri",
                       newfips=29998)
    msg_to_usr("NYT-raw", "Assigning temporary fake-FIPS to Kansas City")
    nytjhu_change_data(nytraw_df, county="Joplin", state="Missouri",
                       newfips=29999)
    #==================================================
    #==== Assign fake-FIPS for composite counties =====
    #==================================================
    #=== Deal with the NYC entries
    #      NYTimes lists all five boroughs under "New York City" with no FIPS
    # ---> Create new FIPS 36901
    msg_to_usr("NYT-raw", "Assigning fake-FIPS to NYC")
    nytjhu_change_data(nytraw_df, county="New York City", newfips=36901)
    #=== Deal with the Guam entries
    #    NYTimes lists the county for Guam as "Unknown" with no FIPS
    # ---> Change county to "Guam" and FIPS to its only one: 66010
    msg_to_usr("NYT-raw", "Asigning FIPS to Guam")
    nytjhu_change_data(nytraw_df, state="Guam", newcounty="Guam", newfips=66010)
    #=== Deal with Alaska combined-county entries
    #    NYTimes combines the following counties and gives them
    #    fake FIPS codes:
    #      * Bristol Bay Borough (2060) + Lake and Peninsula Borough (2164)
    #                ---> 2997
    #      * Yakutat City and Borough (2282) + Hoonah-Angoon Census Area (2105)
    #                ---> 2998
    #    And NYTimes uses the former Valdez-Cordova Census Area (261), rather
    #    than the new/correct (in 2019) Chugach (2063) and Copper River (2066)
    #      * Valdez-Cordova (2261) --> Chugach + Copper River (2903)
    # ---> Create new FIPS entries 2901 and 2902, respectively
    msg_to_usr("NYT-raw", "Re-assigning fake-FIPS for two Alaska county pairs")
    nytjhu_change_data(nytraw_df, fips=2997, newfips=2901)
    nytjhu_change_data(nytraw_df, fips=2998, newfips=2902)
    nytjhu_change_data(nytraw_df, fips=2261, newfips=2903)    
    #=====================================================
    #=== Deal with the other "Unknown" county entries ====
    #=====================================================
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
    msg_to_usr("NYT-raw", "Putting early unknown for MP into Saipan")    
    nytjhu_change_data(nytraw_df, state="Northern Mariana Islands",
                    county="Unknown", newcounty="Saipan", newfips=69110,
                    date_begin="2020-01-01", date_end="2020-07-13")
    #    PR: 100% of deaths are Unknown, and increase monotonically,
    #              as expected (these *are* cumulative numbers)
    #        100% of cases prior to 2020-05-05 are Unknown, then dropping
    #              down to 3-5% afterwards
    #        3% of cases are Unknown, but do not increase monotonically
    #        ---> Change "Unknown" to "All" with county FIPS 000
    #        ---> delete all puerto rico counties in "deaths" files (below)
    msg_to_usr("NYT-raw", "Moving unknown cases and deaths in PR to \"All\"")
    nytjhu_change_data(nytraw_df, county="Unknown", state="Puerto Rico",
                    newfips=72000, newcounty="All")
    #    RI: 100% of cases and deaths are Unknown before 2020-03-25. A
    #        significant fraction (50-100%) of deaths remain Unknown through
    #        2020-05-02.  Then the percentage drops to 10% through the end of
    #        June2020, and is 1-5% after that ... no idea what is going on.
    #        ---> Before 2020-05-03: Change "Unknown" to "All" with county FIPS 000
    #        ---> Otherwise: Unfixable, drop "Unknown" entries
    msg_to_usr("NYT-raw", "Moving early unknown cases and deaths in RI to \"All\"")    
    nytjhu_change_data(nytraw_df, state="Rhode Island",
                    county="Unknown", newcounty="All", newfips=44000,
                    date_begin="2020-01-01", date_end="2020-05-02")
    #    TN: Couple high percentages in early days, 1% later
    #        ---> Unfixable, drop "Unknown" entries
    #
    #    UT: High percentages 2020-03-27 through 2020-04-15, then drop off
    #        to 3-6% until 2020-07-16, then approx zero percent unknown
    #        ---> Before 2020-04-16: Change "Unknown" to "All" with county FIPS 000
    #        ---> Otherwise: Unfixable, drop "Unknown" entries    
    msg_to_usr("NYT-raw", "Moving early unknown cases and deaths in UT to \"All\"")
    nytjhu_change_data(nytraw_df, state="Utah",
                    county="Unknown", newcounty="All", newfips=49000,
                    date_begin="2020-01-01", date_end="2020-04-15")
    #    VT: High percentages before 2020-04-08, then death percentage unknown
    #        drops to zero (cases at 0.5%).
    #        ---> Before 2020-04-08: Change "Unknown" to "All" with county FIPS 000
    #        ---> Otherwise: Unfixable, drop "Unknown" entries
    msg_to_usr("NYT-raw", "Moving early unknown cases and deaths in VT to \"All\"")
    nytjhu_change_data(nytraw_df, state="Vermont",
                    county="Unknown", newcounty="All", newfips=50000,
                    date_begin="2020-01-01", date_end="2020-04-07")
    #    VI: All Virgin Islands deaths are listed under "Unknown" before 2020-07-23,
    #        then everything is properly assigned them to the correct "county". There
    #        are four more "Unknown" entries from random days in 2021.
    #        ---> Before 2020-07-23: Change "Unknown" to "All" with county FIPS 000
    #        ---> Otherwise: do same... there's only 4 other entries with 1 case/death
    msg_to_usr("NYT-raw", "Moving early unknown cases and deaths in VI to \"All\"")
    nytjhu_change_data(nytraw_df, state="Virgin Islands",
                    county="Unknown", newcounty="All", newfips=78000)
    #
    #   VA: Unknown deaths make up significant percentage before 2020-04-21, then
    #       basically zero.
    #        ---> Before 2020-04-08: Change "Unknown" to "All" with county FIPS 000
    #        ---> Otherwise: Unfixable, drop "Unknown" entries
    msg_to_usr("NYT-raw", "Moving early unknown cases and deaths in VA to \"All\"")    
    nytjhu_change_data(nytraw_df, state="Virginia",
                    county="Unknown", newcounty="All", newfips=51000,
                    date_begin="2020-01-01", date_end="2020-04-07")
    #   WI: 6-8% of cases from 2020-06-10 to 2020-09-03 are Unknown, but less than
    #       1% of deaths
    #        ---> Unfixable, drop "Unknown" entries
    #
    # Dropping all Unknown entries...
    msg_to_usr("NYT-raw", "Dropping all other \"Unknown\" cases and deaths")    
    nytraw_df = nytraw_df[(nytraw_df['county'] != 'Unknown')] # delete all unknown

    #================================================
    #==== Read in JHU data to get the date range ====
    #================================================
    #
    # JHU columns:
    #    [UID, iso2, iso3, code3, FIPS <5-digit>,
    #     Admin2 <county name>, Province_State <state name>,
    #     Country_Region, Lat, Long_, Combined_Key <full name>,
    #     1/22/20, ... <all dates> ..., <download date>]
    #
    jhuraw_c_df = pd.read_csv(filename_jhu_cases_raw)
    jhuraw_d_df = pd.read_csv(filename_jhu_deaths_raw)
    # Use JHU dataframe to determine range of dates for both it and NYT
    first_date = dt.datetime.strptime(jhuraw_c_df.columns.to_list()[11], "%m/%d/%y")
    last_date = dt.datetime.strptime(jhuraw_c_df.columns.to_list()[-1], "%m/%d/%y")

    #===========================================
    #==== Rearrange NYT data to be like JHU ====
    #===========================================    
    msg_to_usr("NYT-raw", "Re-arranging NYTimes data to look like JHU")
    allfips = np.unique(nytraw_df['fips'].to_list()).astype(int)
    # make two dictionary entries (cases and deaths) for each fip
    #   {fips: XXXXX, 1/22/2020: X, 1/23/2020: X, ..., <last_date>: X}
    fip_dicts_c = []
    fip_dicts_d = []
    for f in allfips:
        fd_c, fd_d = nyt_make_fips_dicts(nytraw_df, f, first_date, last_date)
        fip_dicts_c.append(fd_c)
        fip_dicts_d.append(fd_d)        
    # create new dataframe from list of dictionary entries
    # (one for cases one for deaths)
    nyt_c_df = pd.DataFrame(fip_dicts_c)
    nyt_d_df = pd.DataFrame(fip_dicts_d)    
        
    #================================================================
    #==== Moving the Kansas City and Joplin values into counties ====
    #================================================================
    #=== Deal with the Kansas City entries
    #      NYTimes lists Kansas City, MO separately with no FIPS
    #
    # --> Move to Jackson County (29095)
    #      (Northern parts in Platte and Clay, but oh well) 
    msg_to_usr("NYT-raw", "Moving Kansas City to Jackson County")
    nyt_c_df = create_composite_entry(nyt_c_df, [29998,29095], 29095, "Jackson", "Missori",
                                      dropall=True)
    nyt_d_df = create_composite_entry(nyt_d_df, [29998,29095], 29095, "Jackson", "Missori",
                                      dropall=True)
    #=== Deal with the Joplin entries
    #      NYTimes lists Joplin, MO separately with no FIPS (starting 2020-06-25)
    # Move to Jasper County (29097) (southern part of Joplin in Newton, but oh well)
    msg_to_usr("NYT-raw", "Moving Joplin to Jasper County")
    nyt_c_df = create_composite_entry(nyt_c_df, [29999,29097], 29095, "Jasper", "Missori",
                                      dropall=True)
    nyt_d_df = create_composite_entry(nyt_d_df, [29999,29097], 29095, "Jasper", "Missori",
                                      dropall=True)
    #===============================================================
    #==== Delete the Puerto Rico Counties from deaths dataframe ====
    #===============================================================
    msg_to_usr("NYT-raw", "Dropping all other PR counties from deaths" )            
    ind_to_drop = []
    for index, row in \
        nyt_d_df[nyt_d_df['state'] == "Puerto Rico"].iterrows():
        if (row['county'] != "All"):
            ind_to_drop.append(index)
    nyt_d_df.drop(ind_to_drop, inplace=True)
    #====================================================================
    #==== Create "All" composite-county entries for each state (NYT) ====
    #====================================================================
    #
    #      * thus far, no redundant data (we'll make some below with
    #        composite counties), so we can just sum over state fips
    #
    #      * I have some "All" entries already with unknown data and
    #        these will just be added to the sum of all counties
    #
    #      * Must delete the previous "All" entries (i.e., PR-deaths,
    #        VI, AS, NMI, and  IL, IN, TN, MA) so give them a fake fips
    #        ahead of time, and then delete that afterwards
    #
    msg_to_usr("NYT-raw", "Creating composite \"All\" entries for each state")
    nyt_c_df = nytjhu_create_composite_alls(nyt_c_df)
    nyt_d_df = nytjhu_create_composite_alls(nyt_d_df)
    #=======================================================
    #==== Create a few composite-county entries for NYT ====
    #=======================================================
    #=== Utah Health Districts
    msg_to_usr("NYT-raw", "Creating the Utah HD: Bear River")
    nyt_c_df = create_composite_entry(nyt_c_df, [49003, 49005, 49033],
                                      49901, "Bear River", "Utah", dropall=False)
    nyt_d_df = create_composite_entry(nyt_d_df, [49003, 49005, 49033],
                                      49901, "Bear River", "Utah", dropall=False)
    msg_to_usr("NYT-raw", "Creating the Utah HD: Central Utah")
    nyt_c_df = create_composite_entry(nyt_c_df, [49023, 49027, 49031, 49039, 49041, 49055], 
                                     49902, "Central Utah", "Utah", dropall=False)
    nyt_d_df = create_composite_entry(nyt_d_df, [49023, 49027, 49031, 49039, 49041, 49055],
                                      49902, "Central Utah", "Utah", dropall=False)
    msg_to_usr("NYT-raw", "Creating the Utah HD: Southeast Utah")
    nyt_c_df = create_composite_entry(nyt_c_df, [49007, 49015, 49019],
                                      49903, "Southeast Utah", "Utah", dropall=False)
    nyt_d_df = create_composite_entry(nyt_d_df, [49007, 49015, 49019],
                                      49903, "Southeast Utah", "Utah", dropall=False)
    msg_to_usr("NYT-raw", "Creating the Utah HD: Southwest Utah")
    nyt_c_df = create_composite_entry(nyt_c_df, [49001, 49017, 49021, 49025, 49053],
                                      49904, "Southwest", "Utah", dropall=False)
    nyt_d_df = create_composite_entry(nyt_d_df, [49001, 49017, 49021, 49025, 49053],
                                      49904, "Bear River", "Utah", dropall=False)
    msg_to_usr("NYT-raw", "Creating the Utah HD: TriCounty")
    nyt_c_df = create_composite_entry(nyt_c_df, [49009, 49013, 49047],
                                      49905, "TriCounty", "Utah", dropall=False)
    nyt_d_df = create_composite_entry(nyt_d_df, [49009, 49013, 49047],
                                      49905, "TriCounty", "Utah", dropall=False)
    msg_to_usr("NYT-raw", "Creating the Utah HD: Weber-Morgan")
    nyt_c_df = create_composite_entry(nyt_c_df, [49029, 49057],
                                      49906, "Weber-Morgan", "Utah", dropall=False)
    nyt_d_df = create_composite_entry(nyt_d_df, [49029, 49057],
                                      49906, "Weber-Morgan", "Utah", dropall=False)
    #=== Dukes and Nantucket
    msg_to_usr("NYT-raw", "Creating the composite Dukes + Nantucket (MA)")
    nyt_c_df = create_composite_entry(nyt_c_df, [25007, 25019],
                                      25901, "Dukes and Nantucket", "Massachusetts",
                                      dropall=False)
    nyt_d_df = create_composite_entry(nyt_d_df, [25007, 25019],
                                      25901, "Dukes and Nantucket", "Massachusetts",
                                      dropall=False)
    #=================================================================
    #=== Create composite DMA (metro area) entries for each state ====
    #=================================================================
    #
    # get the list of dmas from the counties FIPS file
    #    (as a global, will be used in below subroutine)
    dmalist = np.unique(counties_df['dma'].to_list()).astype(int)
    # remove the state entry (not a dma)
    # and the blanks (gives some weird -9223372036854775808 value)
    dmalist = np.delete(dmalist, np.where(dmalist < 0))
    msg_to_usr("NYT-raw", "Creating composite DMA (Metro Area) entries")
    nyt_c_df = nytjhu_create_composite_dmas(nyt_c_df, "cases", dmalist)
    nyt_d_df = nytjhu_create_composite_dmas(nyt_d_df, "deaths", dmalist)
    # NYTimes has no American Samoa
    #    --> drop that DMA
    drop_by_fips(nyt_c_df, [99500])
    drop_by_fips(nyt_d_df, [99500])    
    
    #=================================
    #==== Output NYT data to file ====
    #=================================
    nyt_c_df.to_csv(nyt_c_cleaned_output_file, index=False)
    nyt_d_df.to_csv(nyt_d_cleaned_output_file, index=False)    
    
    #===================================
    #==== Continue parsing JHU data ====
    #===================================
    # Give column names for main columns that match NYT
    newcols = jhuraw_c_df.columns.values
    newcols[4:7] = ['fips', 'county', 'state']
    jhuraw_c_df.columns = newcols
    newcols = jhuraw_d_df.columns.values
    newcols[4:7] = ['fips', 'county', 'state']
    jhuraw_d_df.columns = newcols
    #=====================================================
    #==== Delete "Out of [state]" entries in JHU data ====
    #=====================================================
    #       JHU has "Out of [state]" entries for every state.  These don't seem
    #       to be reliably cumulative.  They have 80XXX FIPS codes
    # ---> Just delete them all
    msg_to_usr("JHU-raw", "Dropping \"Out of <state>\" entries" )
    jhu_drop_out_of_state(jhuraw_c_df)
    jhu_drop_out_of_state(jhuraw_d_df)
    #============================================================
    #==== Deal with "Unassigned [state]" entries in JHU data ====
    #============================================================
    #       JHU has "Unassigned" county entries for every state. 
    #       These don't seem to be reliably cumulative.
    #       They have 90XXX FIPS codes
    #
    #    Exceptions:
    #       - all of PR deaths are in Unassigned
    #       - lots of IL, IN, TN deaths in Unassigned and also cumulative
    #         (e.g., IL=2300; while IL,Cook has 9900)
    #       - MA has 43k unassigned cases that are cumulative
    #       - NJ has 1868 unassigned deaths up until about halfway, then zeros
    #
    # ---> Change Puerto Rico "Unassigned" deaths to "All" (72000)
    # ---> Delete Puerto Rico counties for deaths
    msg_to_usr("JHU-raw", "Saving PR \"Unassigned\" deaths to \"All\"" )
    nytjhu_change_data(jhuraw_d_df, state="Puerto Rico",
                       county="Unassigned", newfips=72000, newcounty="All")
    msg_to_usr("JHU-raw", "Dropping all other PR counties from deaths" )            
    ind_to_drop = []
    for index, row in \
        jhuraw_d_df[jhuraw_d_df['state'] == "Puerto Rico"].iterrows():
        if (row['county'] != "All"):
            ind_to_drop.append(index)
    jhuraw_d_df.drop(ind_to_drop, inplace=True)
    # ---> Save IL, IN, TN Unassigned deaths as an "All" county
    msg_to_usr("JHU-raw", "Moving IL, IN, and TN \"Unassigned\" deaths to \"All\"" )
    nytjhu_change_data(jhuraw_d_df, state="Illinois",
                       county="Unassigned", newfips=17000, newcounty="All")
    nytjhu_change_data(jhuraw_d_df, state="Indiana",
                       county="Unassigned", newfips=18000, newcounty="All")
    nytjhu_change_data(jhuraw_d_df, state="Tennessee",
                       county="Unassigned", newfips=47000, newcounty="All")
    # ---> Save MA Unassigned cases as an "All" county    
    msg_to_usr("JHU-raw", "Moving MA \"Unassigned\" cases to \"All\"" )
    nytjhu_change_data(jhuraw_c_df, state="Massachusetts",
                       county="Unassigned", newfips=25000, newcounty="All")
    # ---> Delete all other "Unassigned" entries
    msg_to_usr("JHU-raw", "Dropping all other \"Unassigned\" cases and deaths" )
    jhu_drop_unassigned(jhuraw_c_df)
    jhu_drop_unassigned(jhuraw_d_df)    
    #========================================================
    #==== Deal with some mis-labeled entries in JHU data ====
    #========================================================
    #=== Deal with American Samoa entry
    #      JHU lists AS as single entry with a "60" fips
    #
    # ---> Changing to fake-FIPS: 60000 (American Samoa -- All)
    msg_to_usr("JHU-raw", "Moving American Samoa data into \"All\"")
    nytjhu_change_data(jhuraw_c_df, state="American Samoa", newcounty="All",
                       newfips=60000)
    nytjhu_change_data(jhuraw_d_df, state="American Samoa", newcounty="All",
                       newfips=60000)
    #=== Deal with Guam entry
    #      JHU lists Guam as single entry but with fips="66"
    # Give it the Guam FIPS: 66010
    msg_to_usr("JHU-raw", "Giving Guam its proper FIPS number")
    nytjhu_change_data(jhuraw_c_df, state="Guam", newcounty="Guam",
                       newfips=66010)
    nytjhu_change_data(jhuraw_d_df, state="Guam", newcounty="Guam",
                       newfips=66010)
    #=== Deal with Northern Mariana Islands entry
    #      JHU lists NMI as single entry but without a fips
    # ---> Create new FIPS: 69000 (NMI -- All)
    msg_to_usr("JHU-raw", "Putting Northern Mariana Islands data in \"All\"")
    nytjhu_change_data(jhuraw_c_df, state="Northern Mariana Islands",
                       newfips=69000, newcounty="All")
    nytjhu_change_data(jhuraw_d_df, state="Northern Mariana Islands",
                       newfips=69000, newcounty="All")    
    #=== Deal with Virgin Islands entry
    #      JHU lists VI as single entry but without a fips
    # ---> Create new FIPS: 78000 (VI -- All)    
    msg_to_usr("JHU-raw", "Moving Virgin Islands cases and deaths to \"All\"")
    nytjhu_change_data(jhuraw_c_df, state="Virgin Islands",
                       newcounty="All", newfips=78000)
    nytjhu_change_data(jhuraw_d_df, state="Virgin Islands",
                       newcounty="All", newfips=78000)
    #==============================================
    #==== Deal with cruise-ship/prison entries ====
    #==============================================
    #=== Delete Cruise Ship entries
    #      JHU lists "Diamond Princess" and "Grand Princess"
    #      as individual (no FIPS) entries.
    # ---> delete these rows
    if delete_jhu_cruise_entries:
        msg_to_usr("JHU-raw", "Deleting cruise ship entries")
        jhuraw_c_df = jhuraw_c_df[jhuraw_c_df['state'] != "Grand Princess"]
        jhuraw_c_df = jhuraw_c_df[jhuraw_c_df['state'] != "Diamond Princess"]    
        jhuraw_d_df = jhuraw_d_df[jhuraw_d_df['state'] != "Grand Princess"]
        jhuraw_d_df = jhuraw_d_df[jhuraw_d_df['state'] != "Diamond Princess"]    
    #=== Deal with Michigan prisons 
    #      JHU has (no fips) entries for:
    #          "Federal Correctional Institution (FCI)" (michigan)
    #          "Michigan Department of Corrections (MDOC)"
    # ---> Create new FIPS for MDOC: 26901   (there are 31 prisons all over the state)
    # ---> Create new FIPS for FCI (in Washtenaw): 26902 
    msg_to_usr("JHU-raw", "Moving Michigan Prisons into fake FIPS entries")
    nytjhu_change_data(jhuraw_c_df, county="Michigan Department of Corrections (MDOC)",
                       newfips=26901)
    nytjhu_change_data(jhuraw_d_df, county="Michigan Department of Corrections (MDOC)",
                       newfips=26901)
    nytjhu_change_data(jhuraw_c_df, county="Federal Correctional Institution (FCI)",
                       newfips=26902)
    nytjhu_change_data(jhuraw_d_df, county="Federal Correctional Institution (FCI)",
                       newfips=26902)
    if delete_jhu_prison_entries:
        # But probably just drop them
        msg_to_usr("JHU-raw", "Deleting Michigan prison entries")
        drop_by_fips(jhuraw_c_df, [26901,26902])
        drop_by_fips(jhuraw_d_df, [26901,26902])        
    #=============================================================
    #==== Deal with some composite-county entries in JHU data ====
    #=============================================================
    #=== Deal with Alaska combined-county entries
    #    JHU (also NYTimes) combines the following counties and gives them
    #    one FIPS code:
    #      * Bristol Bay Borough (2060) + Lake and Peninsula Borough (2164)
    #                ==  "2164"
    # ---> Change this FIPS to our created one 2901 (see NYT)
    # ---> Delete the (empty) "Bristol Bay" entry
    #
    #  (DO THESE BELOW WHEN CREATING COMPOSITES)
    # ---> Create a combined entry for Yakutat (2282)
    #      + Hoonah-Angoon (2105) == 2902
    # ---> Create a combined entry for Chugach (2063)
    #      + Hoonah-Angoon (2066) == 2903
    msg_to_usr("JHU-raw", "Adjusting Bristol Bay Alaska")
    nytjhu_change_data(jhuraw_c_df, county="Bristol Bay plus Lake and Peninsula",
                       newfips=2901)
    nytjhu_change_data(jhuraw_d_df, county="Bristol Bay plus Lake and Peninsula",
                       newfips=2901)
    # delete the bristol bay entries (lake and peninsula don't have entries)
    jhuraw_c_df = jhuraw_c_df[jhuraw_c_df['county'] != "Bristol Bay"]
    jhuraw_d_df = jhuraw_d_df[jhuraw_d_df['county'] != "Bristol Bay"]        
    #=== Deal with Dukes and Nantucket entry
    #      JHU combines these two MA counties (entry w/o fips)
    #      and then has blank entries for them individually (w/ fips)
    #
    # ---> Create new FIPS 25901
    msg_to_usr("JHU-raw", "Giving \"Dukes and Nantucket\" a fake-FIPS")
    nytjhu_change_data(jhuraw_c_df, county="Dukes and Nantucket", newfips=25901)
    nytjhu_change_data(jhuraw_d_df, county="Dukes and Nantucket", newfips=25901)
    # ---> Delete individual counties of Dukes and Nantucket 
    msg_to_usr("JHU-raw", "Deleting the individual counties of Dukes and Nantucket")
    jhuraw_c_df = jhuraw_c_df[jhuraw_c_df['fips'] != 25007]
    jhuraw_c_df = jhuraw_c_df[jhuraw_c_df['fips'] != 25019]    
    jhuraw_d_df = jhuraw_d_df[jhuraw_d_df['fips'] != 25007]
    jhuraw_d_df = jhuraw_d_df[jhuraw_d_df['fips'] != 25019]    
    # *** For all Utah Health Departments ***
    #    - JHU gives health department entries (see below), and has
    #      blank entries for each county contained in those
    #    - all of Utah is in same DMA: metro area SLC
    #
    #    ---> delete entries for counties contained in health departments
    counties_to_delete = [49001, 49003, 49005, 49007, 49009, 49013,
                          49015, 49017, 49019, 49021, 49023, 49025,
                          49027, 49029, 49031, 49033, 49039, 49041,
                          49047, 49053, 49055, 49057]
    drop_by_fips(jhuraw_c_df, counties_to_delete)
    drop_by_fips(jhuraw_d_df, counties_to_delete)
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
    msg_to_usr("JHU-raw", "Giving fake-FIPS to Bear River Utah HD")
    nytjhu_change_data(jhuraw_c_df, county="Bear River", newfips=49901)
    nytjhu_change_data(jhuraw_d_df, county="Bear River", newfips=49901)    
    #=== Deal with Central Utah entry
    #      JHU has a single entry for this health region (w/o fips),
    #      which incorporates the counties:
    #        Juab, Millard, Piute, Sanpete, Sevier, Wayne
    #      all of which have their own entries (w/ fips) which are zeros
    # ---> Create new FIPS: 49902    
    msg_to_usr("JHU-raw", "Giving fake-FIPS to Central Utah HD")
    nytjhu_change_data(jhuraw_c_df, county="Central Utah", newfips=49902)
    nytjhu_change_data(jhuraw_d_df, county="Central Utah", newfips=49902)    
    #=== Deal with Southeast Utah entry
    #      JHU has a single entry for this health region (w/o fips),
    #      which incorporates the counties:
    #        Grand, Emery, Carbon
    #      all of which have their own entries (w/ fips) which are zeros
    # ---> Create new FIPS: 49903
    msg_to_usr("JHU-raw", "Giving fake-FIPS to Southeast Utah HD")
    nytjhu_change_data(jhuraw_c_df, county="Southeast Utah", newfips=49903)
    nytjhu_change_data(jhuraw_d_df, county="Southeast Utah", newfips=49903)
    #=== Deal with Southwest Utah entry
    #      JHU has a single entry for this health region (w/o fips),
    #      which incorporates the counties:
    #        Beaver, Garfield, Iron, Kane, Washington
    #      all of which have their own entries (w/ fips) which are zeros
    #          (actually Washington has some data in the first couple months...
    #           then all zeros???!)
    # ---> Create new FIPS: 49904    
    msg_to_usr("JHU-raw", "Giving fake-FIPS to Southwest Utah HD")
    nytjhu_change_data(jhuraw_c_df, county="Southwest Utah", newfips=49904)
    nytjhu_change_data(jhuraw_d_df, county="Southwest Utah", newfips=49904)
    #=== Deal with TriCounty Utah entry
    #      JHU has a single entry for this health region (w/o fips),
    #      which incorporates the counties:
    #         Uintah, Duchesne, Daggett
    #      all of which have their own entries (w/ fips) which are zeros
    #          (actually Duchesne has some data in the first couple months...
    #           then all zeros???!)
    # ---> Create new FIPS: 49905
    msg_to_usr("JHU-raw", "Giving fake-FIPS to TriCounty Utah HD")
    nytjhu_change_data(jhuraw_c_df, county="TriCounty", newfips=49905)
    nytjhu_change_data(jhuraw_d_df, county="TriCounty", newfips=49905)
    #=== Deal with Weber-Morgan Utah entry
    #      JHU has a single entry for this health region (w/o fips),
    #      which incorporates the counties:
    #          Weber, Morgan
    #      both of which have their own entries (w/ fips) which are zeros
    # ---> Create new FIPS: 49906    
    msg_to_usr("JHU-raw", "Giving fake-FIPS to Weber-Morgan Utah HD")
    nytjhu_change_data(jhuraw_c_df, county="Weber-Morgan", newfips=49906)
    nytjhu_change_data(jhuraw_d_df, county="Weber-Morgan", newfips=49906)
    #=================================================
    #==== Keep only important columns in JHU data ====
    #=================================================
    #=== Delete unnecessary columns, keeping only:
    #         [fips, county, state, <dates>]
    jhuraw_c_df.drop(['UID', 'iso2', 'iso3', 'code3', 'Country_Region',
                      'Lat', 'Long_', 'Combined_Key'], axis=1, inplace=True)
    # (one extra column in the "deaths" file)
    jhuraw_d_df.drop(['UID', 'iso2', 'iso3', 'code3', 'Country_Region', 'Population',
                      'Lat', 'Long_', 'Combined_Key'], axis=1, inplace=True)
    #======================================================
    #==== Move Kansas City into Jackson County for JHU ====
    #======================================================
    #=== Deal with the Kansas City entry
    #      JHU lists Kansas City, MO separately with no FIPS
    #    ---> Move into Jackson County
    msg_to_usr("JHU-raw", "Moving Kansas City into Jackson County")
    # give KC a fake fips
    kc_ind = jhuraw_c_df.index[jhuraw_c_df['county'] == "Kansas City"].to_list()[0]
    jhuraw_c_df.at[kc_ind, 'fips'] = 99999
    # merge KC and Jackson County entries into new Jackson entry
    jhuraw_c_df = create_composite_entry(jhuraw_c_df, [99999, 29095],
                                         29095, "Jackson", "Missouri", dropall=True)
    # give KC a fake fips
    kc_ind = jhuraw_d_df.index[jhuraw_d_df['county'] == "Kansas City"].to_list()[0]
    jhuraw_d_df.at[kc_ind, 'fips'] = 99999
    # merge KC and Jackson County entries into new Jackson entry
    jhuraw_d_df = create_composite_entry(jhuraw_d_df, [99999, 29095],
                                         29095, "Jackson", "Missouri", dropall=True)
    #======================================================
    #=== Create composite "All" entries for each state ====
    #======================================================
    #
    #      * thus far, no redundant data (we'll make some below with
    #        composite counties), so we can just sum over state fips
    #
    #      * I have some "All" entries already with unknown data and
    #        these will just be added to the sum of all counties
    #
    #      * Must delete the previous "All" entries (i.e., PR-deaths,
    #        VI, AS, NMI, and  IL, IN, TN, MA) so give them a fake fips
    #        ahead of time, and then delete that afterwards
    #
    msg_to_usr("JHU-raw", "Creating composite \"All\" entries for each state")
    jhuraw_c_df = nytjhu_create_composite_alls(jhuraw_c_df)
    jhuraw_d_df = nytjhu_create_composite_alls(jhuraw_d_df)
    #===============================================================================
    #=== Create two composite counties to match NYTimes: 2 Alaska pairs and NYC ====
    #===============================================================================
    #=== Create composite Yakutat (2282) + Hoonah-Angoon (2105) (AK) entry    
    #      JHU lists these counties separately, but NYT has them joined
    #    ---> create the fake-fips 2902 for their combination
    msg_to_usr("JHU-raw", "Creating composite Yakutat + Hoonah (AK) entry")
    jhuraw_c_df = create_composite_entry(jhuraw_c_df, [2282, 2105],
                                         2902, "Yakutat plus Hoonah-Angoon",
                                         "Alaska", dropall=False)    
    jhuraw_d_df = create_composite_entry(jhuraw_d_df, [2282, 2105],
                                         2902, "Yakutat plus Hoonah-Angoon",
                                         "Alaska", dropall=False)
    #=== Create composite Chugach (2063) + Copper River (2066) (AK) entry    
    #      JHU lists these counties separately, but NYT has them joined
    #      as the former Valdez-Cordova (2261)
    #    ---> create the fake-fips 2903 for their combination
    msg_to_usr("JHU-raw", "Creating composite Chugach + Copper River (AK) entry")
    jhuraw_c_df = create_composite_entry(jhuraw_c_df, [2063, 2066],
                                         2903, "Chugach plus Copper River",
                                         "Alaska", dropall=False)    
    jhuraw_d_df = create_composite_entry(jhuraw_d_df, [2063, 2066],
                                         2903, "Chugach plus Copper River",
                                         "Alaska", dropall=False)
    #=== Create composite NYC entry
    #      JHU lists the five borough counties separately with their FIPS
    # ---> Leave them (can display just JHU data),
    # ---> but also create a new 'New York City' entry (36901) of their sum
    msg_to_usr("JHU-raw", "Creating composite \"New York City\" entry")
    nyc_fips = [36047, 36081, 36005, 36085, 36061]
    jhuraw_c_df = create_composite_entry(jhuraw_c_df, nyc_fips,
                                         36901, "New York City",
                                         "New York", dropall=False)    
    jhuraw_d_df = create_composite_entry(jhuraw_d_df, nyc_fips,
                                         36901, "New York City",
                                         "New York", dropall=False)    
    #=================================================================
    #=== Create composite DMA (metro area) entries for each state ====
    #=================================================================
    # create composite entries for each dma
    #
    #   take into account special cases:
    #       * states with composite counties (AK, MA, UT, NY)
    #       * places in which cases/deaths are in "All" (PR, AS, NMI)
    #
    msg_to_usr("JHU-raw", "Creating composite DMA (Metro Area) entries")
    jhuraw_c_df = nytjhu_create_composite_dmas(jhuraw_c_df, "cases", dmalist)
    jhuraw_d_df = nytjhu_create_composite_dmas(jhuraw_d_df, "deaths", dmalist)    
    #===============================
    #=== Output the JHU dataset ====
    #===============================
    # Create final dataframes
    jhu_c_df = jhuraw_c_df.copy(deep=True)
    jhu_d_df = jhuraw_d_df.copy(deep=True)
    # output to csv
    jhu_c_df.to_csv(jhu_c_cleaned_output_file, index=False)
    jhu_d_df.to_csv(jhu_d_cleaned_output_file, index=False)
    # return all cleaned dataframes
    return [nyt_c_df, nyt_d_df, jhu_c_df, jhu_d_df]

def get_daily_data(dfin, datatype):
    # Input dataframe is
    # 
    #   [fips, county, state, <day1 data>, <day2 data>, ...]
    #
    # get rid of [county, state]
    df = dfin.drop(['county', 'state'], axis=1).copy()
    # Transpose dataframes into:
    #
    #        [date, fips, <data>]
    #
    # set fips as index
    df.set_index('fips', inplace=True)
    # transpose date into row label, and then "stack"
    # to get fips as additional column
    df = pd.DataFrame(df.transpose().stack())
    # reset index to usual
    df = df.reset_index()
    # rename columns
    df.columns = ['date', 'fips', 'cum']
    # set date as a date
    df['date'] = pd.to_datetime(df['date'], format="%m/%d/%y")
    # ensure fips is an integer
    df['fips'] = df['fips'].astype(int)
    # sort by fips, date
    df = df.sort_values(['fips','date'])
    #=== Check if data is cumulative
    # create a new daily cases/deaths column
    df['daily'] = 0
    allfips = np.unique(df['fips'].to_list()).astype(int)
    msg_to_usr("daily-counts-" + datatype, "Checking for negative daily counts:")
    for f in allfips:
        print(f)
        # print the location
        thecounty = (counties_df['fips'] == f)
        sabb = counties_df[thecounty]['stateabb'].to_list()[0]
        county = counties_df[thecounty]['countylong'].to_list()[0]
        print("\t", county, sabb)
        # restrict to one fips
        onefip = (df['fips'] == f)
        df.loc[onefip, 'daily'] = df[onefip]['cum'].diff()
        # check for negative daily values
        dfsub = df[onefip]
        for index, row in dfsub[dfsub['daily'] < 0].iterrows():
            print("\t\t", row['fips'], "\t",
                  row['date'].strftime("%Y-%m-%d"), "\t",
                  row['cum'], "\t", row['daily'])
    return df

#############
# Main Code # 
#############

# Create the basic county FIPS and DMA file, or read it in from file
if do_fips_dma_collection:
    counties_df = output_fips_dma_file()
else:
    msg_to_usr("main", "Loading the counties fips file")
    #
    # columns are:
    #
    #    [fips_state, fips_county, fips, county_type,
    #     state, stateabb, county, countylong, dma, dmaname]
    #
    counties_df = pd.read_csv(outfilename_statecounty_fips)

# Load the NYTimes and JHU cases and deaths files and clean the data
#
#   output is:
#
#              [fips, county, state,
#               <cases/deaths on date1>, <cases/deaths on date2>, ... ]
#
#   includes:
#
#        * individual counties (if data was given)
#        * composite counties (if either NYT/JHU used them)
#        * full-state "All" (w/ county fips 000)
#        * full-DMA metro areas (w/ state fips 99, county fips = DMA)
#
if do_load_and_clean_nytjhu:
    [nyt_c_df, nyt_d_df, jhu_c_df, jhu_d_df] = load_nyt_jhu_covid()
else:
    msg_to_usr("main", "Loading already cleaned data files")
    nyt_c_df = pd.read_csv(nyt_c_cleaned_output_file)
    nyt_d_df = pd.read_csv(nyt_d_cleaned_output_file)
    jhu_c_df = pd.read_csv(jhu_c_cleaned_output_file)
    jhu_d_df = pd.read_csv(jhu_d_cleaned_output_file)    

# Combine NYT and JHU data into single dataframe:
#
# transpose to get form:
#
#     [date, fips, cases/deaths]
msg_to_usr("main", "Transposing and getting daily values for NYT cases")
nyt_c_df = get_daily_data(nyt_c_df, 'cases')
msg_to_usr("main", "Transposing and getting daily values for NYT deaths")
nyt_d_df = get_daily_data(nyt_d_df, 'deaths')
msg_to_usr("main", "Transposing and getting daily values for JHU cases")
jhu_c_df = get_daily_data(jhu_c_df, 'cases')
msg_to_usr("main", "Transposing and getting daily values for JHU deaths")
jhu_d_df = get_daily_data(jhu_d_df, 'deaths')

#==================================
#==== Output dataframes to csv ====
#==================================
nyt_c_df.to_csv(nyt_c_daily_output_file, index=False)
nyt_d_df.to_csv(nyt_d_daily_output_file, index=False)    
jhu_c_df.to_csv(jhu_c_daily_output_file, index=False)
jhu_d_df.to_csv(jhu_d_daily_output_file, index=False)


#    * combine into single dataframe
#
#           [date, fips, jhu_cases, nyt_cases, jhu_deaths, nyt_deaths]
#
#    * calculate columns for mean and geometric mean
#
#    * calculate 14-day averages 
#
