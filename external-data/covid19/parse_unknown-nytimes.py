import pandas as pd
import numpy as np

filename_nytimesraw = "rawdata/nytimes/us-counties.csv"
outdir = "rawdata/nytimes/unknown/"

df = pd.read_csv(filename_nytimesraw)
df['total_cases'] = 0.0
df['total_deaths'] = 0.0
df['perc_of_cases'] = 0.0
df['perc_of_deaths'] = 0.0

states = np.unique(df.state.to_list())

for s in states:
    print("====",s,"====")
    dfs = df[df.state == s]
    dates = np.unique(dfs[dfs.county == "Unknown"].date)
    cases = {}
    deaths = {}
    for i in range(len(dates)):
        cases[dates[i]] = dfs[dfs.date == dates[i]]['cases'].sum()
        deaths[dates[i]] = dfs[dfs.date == dates[i]]['deaths'].sum()
    for index,row in dfs[dfs.county == "Unknown"].iterrows():
        dt = row.date
        c = row.cases
        d = row.deaths
        dfs.at[index, 'total_cases'] = cases[dt]
        dfs.at[index, 'total_deaths'] = deaths[dt]
        if (cases[dt]>0):
            dfs.at[index, 'perc_of_cases'] = c/cases[dt]*100.0
        if (deaths[dt]>0):            
            dfs.at[index, 'perc_of_deaths'] = d/deaths[dt]*100.0
    dfs[dfs.county == "Unknown"].to_csv(outdir
                                        + s.replace(" ", "-")
                                        + ".csv", index=False)

