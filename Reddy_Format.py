# -*- coding: utf-8 -*-
"""
Created on Fri Jul 20 11:15:25 2018

@author: Eric Russell
Laboratory for Atmospheric Research
Dept. Civil and Environ. Engineering
Washington State University
eric.s.russell@wsu.edu
"""

import pandas as pd
import numpy as np
from datetime import datetime
def REddy_Format(CE, FileName, col_str):
    #Column names for Reddy and equivalent in the initial dataset; does need to be changed when used within the broader context; will de-hardcode in future updates
    cols = pd.read_csv(r'C:\Users\Eric\Desktop\LTAR\LTAR_National_Projects\PhenologyInitiative\Reddy_Cols.csv',header=0)
    z = pd.DataFrame(CE.index)
    z = z[0].astype(str)
    #Empty variables for the time converts
    adate,Y,H,M = [],[],[],[]
    #Convert time to required format for REddyProc
    for k in range(0,len(z)):
        adate.append(datetime.strptime(z[k],"%Y-%m-%d %H:%M:%S").timetuple().tm_yday)
        dt = datetime.strptime(z[k], "%Y-%m-%d %H:%M:%S")
        Y.append(dt.year); H.append(dt.hour); M.append(dt.minute)
    #Create dataframes for easier use
    M = pd.DataFrame(M);H = pd.DataFrame(H);Y = pd.DataFrame(Y);
    adate = pd.DataFrame(adate)
    #Find the half-hour and add the 0.5 to the timestamp to match correct format
    qn = M==30
    H[qn] = H[qn]+0.5
    #Start building final dataframe output with time-based index
    Outa = []; Outa = pd.DataFrame(Outa)
    Outa['Year'] = Y[0]; Outa['DoY'] = adate[0]; Outa['Hour'] = H[0]
    Outa.index = CE.index
    #Rename column headers to correct names and drop columns not in the list of needed data
    cls = CE.columns
    s = cls.isin(cols[col_str][3:])
    AF_Out = CE.drop(CE[cls[~s]],axis = 1)
    cls = AF_Out.columns
    Outa = Outa.join(AF_Out).astype(float)
    #Rename columns to the appropriate column headers from AmeriFlux-header base
    for k in range (3,len(cols)):
        Outa = Outa.rename(columns={cols[col_str][k]:cols['ReddyProc'][k]})
        qq = np.isnan(Outa[cols['ReddyProc'][k]].astype(float))
        del qq
    #Output formatted data to FileName; path in the input driver file. Replace nans to int -9999 within the output call.
    Outa.to_csv(FileName, sep = '\t', index=False, na_rep = -9999)