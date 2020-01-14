# -*- coding: utf-8 -*-
"""
Created on Mon Nov 12 14:48:20 2018

@author: Eric S. Russell
Laboratory for Atmospheric Research
Dept. of Civil and Environmental Engineering
Washington State University
eric.s.russell@wsu.edu

Formats output from the gap-filling code into 30-minute and daily values of the ecosystem exchange and meteorology values
Does not include any wind data or meteorology data that is not used within the gap-filling code (Tair, RH, Rg [SW_IN], H, LE, and Fc)
Initially developed for use with the Phenology Iniative
"""
import pandas as pd
import glob
import calendar
#Conversation form the REddyProc timeformat to a variant of ISO8601 used as the Pandas time index
def JulianDate_to_MMDDYYY(y,jd):
    month = 1
    while jd - calendar.monthrange(y,month)[1] > 0 and month <= 12:
        jd = jd - calendar.monthrange(y,month)[1]
        month = month + 1
    return month,jd,y
#Path is the output path for the final data sets
Path = 'C:\\Users\\Eric\\Desktop\\LTAR\\LTAR_National_Projects\\PhenologyInitiative\\EC Data\\Final_Data\\'
#Collect all filenames for the processed and gap-filled data
files = glob.glob('C:\\Users\\Eric\\Desktop\\LTAR\\LTAR_National_Projects\\REDDY\\Gap-Filled\\*.csv')
for K in range(0, len(files)):
    #Define output filenames based on path names; needs to be coded better since hardcoded to this file-structure
    OutName_Daily =  Path+files[K][66:-4]+'_Summary_Prelim_Daily.csv'
    OutName_30Mins = Path+files[K][66:-4]+'_Summary_Prelim_30Mins.csv'
    #Readin the file
    datae = pd.read_csv(files[K],header=0)
    #Convert timestamp back to something usable from REddy format.
    Y = datae['Year']; D = datae['DoY']; H = datae['Hour']
    half = H % 1
    #Create the index for the file based on the REddyProc Output
    idx = []
    for i in range(0,len(Y)):
        month, jd, y = JulianDate_to_MMDDYYY(Y[i],D[i])
        if half[i] == 0:
            M = '00'
        else: M = '30'
        dt = str(y)+'-'+str(month)+'-'+str(jd)+' '+str(int(H[i]))+':'+str(M)
        idx.append(dt)
    #Create datetime index for dataset
    datae.index = idx
    datae.index = pd.to_datetime(datae.index)
    datae = datae['2018-01-01':'2018-12-31']
    # Columns reduction and renaming to more descriptive names
    cols = pd.read_csv('C:\\Users\\Eric\\Desktop\\LTAR\\Updated_Fluxes\\L3_GapFilled\\Rename_Template.csv',header=0)
    cls = datae.columns # Keeping data as an unchanged variable from this point forward
    #Dropping not needed columns going forward
    s = cls.isin(cols['REDDY']) 
    data_out = datae.drop(datae[cls[~s]],axis = 1)
    cls = data_out.columns
    # Change column header names to final versions
    for k in range (0,len(cols)):
        if cols['REDDY'][k] in cls:
            qn = cols['REDDY'][k] == cls
            data_out = data_out.rename(columns={cls[qn][0]:cols['Final'][k]})
    #Calculate ET from LE using a half-hour based latent heat of vaporization
    L_v = (2501000 - 2370*(data_out['Tair_Gapfilled_L3']))   
    Con = (1/L_v)*(30*60)
    data_out['ET_Gapfilled_L3'] = data_out['LE_Gapfilled_L3']*Con
    if 'LE_Gapfilled_SD' in data_out.columns:
        data_out['ET_Gapfilled_SD'] = data_out['LE_Gapfilled_SD']*Con
    # Set final 30-minute output variable name
    Data_30 = data_out
    # Resample sum, mean, min, and max for entire dataset 
    CE_S = data_out.resample('D').sum()
    CE_M = data_out.resample('D').mean()
    CE_Max = data_out.resample('D').max()
    CE_Min = data_out.resample('D').min()
    # Combine the daily resampled data
    Full = []; Full = pd.DataFrame(Full)
    Full = CE_M.join(CE_S,rsuffix='_Sum').join(CE_Max,rsuffix='_Max').join(CE_Min,rsuffix='_Min')
    # Convert NEE, GPP, and Reco into gC-CO2 units
    Full['Fc_Gapfilled_L3_Sum_Carbon'] =Full['Fc_Gapfilled_L3_Sum']*12*10**(-6)*60*30 
    Full['GPP_L3_Sum_Carbon'] =Full['GPP_L3_Sum']*12*10**(-6)*60*30
    Full['Reco_L3_Sum_Carbon'] =Full['Reco_L3_Sum']*12*10**(-6)*60*30
    # Drop unwanted columns in the day-based dataset
    cls = Full.columns 
    s = cls.isin(cols['Keepers'])
    data_out = Full.drop(Full[cls[~s]],axis = 1)
    # Output 30-minute and daily datasets
    data_out.to_csv(OutName_Daily)
    Data_30.to_csv(OutName_30Mins)
    print(OutName_Daily)