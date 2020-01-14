# -*- coding: utf-8 -*-
"""
Created on Mon Oct  1 16:22:52 2018

@author: Eric S. Russell
Laboratory for Atmospheric Research
Dept. of Civil and Environmental Engineering
Washington State University
eric.s.russell@wsu.edu
"""
import re
import pandas as pd
import glob
import os
import numpy as np
import datetime
import warnings
# Change this path to the directory where the LTAR_Flux_QC.py file is located
os.chdir(r'C:\Users\Eric\Documents\GitHub\LTAR_Phenolog_Initiative_EC_Processing')       
import LTAR_Pheno_QC_Functions as LLT
import Reddy_Format as REF
ds = '2018-01-01'
de = '2019-01-01'

files = glob.glob('C:\\Users\\Eric\\Desktop\\LTAR\\LTAR_National_Projects\\PhenologyInitiative\\EC Data\\Processed\\Unprocessed\\*.csv') #Directory or file name with file names here
# File with upper and lower limits for the flux values for each site based on visual inspection of each dataset
QC = pd.read_csv('C:\\Users\\Eric\\Desktop\\LTAR\\LTAR_National_Projects\\PhenologyInitiative\\QC_Limits_List.csv',header = 0, index_col = 'Site')
#%%files
for K in range (0,len(files)):
#Read in data and concat to one dataframe; no processing until data all read in - assumes data is from AmeriFlux or in the format that was defined by the group for data requests
    df = pd.read_csv(files[K],header= 0,sep=',',low_memory=False)
    dt = []
    nme = files[K][100:-21]+ds+'_'+de # These values change with filepath; still need to de-hardcode this value
    nme = re.sub(r'\W+', '', nme) #Remove the dashes from the date start and end points
    Site = ''.join(filter(str.isalpha, files[K][108:111])) # These values change with filepath; still need to de-hardcode this value; grabs the 3-letter site abbreviation I use to ID site sets; work from end of datafile
    if ('/' in str(df['TIMESTAMP_START'][0])) | ('-' in str(df['TIMESTAMP_START'][0])):
        df.index = pd.to_datetime(df['TIMESTAMP_START'])
    else: # Convert timestamp to time-index for easier use
        for k in range (0,len(df)):
            Y =  str(int(df['TIMESTAMP_START'][k]))[0:4]
            M =  str(int(df['TIMESTAMP_START'][k]))[4:6]  
            D =  str(int(df['TIMESTAMP_START'][k]))[6:8]
            hh = str(int(df['TIMESTAMP_START'][k]))[8:10] 
            mm = str(int(df['TIMESTAMP_START'][k]))[10:12]
            dt.append(Y+'-'+M+'-'+D+' '+hh+':'+mm)
        dt = pd.DataFrame(dt);df.index = dt[0]
        df.index=pd.to_datetime(df.index) # Time-based index
        if 'TimeStamp' in df.columns: df=df.drop(columns = 'TimeStamp') # Specific to JORN
        if 'timestamp' in df.columns: df=df.drop(columns = 'timestamp') # Specific to JORN
        if 'TIMESTAMP' in df.columns: df=df.drop(columns = 'TIMESTAMP') # Specific to JORN
        if 'HRMIN' in df.columns: df=df.drop(columns = 'HRMIN') # Specific to UCB because formating time is bad
        df = df.astype(float)
    df = LLT.indx_fill(df,'30min') # Fill in and missing half-hours in the dataset to have a continuous data set from start time to end.
    df = df[ds:de] # Limit data to set timeframe based on defined bounds above
    #%%
#    Site = 'JOR' # If need to define specific site
    data_qc, data_flags = LLT.Grade_cs(df, QC, Site, site=True) #QC flux data
    sel = []
    #QA/QC and format the meteorology data needed for the gap-filling processing; see the processing notes for details on how this works
    if len(data_qc.filter(like='RH_').columns) >0:
        print('Multiple RH columns')
        for k in range (0,len(data_qc.filter(like='RH_').columns)):
            sel = data_qc.filter(like='RH_').columns
            data_qc[sel[k]][data_qc[sel[k]]>101] = np.NaN
            data_qc[sel[k]][data_qc[sel[k]]<0] = np.NaN
    if len(sel)>0:
        data_qc['RH'] = data_qc[sel].mean(axis=1)
        del sel
    if len(data_qc.filter(like='TA_').columns) >0:
        print('Multiple TA columns')
        sel = data_qc.filter(like='TA_').columns
        if len(sel)>0:
            data_qc['TA'] = data_qc[sel].mean(axis=1)
        del sel
    qn = (data_qc['TA']>150)&(data_qc['TA']<400) #Check to seee if temperature is in Kelvin or not
    data_qc['TA'][qn] = data_qc['TA'][qn] - 273.15 # Convert temperature to Celsius
    data_qc['TA'][data_qc['TA']<-500] = np.NaN
    if len(data_qc.filter(like='VPD').columns) == 0:
        Es = 0.61121*np.exp((18.678-(data_qc['TA']/234.5))*(data_qc['TA']/(257.14+data_qc['TA']))) # Calculates saturation vapor pressure for VPD calculation
        E = (data_qc['RH']/100) * Es
        data_qc['VPD'] = Es - E
        print('Calculated VPD')
    else: data_qc['VPD'] = data_qc[data_qc.filter(like='VPD').columns[0]]; print('Renamed to VPD')
    if Site == 'CAF':
        data_qc['VPD'] = data_qc['VPD']/1000
        data_qc['PA'] = data_qc['PA']/1000
        print('Convert CAF to kPa')
    data_qc['VPD'][data_qc['VPD']>35] = np.NaN
    if ('SW_IN' not in data_qc.columns) & ('Rg' not in data_qc.columns):
        data_qc['SW_IN'] = data_qc['PPFD_IN']/2.1
        data_qc['SW_IN'][data_qc['SW_IN']< -100] = np.NaN
    if 'Rg' in data_qc.columns:
        if Site != 'UCB': 
            data_qc['SW_IN'] = data_qc['Rg']
    s = data_qc.index[0]; ss = s
    s+= datetime.timedelta(days=5)
    with warnings.catch_warnings(): #Despike function for turbulent fluxes
        warnings.simplefilter("ignore", category=RuntimeWarning)
        FC = LLT.Despike_7(s,ss,data_qc['FC'].astype(float),'FC',5,3)
        LE = LLT.Despike_7(s,ss,data_qc['LE'].astype(float),'LE',5,3)
        H = LLT.Despike_7(s,ss,data_qc['H'].astype(float),'H',5,3)
        
    data_qc['LE'] = data_qc['LE'][LE['LE']] 
    data_qc['H'] = data_qc['H'][H['H']] 
    data_qc['FC'] = data_qc['FC'][FC['FC']] 
    FileName = 'C:\\Users\\Eric\\Desktop\\LTAR\\LTAR_National_Projects\\REDDY\\L2_QC_Gapped_'+nme+'.txt'
    REF.REddy_Format(data_qc, FileName,'Start_AF') # Function that format
    print('**********Completed '+FileName[72:]+'**********')
    df = [];data_qc = []
    
