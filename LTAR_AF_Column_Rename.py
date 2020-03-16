# -*- coding: utf-8 -*-
"""
@author: Eric S. Russell
Laboratory for Atmospheric Research
Dept. of Civil and Environmental Engineering
Washington State University
eric.s.russell@wsu.edu
"""

import pandas as pd
import glob
import os
import numpy as np
import datetime
import warnings
# Change this path to the directory where the LTAR_Flux_QC.py file is located
os.chdir(r'C:\Users\Eric\Desktop\PyScripts\Flux_Processing_Code\CPER_LTAR_Code')       
import LTAR_Flux_QC as LLT
import Reddy_Format as RF
Driver = pd.read_csv(r'C:\Users\Eric\Desktop\PyScripts\Flux_Processing_Code\CPER_LTAR_Code\AF_Rename_Template.csv',header = 0, index_col = 'Variable')

#%
AF = pd.read_csv(Driver['Val_L']['AF_Cols'],header = 0) # File path for where the column names sit
files = glob.glob(Driver['Val_L']['files']) #Directory or file name with file names here
MET_QC = Driver['Val_L']['MET_QC']
EP = True if Driver['Val_L']['EP'].upper() == 'TRUE' else False # True if data being used is from EddyPro; must be false if EF is true
EF = True if Driver['Val_L']['EF'].upper() =='TRUE' else False   # True if data being used is from EasyFlux; must be false if EP is true
FN = True if Driver['Val_L']['FN'].upper() =='TRUE' else False
Join = True if Driver['Val_L']['Join'].upper() =='TRUE' else False # True if there are other columns to be readin from a separate file; false if not
Biomet = True if Driver['Val_L']['Biomet'].upper() =='TRUE' else False
Flux = True if Driver['Val_L']['Flux'].upper() =='TRUE' else False
#Soil = True if Driver['Val_L']['Soil'].upper() =='TRUE' else False
Met = True if Driver['Val_L']['Met'].upper() =='TRUE' else False
REP = True if Driver['Val_L']['REP'].upper() =='TRUE' else False
Despike = True if Driver['Val_L']['Despike'].upper() =='TRUE' else False
Format = Driver['Val_L']['Format'].upper() # Which format the initial column headers are in; 'Epro' or 'Eflux' are only accepted; must be in single quotes
#%%***************************                                                   
data= []; data= pd.DataFrame(data) # initialize a blank dataframe
dt = []
for K in range (0,len(files)):
#Read in data and concat to one dataframe; no processing until data all read in - data formatted from EddyPro FullOutput        
    if EP == True:
        df = pd.read_csv(files[K],header= 0,sep=',',skiprows=[1], low_memory=False)
        data= pd.concat([data,df], sort='True')
        data.index = data['date']+' '+data['time'] # Eddypro outputs both time and date as separate columns
#        data =data.drop(['filename'],1) # not needed string-based column; gets in the way of converting to floating point
    elif EF == True:
    #Read in data and concat to one dataframe; no processing until data all read in; formatted for EasyFlux header style
        df = pd.read_csv(files[K],index_col = 'TIMESTAMP',header= 1,skiprows=[2,3],low_memory=False)
        data = pd.concat([data,df], sort='True')
    elif FN == True:
        df = pd.read_csv(files[K],header= 0,sep=',',low_memory=False)
        for k in range (0,len(df)):
            Y =  str(int(df['TIMESTAMP_START'][k]))[0:4]
            M =  str(int(df['TIMESTAMP_START'][k]))[4:6]  
            D =  str(int(df['TIMESTAMP_START'][k]))[6:8]
            hh = str(int(df['TIMESTAMP_START'][k]))[8:10] 
            mm = str(int(df['TIMESTAMP_START'][k]))[10:12]
            dt.append(Y+'-'+M+'-'+D+' '+hh+':'+mm)
        dt = pd.DataFrame(dt);df.index = dt[0]
        df.index=pd.to_datetime(df.index) # Time-based index
        data = df
    else: print('EF or EP needs to be true; script will Error')
    data = LLT.indx_fill(data, '30min')

    if EP or EF or FN:
        data.index=pd.to_datetime(data.index) # Convert to a time-based index
        if Join:
            if Biomet:
                filenames = glob.glob(Driver['Val_L']['Join_cols']) #Directory or file name with file names that need to added to the main list put here
                Final = LLT.Fast_Read(filenames,'30min', 'Biomet') # Read-in data that contains extra columns not in the EddyPro output; specify 'EF' or 'EP' for EasyFlux or EddyPro
                Join_Cols = AF['BioMet'].dropna() # Drop blank columns since this list is shorter than the other lists and good housekeeping
                for k in range (0,len(Join_Cols)): 
                    data=data.join(Final[Join_Cols[k]]) # Loop to join the extra columns as defined above
            elif EF:
                filenames = glob.glob(Driver['Val_L']['Join_cols']) #Directory or file name with file names that need to added to the main list put here
                Final = LLT.Fast_Read(filenames,'30min', 'EF') # Read-in data that contains extra columns not in the EddyPro output; specify 'EF' or 'EP' for EasyFlux or EddyPro
                Join_Cols = AF['Extra_Cols'].dropna() # Drop blank columns since this list is shorter than the other lists and good housekeeping
                for k in range (0,len(Join_Cols)): 
                    data=data.join(Final[Join_Cols[k]]) # Loop to join the extra columns as defined above
        if EP:
#EddyPro outputs the variance which is the square of the standard deviation so need to convert back to standard deviation
            data['u_var'] = data['u_var'].astype(float)**0.5
            data['v_var'] = data['v_var'].astype(float)**0.5
            data['w_var'] = data['w_var'].astype(float)**0.5
            data['ts_var'] = data['ts_var'].astype(float)**0.5
        AM = data; cls = AM.columns # Keeping data as an unchanged variable from this point forward in case want to do more with it; can be changed
# Using data that came from EddyPro so selected the Epro column to check column names against; AF_Rename function add here.
        s = cls.isin(AF[Format])

# Drop columns not in the AmeriFlux data list
        AF_Out = AM.drop(AM[cls[~s]],axis = 1)
        cls = AF_Out.columns  #Grab column headers from AF_Out after dropping unneeded columns
    
# Change column header names and keep only columns that match
        for k in range (2,len(AF)):
            if AF[Format][k] in cls:
                qn = AF[Format][k] == cls
                AF_Out = AF_Out.rename(columns={cls[qn][0]:AF['AMERIFLUX'][k]})
                print('Converting ',AF[Format][k],' to ',AF['AMERIFLUX'][k])
# In case SW_IN not a part of the initial data set; this conversion can work
        if 'SW_IN' not in AF_Out.columns:
            if 'PPFD' in AF_Out.columns:        
                AF_Out['SW_IN'] = AF_Out['PPFD_IN'].astype(float)/2.1
                AF_Out['SW_IN'][AF_Out['SW_IN']< -100] = np.NaN

#Shift time to match AmeriFlux format; can change this depending on how averaging time is assigned
        AF_Out['TIMESTAMP_END'] = AF_Out.index.shift(0, '30T')
        AF_Out['TIMESTAMP_START'] = AF_Out.index.shift(-1, '30T')    
        AF_Out['TIMESTAMP_START']= AF_Out.TIMESTAMP_START.map(lambda x: datetime.datetime.strftime(x, '%Y%m%d%H%M'))
        AF_Out['TIMESTAMP_END']= AF_Out.TIMESTAMP_END.map(lambda x: datetime.datetime.strftime(x, '%Y%m%d%H%M'))
# Format columns into a same order as in the input *.csv file because housekeeping is always good
        acl = AF['AMERIFLUX']
        tt = acl[acl.isin(AF_Out.columns)]
        AF_Out_QC=AF_Out[tt]   

        if Flux:
            print('****** Flux Quality Control ******')
            AF_Out_QC, QC_Dataset = LLT.Grade_cs(AF_Out,Driver,Site=False)
# Meteorology data QC step; send the whole data set and check for if in cls since will be in AF format so can hardcode it for this purpose
        if Met:
            print('****** Meteorology Quality Control ******')
            Met_QC = LLT.Met_QAQC(RH=AF_Out_QC['RH'].astype(float),P=AF_Out_QC['PA'].astype(float)/1000, Tair = AF_Out_QC['TA'].astype(float)-273.15, 
                                  WS = AF_Out_QC['WS'].astype(float), WD = AF_Out_QC['WD'].astype(float), Precip = AF_Out_QC['P'].astype(float)*1000,
                                  Rn =AF_Out_QC['NETRAD'].astype(float),VPD = AF_Out_QC['VPD'].astype(float)/1000,z = 0)
            Met_QC.to_csv(MET_QC)
            AF_Out_QC['TA'] = Met_QC['Tair_Filtered']
            AF_Out_QC['RH'] = Met_QC['RH_Filtered']
            AF_Out_QC['PA'] = Met_QC['P_Filtered']
            AF_Out_QC['WS'] = Met_QC['WS_Filtered']
            AF_Out_QC['WD'] = Met_QC['WD_Filtered']
            AF_Out_QC['NETRAD'] = Met_QC['Rn_Filtered']
            AF_Out_QC['VPD'] = Met_QC['VPD_Filtered']
            AF_Out_QC['P'] = Met_QC['Precip_Filtered']
# Add in Despike function - Need to clean this up; pretty crummy looking code - might have done that somewhere.
        if Despike:
            if ~Flux:
                QC_Dataset=[]; QC_Dataset = pd.DataFrame(QC_Dataset,index = AF_Out.index)
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", category=RuntimeWarning)
                s = AF_Out_QC.index[0]; ss = s
                s+= datetime.timedelta(days=5)
                QC_Dataset_H = LLT.Despike_7(s,ss,AF_Out_QC['H'].astype(float),'H_Despike',5, 3.5)    
                QC_Dataset_LE = LLT.Despike_7(s,ss,AF_Out_QC['LE'].astype(float),'LE_Despike',5, 3.5)    
                QC_Dataset_FC = LLT.Despike_7(s,ss,AF_Out_QC['FC'].astype(float),'FC_Despike',5, 3.5)
                AF_Out_QC['FC'] = AF_Out_QC['FC'][QC_Dataset_FC['FC_Despike']]
                AF_Out_QC['LE'] = AF_Out_QC['LE'][QC_Dataset_LE['LE_Despike']]
                AF_Out_QC['H']  = AF_Out_QC['H'][QC_Dataset_H['H_Despike']]
                QC_Dataset = QC_Dataset.join(QC_Dataset_H).join(QC_Dataset_LE).join(QC_Dataset_FC)
# Format for the gap-filling code - Need to add a variable for the path of the rename script?
        if REP:
            print('****** Format for Gap-filling ******')
            RF.REddy_Format(AF_Out_QC, Driver['Val_L']['REDDY_File'], 'Start_AF')
# Ameriflux uses -9999 to represent missing data so convert NaN to -9999
    AF_Out_QC = AF_Out_QC.fillna(-9999)
#%%
# Change output directory to whatever it needs to be
    cols = AF_Out_QC.columns.tolist()
    cols.insert(0,cols.pop(cols.index('TIMESTAMP_START')))
    cols.insert(1,cols.pop(cols.index('TIMESTAMP_END')))
    AF_Out_QC = AF_Out_QC.reindex(columns = cols) 
    AF_Out_QC.to_csv(files[K][:-4]+'_QC.csv',index = False)
#    AF_Out_QC.to_csv(Driver['Val_L']['Output'], index = False)
else: print('Select either EF or EP as true')