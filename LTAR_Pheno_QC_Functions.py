# -*- coding: utf-8 -*-
"""
Created on Tue Aug 21 15:53:46 2018

@author: Eric S. Russell
Laboratory for Atmospheric Research
Dept. of Civil and Environmental Engineering
Washington State University
eric.s.russell@wsu.edu
"""
import numpy as np
import pandas as pd
import datetime
"""        
QA/QC processing for flux data:
    Assumes the column headers follow the AmeriFlux format; other formats will cause errors
    Does NOT check for signal strength or other QC/QA variables; assumes the data has already gone through those checks since this is being used for a 
    synthesis project; not base post-processing. there is another post-processing script with a more rigorous QA/QC system that could be used.
    Outputs:
        data:  Dataframe with the filtered data; does not track reason for removing data.     
"""
def Grade_cs(df,info, Site, site=False):
    if site == True: #Site wuld trigger a specific set of bounds based on the Site-ID used from the initial driver   
        grade = int(info['grade'][Site])
        LE_B = [float(info['LEL'][Site]),float(info['LEU'][Site])]
        H_B = [float(info['HL'][Site]),float(info['HU'][Site])]
        F_B = [float(info['FCL'][Site]),float(info['FCU'][Site])]
        T_B = [float(info['TL'][Site]),float(info['TU'][Site])]
    elif site == False: #The bound limits are within the driver *csv sheet, not a separate driver *csv as in the above
        grade = int(info['Val_L']['grade'])
        LE_B = [float(info['Val_L']['LE_B']),float(info['Val_U']['LE_B'])]
        H_B = [float(info['Val_L']['H_B']),float(info['Val_U']['H_B'])]
        F_B = [float(info['Val_L']['F_B']),float(info['Val_U']['F_B'])]
        T_B = [float(info['Val_L']['T_B']),float(info['Val_U']['T_B'])]
    #Assumes AF-based headers system
    gg = ['H_SSITC_TEST','LE_SSITC_TEST','FC_SSITC_TEST','TAU_SSITC_TEST'] 
    cls =['H','LE','FC', 'TAU']
    pd.options.mode.chained_assignment = None  
    if (grade >9) | (grade<1):
        print('Error: Grade number must be between 0-9.')
        return  # 'exit' function and return error 
    Good = None
    data = []; data=pd.DataFrame(data,index=df.index)
    if cls[1] in df.columns: #Check the LE flux against the bounds
        HL = (df[cls[1]].astype(float) < LE_B[0]) | (df[cls[1]].astype(float)>LE_B[1]) | df[cls[1]].astype(float).isnull()
        if gg[1] in df.columns:
            Grade = (df[gg[1]].astype(float) <= grade) & (~HL)
        else: Grade = ~HL
        df[cls[1]][~Grade] = np.NaN
        data[cls[1]+'_Flag'] = 0
        data[cls[1]+'_Flag'][~Grade] = 1
    if cls[0] in df.columns: #Check the H flux against the bounds
        HL = (df[cls[0]].astype(float) < H_B[0]) | (df[cls[0]].astype(float)> H_B[1]) | df[cls[0]].astype(float).isnull()
        if gg[0] in df.columns:
            Grade = (df[gg[0]].astype(float) <= grade) & (~HL)
        else: Grade = ~HL
        df[cls[0]][~Grade] = np.NaN
        data[cls[0]+'_Flag'] = 0
        data[cls[0]+'_Flag'][~Grade] = 1
    if cls[2] in df.columns: #Check the FC flux against the bounds
        HL = (df[cls[2]].astype(float) < F_B[0])|(df[cls[2]].astype(float) > F_B[1]) | df[cls[2]].astype(float).isnull()
        if gg[2] in df.columns:
            Grade = (df[gg[2]].astype(float) <= grade) & (~HL)
        else: Grade = ~HL
        df[cls[2]][~Grade] = np.NaN
        data[cls[2]+'_Flag'] = 0
        data[cls[2]+'_Flag'][~Grade] = 1
    if cls[3] in df.columns: #Check TAU against the bounds
        HL = (df[cls[3]].astype(float) < T_B[0])|(df[cls[3]].astype(float) > T_B[1]) | df[cls[3]].astype(float).isnull()
        if gg[3] in df.columns:
            Grade = (df[gg[3]].astype(float) <= grade) & (~HL)
        else: Grade = ~HL
        data[cls[3]+'_Flag'] = 0
        data[cls[3]+'_Flag'][~Grade] = 1
        # Rain Mask if precip is in the dataset
    if 'P' in df.columns:
        Precip = (df['P'].astype(float) == 0) | (df['P'].astype(float) == -9999)
        precip = True
        data['P_Flag'] = 0
        data['P_Flag'][~Precip] = 1
    else: precip = False     
    if precip:
        Good = Precip
    if Good is not None:
        if cls[3] in df.columns:
            df[cls[3]][~Good] = np.NaN
        if cls[2] in df.columns:
            df[cls[2]][~Good] = np.NaN
        if cls[1] in df.columns:
            df[cls[1]][~Good] = np.NaN
        if cls[0] in df.columns:
            df[cls[0]][~Good] = np.NaN
    return df, data

#Fills in the blanks spaces with NaN's so the time index is continuous
def indx_fill(df, time):   
    df.index = pd.to_datetime(df.index)
        # Sort index in case it came in out of order, a possibility depending on filenames and naming scheme
    df = df.sort_index()
        # Remove any duplicate times, can occur if files from mixed sources and have overlapping endpoints
    df = df[~df.index.duplicated(keep='first')]
        # Fill in missing times due to tower being down and pad dataframe to midnight of the first and last day
    idx = pd.date_range(df.index[0].floor('D'),df.index[len(df.index)-1].ceil('D'),freq = time)
    df = df.reindex(idx, fill_value=np.NaN)
    return df

# Reads in a directory of files based on the format for either EddyPro or EasyFlux
def Fast_Read(filenames, time, form):
    if len(filenames) == 0:
        print('No Files in directory, check the path name.')
        return  # 'exit' function and return error
    else:
        #Initialize dataframe used within function
        Final = [];Final = pd.DataFrame(Final)
        if form == 'EF':
            for k in range (0,len(filenames)):
                df = pd.read_csv(filenames[k],index_col = 'TIMESTAMP',header= 1,skiprows=[2,3],low_memory=False)
                Final = pd.concat([Final,df], sort = True)
        elif form == 'EP':
            for k in range (0,len(filenames)):
                df = pd.read_csv(filenames[k],header= 1,skiprows=[2],sep=',',low_memory=False)
                Final = pd.concat([Final,df])
            Final.index = Final['date']+' '+Final['time'] # Eddypro outputs both time and date as separate columns
            Final =Final.drop(['filename'],1) # not needed string-based column; gets in the way of converting to floating point
        elif form == 'Biomet':
            for k in range (0,len(filenames)):
                df = pd.read_csv(filenames[k],header= 0,skiprows=[1],sep=',',low_memory=False)
                Final = pd.concat([Final,df])
            Final.index = Final['date']+' '+Final['time'] # Eddypro outputs both time and date as separate columns
        else: 
            print('Format must be either EF or EP')
            return
        # Convert time index
        Final = Final.sort_index()
        Out = indx_fill(Final, time)
    return Out # Return dataframe to main function.    

#Function to despike flux data using the mean and standard deviation method.
def Despike_7(s,ss,x,lab,delta_time, multi):
    an,Tim = [],[]
    while ss < x.index[-1]:
        x_m = np.nanmean(x[ss:s])
        x_s = np.nanstd(x[ss:s])
        x_d = x[ss:s]
        an.append((x_d > (x_m-(multi*x_s))) & (x_d < (x_m+(multi*x_s))))
        ss+= datetime.timedelta(days=delta_time)
        Tim.append((x_d.index))
        s+= datetime.timedelta(days=delta_time)
    qq = np.hstack(an)
    an = pd.DataFrame(qq, columns = [lab])
    an.index = np.hstack(Tim)
    an = an[~an.index.duplicated(keep='first')]
    return an