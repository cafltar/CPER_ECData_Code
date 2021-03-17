# -*- coding: utf-8 -*-
"""
Created on Fri Jul 12 10:45:15 2019

@author: Eric Russell
eric.s.russell@wsu.edu

Script reformats the FLUXNET output from the EddyPro option into the general AmeriFLux data using a rename library. 
Eliminates all the extra columns that come with the FLUXNET output that are unneeded and clutter the dataframe
"""

import pandas as pd
AF = pd.read_csv('C:\\Users\\Eric\\Desktop\\LTAR\\Column_Names\\AF_EP_EF_Column_Renames.csv',header = 0) # Column library

data = pd.read_csv(r'C:\Users\Eric\Desktop\LTAR\LTAR_National_Projects\PhenologyInitiative\EC Data\Processed\Unprocessed\LTAR_EC_CPER_tgm_20180101_20181231.csv',
                   header = 0) # FLUXNET data set


AM = data; cls = AM.columns # Keeping data as an unchanged variable from this point forward in case want to do more with it; can be changed
# Using data that came from EddyPro so selected the Epro column to check column names against.
s = cls.isin(AF['AmeriFlux'])
# Drop columns not in the AmeriFlux data list
AF_Out = AM.drop(AM[cls[~s]],axis = 1)

AF_Out.to_csv(r'C:\Users\Eric\Desktop\LTAR\LTAR_National_Projects\PhenologyInitiative\EC Data\Processed\Unprocessed\LTAR_EC_CPER_tgm_20180101_20181231.csv', index = False) # Output