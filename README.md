
# Read_me for the CPER data processing scripts

## FLUXNET_Rename
- Script reformats the FLUXNET output from the EddyPro option into the general AmeriFLux data using a rename library. 
- Eliminates all the extra columns that come with the FLUXNET output that are unneeded and clutter the dataframe

## Reddy_Format
- Formats the data into needed headers for the REddyProc input format.
- Assumes all units are in the correct form and only converts time to the correct form and names column headers based off the column name library (hard-coded) 
- Operates off the AmeriFLux column header basis for consistency; often last step in workflow

## LTAR_Phenology_Initiative_QC_REddy
- 


#### Last updated: 2020-03-10 ESR