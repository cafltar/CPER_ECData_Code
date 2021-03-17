
# Read_me for the CPER data processing scripts

Updating slowly. See bottom for last update date.

## Scripts

### FLUXNET_Rename
- Script reformats the FLUXNET output from the EddyPro option into the general AmeriFLux data using a rename library (AF_EP_EF_Column Renames). 
- Eliminates all the extra columns that come with the FLUXNET output that are unneeded and clutter the dataframe
- Not part of the general processing stream

### Reddy_Format
- Formats the data into needed headers for the REddyProc input format.
- Assumes all units are in the correct form and only converts time to the correct form and names column headers based off the column name library (hard-coded) 
- Operates off the AmeriFLux column header basis for consistency; often last step in workflow

### LTAR_Phenology_Initiative_QC_REddy
- 


## Other files

### ProcessingFlowChart
- General flow of the processing scheme for the CAF EC data for yearly data from start to finish. Not all scripts used in that schema are in this repo and specific to CAF setup.

### Flux_Processing_SOP_LTAR
- Draft SOP for processing CAF EC data. As of this update; is still out of date in many spots but being updated slowly as time permits and processing updates are made. 
- Is a copy of what the "more alive" document kept in a separate repo/by CAF so is behind what CAF maintains. 

### AF_EP_EF_Column_Renames
- Equivalent column headers for AmeriFlux, EddyPro, and EasyFlux columns for a core set of variables from an EC flux tower. Is also and extra column for the BioMet data or data from another data set (Extra_Cols) if needed. 
- Units colum reflect the units required by AmeriFlux for upload. 
- This list *is not* exhaustive and cna be added to as needed; this version probably is out of date relative to its original usage.

### AF_Rename_Template
- Driver template for the input and ouptut files along with various options for the different processing and formatting variations inherent in the system.
- Eventually would be nice to be used as part of a GUI instead of Excel sheet; requires generalization.

#### Last updated: 2020-03-17 ESR