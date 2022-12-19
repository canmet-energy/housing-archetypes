#==================================================================================================================================================
import pandas as pd
from datetime import datetime

low_memory=False

import dask.dataframe as dd
import dask.array as da
import dask.bag as db


#==================================================================================================================================================
##### Preprocess 'EE.EVALUATIONS.csv'

print("Reading EE-Evaluations\n")

df_evaluation = pd.read_csv('EE.EVALUATIONS.csv')

# Remove extra columns
df_evaluation = df_evaluation.drop([
    'EGHFCOSTELEC',
    'EGHFCOSTNGAS',
    'EGHFCOSTOIL',
    'EGHFCOSTPROP',
    'EGHFCOSTTOTAL',
    'UGRFCOSTELEC',
    'UGRFCOSTNGAS',
    'UGRFCOSTOIL',
    'UGRFCOSTPROP',
    'UGRFCOSTTOTAL',
    'CREDITPV',
    'CREDITWIND',
    'UGRCREDITPV',
    'UGRCREDITWIND',
    'CREDITTHERMST',
    'CREDITVENT',
    'CREDITGARAGE',
    'CREDITLIGHTING',
    'CREDITEGH',
    'CREDITOTH1OTH2',
    'TBSMNT',
    'TMAIN',
    'EGHCRITNATACH',
    'EGHCRITTOTACH',
    'UGRCRITNATACH', 
    'TOTALOCCUPANTS',
    'PLANSHAPE',
    'EGHHLAIR',
    'EGHHLFOUND',
    'EGHHLCEILING',
    'EGHHLWALLS',
    'EGHHLWINDOOR',
    'UGRHLAIR',
    'UGRHLFOUND',
    'UGRHLCEILING',
    'UGRHLWALLS',
    'UGRHLWINDOOR',
    'UGRRATING',    
    'NELECTHERMOS',
    'UGRNELECTHERMOS',
    'EPACSA',
    'UGREPACSA',
    'SUPPHTGTYPE1',
    'SUPPHTGTYPE2',
    'SUPPHTGFUEL1',
    'SUPPHTGFUEL2',
    'UGRSUPPHTGTYPE1',
    'UGRSUPPHTGTYPE2',
    'UGRSUPPHTGFUEL1',
    'UGRSUPPHTGFUEL2',
    'EPACSASUPPHTG1',
    'EPACSASUPPHTG2',
    'UEPACSASUPPHTG1',
    'UEPACSASUPPHTG2',
    'HVIEQUIP',
    'UGRHVIEQUIP',    
    'TOTCSIA',
    'LARGESTCSIA',    
    'SNDHEATSYS',
    'SNDHEATSYSFUEL',
    'SNDHEATSYSTYPE',
    'SNDHEATAFUE',
    'SNDHEATDCMOTOR',
    'SNDHEATMANUFACTURER',
    'SNDHEATMODEL',
    'SNDHEATESTAR',
    'UGRSNDHEATSYS',
    'UGRSNDHEATSYSFUEL',
    'UGRSNDHEATSYSTYPE',
    'UGRSNDHEATAFUE',
    'UGRSNDHEATDCMOTOR',
    'UGRSNDHEATMANUFACTURER',
    'UGRSNDHEATMODEL',
    'UGRSNDHEATESTAR',
    'NUMWINZONED',
    'NUMDOORZONED',
    'UGRNUMWINZONED',
    'UGRNUMDOORZONED',
    'HVIESTAR',
    'ESTARMURBHRVHVI',
    'UGRHVIESTAR',
    'UGRMURBHRVHVI',
    'UGRESTARMURBHRVHVI',
    'MINR10EXPFLOOR',
    'UGRMINR10EXPFLOOR',
    'NUMWINU122',
    'UGRNUMWINU122',
    'NUMWINU105',
    'UGRNUMWINU105',
    'NUMER40PLUS',
    'UGRNUMER40PLUS',
    'NUMER34TO39',
    'UGRNUMER34TO39',
    'NUMBEROFHEADS',
    'UGRNUMBEROFHEADS',    
    'BATTERYSTORAGE',
    'UGRBATTERYSTORAGE',
    'GREENERHOMES',
    'BACKWATERVALVE',
    'UGRBACKWATERVALVE',
    'SUMPPUMP',
    'UGRSUMPPUMP',
    'PROGSMARTTHERMOSTAT',
    'UGRPROGSMARTTHERMOSTAT'
], 
    axis=1)

# Remove all cases before a specific date
df_evaluation['python-date'] = pd.to_datetime(df_evaluation['CREATIONDATE'])
df_evaluation = df_evaluation[~(df_evaluation['python-date'] < '2020-04-1')]

# Remove all cases with a FileNumber starting with 98
df_evaluation['StartWith98'] = df_evaluation.apply(lambda x: x['FILENUMBER'].startswith('98'), axis=1)
df_evaluation = df_evaluation[(df_evaluation['StartWith98'] == False)]

# Save the dataframe as .csv file
df_evaluation.to_csv('preprocessed_eval.csv')
#==================================================================================================================================================


