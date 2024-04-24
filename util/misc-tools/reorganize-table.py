
import sys 
import pandas as pd
import time as clock
import pymssql as mssql 
import json
import os
from optparse import OptionParser

file_path = os.path.realpath(__file__)
dir_path = os.path.dirname(file_path)
join_path = os.path.normcase(dir_path+'/../../lib')
lib_path = os.path.normpath(join_path)


sys.path.insert(1, lib_path)
import pretty as p
import sql as sql 

# Toggle screen logging
p.init_log_to_screen()
p.log_to_screen_on()
p.fn_start()

# Print header. 
p.header("reorganize-table.py: a python script for rearranging one csv file to look like another")

sep = "   "

# Parse definitions file, username & password from the command-line. 
parser = OptionParser()
parser.add_option('-s',"--srcfile",\
  dest="srcfile",\
  help="CSV file that should be reformatted",\
  metavar="FILE")

parser.add_option('-t',"--templatefile",\
  dest="templatefile",\
  help="CSV file providing the tempalte for reformatting",\
  metavar="PWD")

parser.add_option("-v", "--verbose",
                  action="store_true", dest="verbose", default=False,
                  help="print log messages to screen")

(options, args) = parser.parse_args()

if (options.verbose):
  p.log_to_screen_on()

p.log("Command line arguements:"+sql.dic_to_strlist(options.__dict__))

p.stream(" 1. CONFIGURATION")
p.stream(sep+"Source Data File: "+options.srcfile)
p.stream(sep+"Template File:    "+options.templatefile)

p.stream("2. INITIALIZATION")
p.stream(sep+"Reading template file: "+options.templatefile)

with open(options.templatefile) as f:
    lines = f.readlines()
    
col_order = lines[0].rstrip().lower().split(",")




p.stream(sep+"Reading data file: "+options.srcfile)


with open(options.srcfile) as file:
    lines = file.readlines()


lines[0] = lines[0].rstrip().lower().replace('ï»¿','')


p.log("Creating recost dataframe")
df_recost = pd.DataFrame([x.split(',') for x in lines[1:]], columns = lines[0].split(','))

p.log(str(df_recost.head()))

# lines[0] = toprow


df_reorg = pd.DataFrame()

rows = len(df_recost.index)
for col_name in col_order:
    col = col_name.replace(' ','_') 
    
    if ( col == 'recosted|total avg'): 
        col_from_recost = 'recosted|total-adjust'
    else:
        col_from_recost = col
    
    p.log(col + " -> " + col_from_recost )

    if ( \
      col =='input|opt-h2kfoundation' or \
      col =='input|opt-h2kfoundationslabcrawl'
    ):
      df_recost[col] = ['-']*rows 

    a = df_recost[col_from_recost]

    df_reorg[col] = a 


p.log(df_reorg.columns)


df_reorg.to_csv("HTAP-costing-reorganized.csv")






