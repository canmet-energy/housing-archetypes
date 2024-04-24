

import sys 
import pandas as pd
import time as clock
import pymssql as mssql 
import json

from optparse import OptionParser

# Local modules in /housing_archtypes/lib
sys.path.insert(1, '../../lib')
import pretty as p
import sql as sql 

# Toggle screen logging
log_to_screen = False  

p.log_to_screen_on()
p.fn_start()

# Print header. 
p.header("extract-archetypes-by-criteria.py: a python script for getting HOT2000 files from ERS")


# Parse definitions file, username & password from the command-line. 
parser = OptionParser()
parser.add_option('-f',"--file",\
  dest="filename",\
  help="JSON file defining tables and columns to extract",\
  metavar="FILE")

parser.add_option('-p',"--pwd",\
  dest="password",\
  help="Password for accessing data server",\
  metavar="PWD")

parser.add_option('-u',"--user",\
  dest="username",\
  help="Username for accessing data server",\
  metavar="USER")

parser.add_option("-v", "--verbose",
                  action="store_false", dest="verbose", default=False,
                  help="print log messages to screen")

(options, args) = parser.parse_args()


server   = 'sqlsvr-nrcan-oee-prod-ods.database.windows.net'
database = 'sqldb-nrcan-oee-hd-prod'
username = options.username+'@sqlsvr-nrcan-oee-prod-ods'
password = options.password

p.log("Command line arguements:"+sql.dic_to_strlist(options.__dict__))


# Parse table / column definitions for extraction. 
p.stream(" -> Parsing table & column definitions from "+options.filename)
p.timer_start("read-definitions")
f = open(options.filename)
table_definitions = json.load(f)
f.close()
intervan = p.timer_stop("read-definitions")


cxcn = sql.connect(server,database,username,password)




p.stream(" -> Compiling a list of available HOT2000 file versions ")
query = "SELECT DISTINCT PROGRAMNAME FROM EE.EVALUATIONS" 
list_of_versions = sql.querysql(query,cxcn)






#p.stream("Compiling a list of Provinces  HOT2000 file versions ")
#query = "SELECT DISTINCT PROVINCE FROM EE.EVALUATIONS WHERE " 
#list_of_provinces = sql.querysql(query,cxcn)

#print(list_of_provinces)

h2k_v = pd.DataFrame(columns=["version","count"])

i = 0 
#for version in [elem[0] for elem in list_of_versions]:
#  p.stream ("Counting number of records for '"+version+"'")
#  query = "SELECT TOP 1 COUNT (*) FROM EE.EVALUATIONS WHERE PROGRAMNAME = '"+version+"'"
#  count = sql.querysql(query,cxcn)
#  h2k_v.loc[i] = [version,count[0][0]]
#  i = i + 1 
#h2k_v.sort_values(by=['version'])
#print(h2k_v)





# h2k_versions = [elem[0] for elem in result]

# Get relevant records for HOT2000 by version:
version = "HOT2000 11.11"
table = "EE.EVALUATIONS"
criteria = "PROGRAMNAME = '"+version+"'"
csv_target = "Evaluations_H2KV11p11.csv"
columns = table_definitions["EE.EVALUATIONS"]["columns"]
column_list = sql.arr_to_strlist(columns) 


#p.stream(" -> Extracting " +str(len(columns))+" cols from table "+table+" where " + criteria )
query = "SELECT TOP 1 COUNT (*)  FROM " + table  # + " WHERE " + criteria+ " WHERE " + criteria
count = sql.querysql(query,cxcn)
p.stream("    Query will return "+str(count[0][0])+" rows.")
query = "SELECT " + column_list + " FROM " + table  # + " WHERE " + criteria
sql.run_query_in_chunks(query,csv_target,cxcn)


p.fn_end()

