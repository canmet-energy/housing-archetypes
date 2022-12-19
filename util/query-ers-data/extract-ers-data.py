
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
p.header("extract-ers-data.py: a python script for pulling ERS data")



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
p.stream("Parsing table & column definitions from "+options.filename)
p.timer_start("read-definitions")
f = open(options.filename)
table_definitions = json.load(f)
f.close()
intervan = p.timer_stop("read-definitions")


p.stream("Beginning data dump.\n")
for table in table_definitions:
  p.timer_start(table+"-extract")
  columns = table_definitions[table]["columns"]
  p.stream(" -> Extracting " +str(len(columns))+" cols from table "+table)
  cxcn = sql.connect(server,database,username,password)
  sql.get_table_in_chunks(table,columns,table+".csv",cxcn)
  cxcn.close()
  interval = p.timer_stop(table+"-extract")
  p.stream("    "+table+" extraction complete ("+interval+"s)")
  p.log("Extraction of "+table+" complete [Lapsed time: "+interval+"s]")
  p.pause(20,"Connection closed. Pause to reduce load on SQL server")

  print("")

p.fn_end()
exit()

