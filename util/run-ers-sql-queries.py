
import sys 
import pandas as pd
import time as clock
import pymssql as mssql 
import json

from optparse import OptionParser


sys.path.insert(1, '../lib')
import pretty as p
import sql as sql 

# Toggle screen logging
#global log_to_screen = False 

p.log_to_screen_off()
p.fn_start()


parser = OptionParser()
parser.add_option('-f',"--file",\
  dest="filename",\
  help="JSON file defining SQL queries to run",\
  metavar="FILE")

parser.add_option('-p',"--pwd",\
  dest="password",\
  help="Password for accessing data server",\
  metavar="PWD")

parser.add_option('-u',"--user",\
  dest="username",\
  help="Username for accessing data server",\
  metavar="USER")

parser.add_option('-q',"--query",\
  dest="query",\
  help="query to run on the server",\
  metavar="QUERY")


(options, args) = parser.parse_args()

p.header("run-ers-sql-queries: a python script performing arbitary SQL ")

#print(options["filename"])
p.stream("Parsing sql queries from"+options.filename)

f = open(options.filename)
sql_queries = json.load(f)
f.close()


p.log_to_screen_on()

server   = 'sqlsvr-nrcan-oee-prod-ods.database.windows.net'
database = 'sqldb-nrcan-oee-hd-prod'
username = options.username+'@sqlsvr-nrcan-oee-prod-ods'
password = options.password

query_name = options.query



tic = clock.time()
p.stream(" -> Performing query "+query_name)
cxcn = sql.connect(server,database,username,password)

sql_query = sql_queries[query_name]

sql.run_arbitrary_query(sql_query,cxcn)


#for query in sql_queries:
  #
  #columns = table_definitions[table]["columns"]
  #p.stream(" -> Performing query "+query)
  #cxcn = sql.connect(server,database,username,password)
  #sql.run_query_in_chunks(sql_queries[query]["sql"],cxcn)
  #cxcn.close()
  #toc = clock.time
  #p.stream("Lapsed time: "+str(toc-tic)+" seconds")
  #p.pause(20,"Connection closed. Let the sql server catch its breath")
  #print("")


p.fn_end()
exit()

