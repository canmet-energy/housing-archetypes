from turtle import width
import pretty as p
import pymssql  
import time as clock 
import pandas as pd 
import re

import warnings



sep = "    "
def connect(this_server,this_db,this_user,this_pwd):
    p.fn_start()
    p.stream(sep+"Establishing connection to remote server "+this_server)  
    p.timer_start("sqlcxcn")  
    p.log("Coordinates:")
    p.log(" - Server= "+this_server)
    p.log(" - User=   "+this_user)
    p.log(" - Pwd=    ********")
    p.log(" - DB=     "+this_db)
    conn = pymssql.connect(server=this_server, user=this_user, password=this_pwd, database=this_db)  
    interval = p.timer_stop("sqlcxcn")
    p.log ("Now connected to remote server.[Lapsed time: "+interval+"s]") 
    p.fn_end()
    return conn   

def querysql(sqlstring,cxcn):
    p.fn_start()
    p.log ("Running SQL query: "+sqlstring+"")
    tic = clock.time() 
    p.timer_start("querysql-start")
    cursor = cxcn.cursor()
    cursor.execute(sqlstring)
    col_names = [i[0] for i in cursor.description]
    if col_names == ['']:
      col_names = ['default']
    data = cursor.fetchall() 
    result = pd.DataFrame(data, columns=col_names)
    lapsed = p.timer_stop("querysql-start")
    p.log ("SQL Query done, [[Lapsed time: "+lapsed+"s]")
    p.fn_end()
    return result 

def BREAKreturn_query_in_chunks(query,csv_target,cxcn):

  query = "SELECT DISTINCT PROGRAMNAME FROM EE.EVALUATIONS" 
  result = querysql(query,cxcn)

  print(result) 

  return result


def count_rows_to_return(sql,cxcn):
  p.fn_start()
  p.timer_start("row-count-routine")

  select_range = re.compile('SELECT .* FROM')
  #select_columns = re.compile('')
  query_count = select_range.sub("SELECT TOP 1 COUNT (*)  FROM",sql)

  p.log ("Counting number of rows via query:"+ query_count)

  result = querysql(query_count,cxcn)

  row_count = str(result['default'][0])
  
  p.log ("Query will return "+str(row_count)+" rows")
  p.fn_end()
  return row_count

def run_arbitrary_query(query,cxcn):
  p.fn_start()
  p.timer_start("arbitray sql query")
  p.log("Attempting to run supplied sql, writing results to " + query['target'])
  
  raw_query = ""

  #print (query["sql"])

  for action in query["sql"]:
    p.log ("ACTION:" + action)
    raw_query = raw_query + " " + action + " " +  arr_to_strlist(query["sql"][action])

  p.log("SQL: " + raw_query)

  if (query["page_results"]): 
    p.log("Paging results")
    #row_count = count_rows_to_return(raw_query, cxcn)

    run_query_in_chunks(raw_query,query['target'],cxcn)
    
  else: 
    p.log("Returning all results at once.")
    result = querysql(raw_query,cxcn)
    result.to_csv(query['target'], mode='w', index=False, header=True)

  p.fn_end()

def run_query_in_chunks(query,csv_target,cxcn):
  p.fn_start()
  p.timer_start("run_query_in_chunks")
  chunksize  = 500
  chunkcount = 0
  row_count = int(count_rows_to_return(query, cxcn))
  p.log("Query will recover "+str((row_count))+" rows")
  maxchunks = int(row_count/chunksize)+1 
  p.log("Starting query")
  cursor = cxcn.cursor()
  cursor.execute(query)
  col_names = [i[0] for i in cursor.description]
  

  p.log ("Parsing remote table and extracting data")

  pause_time = 0 
  pause_now  = 100

  short_run = False
  return_now = 200

  #ignore by warning about column conversion
  warnings.filterwarnings("ignore", message="invalid value encountered in cast")

  if (short_run):
    p.log("Short run is active. Run will be stopped after "+str(return_now)+" chunks")

  
  for chunk in range(maxchunks):

    p.timer_start("this-chunk")
    p.timer_stop("this-chunk")

    res = cursor.fetchmany(size=chunksize)
    start_row = chunk * chunksize  
    
    df = pd.DataFrame(res, columns=col_names)

    if  ( chunk == 0 ):
      df.to_csv(csv_target, mode='w', index=False, header=True)
    else:              
      df.to_csv(csv_target, mode='a', index=False, header=False)

    del res 
    del df       

    interval = p.timer_stop("this-chunk") 
    status = sep+"Fetched chunk #" + str(chunk)+"/"+str(maxchunks) +", rows "+str(start_row) +"->"+str(start_row+chunksize)+ " / "+str(interval)+" seconds "

    pad_len = p.term_w - len(status) - 10
    status = status + " "*pad_len
    p.stream("\r"+status, LE="")
    pause_time = pause_time + 1
    
    if pause_time == pause_now+1:
      pause_time = 0
      p.pause(10,"Pausing after chunk #" + str(chunk)+"/"+str(maxchunks) +" to reduce load on SQL server")
    
    if (short_run and chunk >= return_now):
      p.log("Stopping extraction early after "+str(return_now)+" chunks")
      break

  cursor.close()
  p.stream("")
  p.stream(sep+"Table extraction complete. Data saved to: "+csv_target)

  p.log("Lapsed time query: "+p.timer_stop("run_query_in_chunks")+" s")        

  p.fn_end()
  return

def BREAKget_table_in_chunks(table,columns,csv_target,cxcn): 
    p.fn_start()
    p.timer_start("chunks_routine")
    p.log("Column variable is a "+str(type(columns)))
    global width
    if type(columns) == list: 
      column_list = arr_to_strlist(columns)   
    else: 
      column_list = columns

    chunksize  = 500
    chunkcount = 0

    p.log("Counting rows in table "+ table )
    row_count  = querysql('SELECT TOP 1 COUNT(*) FROM '+table,cxcn)
    
    maxchunks  = int(row_count[0][0]/chunksize)+1 
    p.stream (sep+"Table contains "+str(row_count[0][0])+" rows, extracting in "+str(maxchunks)+" steps" )
  
    sqlstring = "SELECT " + column_list + " FROM " + table  

    cursor = cxcn.cursor()
    cursor.execute(sqlstring)
    p.log ("Parsing remote table "+table+" and extracting data")
    #p.log ("SQL String" + sqlstring )
    pause_time = 0 
    pause_now  = 100

    short_run = False
    return_now = 200
    if (short_run):
      p.log("Short run is active. Run will be stopped after "+str(return_now)+" chunks")
    for chunk in range(maxchunks):
      p.timer_start("this-chunk")
    p.log("Lapsed time for table "+table+": "+p.timer_stop("chunks_routine")+" s")
    p.fn_end()
    return 


def BREAKrun_query_in_chunks2(query,cxcn):

  p.log_to_screen_on()
  p.fn_start()
  

  sql_query = ""
  sql_count = ""
  columns = []
  for keyword in query:
    var = query[keyword]
    if type(var) == list:
      arguements = arr_to_strlist(var)
    else: 
      arguements = var 
    p.log(keyword + " "+arguements)
    sql_query= sql_query + " " + keyword + " " + arguements 
    if (keyword == "SELECT"):
      sql_count = "SELECT COUNT(*)" 
      columns = var
    else: 
      sql_count = sql_count + " " + keyword + " " + arguements 
  
  p.log("STRING Q:"+sql_query)
  p.log("COUNT Q :"+sql_count )

  p.log("Counting rows to be returned by query" )
  row_count  = querysql(sql_count,cxcn)
  p.log("ROWS: "+str(row_count))
  cursor = cxcn.cursor()
  p.log("Call to execute")
  cursor.execute(sql_query)
  p.log ("Call complete.")

  chunksize = 1000
  maxchunks  = int(row_count[0][0]/chunksize)+1 
  csv_target = "name.csv"
  for chunk in range(maxchunks):
    tic = clock.time() 
    res = cursor.fetchmany(size=chunksize)
    start_row = chunk * chunksize       
    df = pd.DataFrame(res, columns=columns) 
    if  ( chunk == 0 ):
      df.to_csv(csv_target, mode='w', index=False, header=True)
    else:              
      df.to_csv(csv_target, mode='a', index=False, header=False)
    del res 
    del df 

    toc = clock.time()
    lapse = toc - tic     
    p.stream("\r"+sep+"Fetched chunk #" + str(chunk)+"/"+str(maxchunks) +", rows "+str(start_row) +"->"+str(start_row+chunksize)+ " / "+str(lapse)+" seconds       ", LE="")
        
  cursor.close()

  p.fn_end()
  return



def arr_to_strlist(arr):
  p.fn_start()
  strlist = ""
  first = True 
  for item in arr: 
    if  not first:
      strlist = strlist + ", "
    else:
      first = False
    strlist = strlist + item
  p.fn_end()
  return strlist 

def dic_to_strlist(dic):

  p.fn_start()
  #p.log("dic is a :"+str(type(dic)))

  if ("password" in dic and dic["password"] != None ):
    dic["password"] = '*******'
  strlist = ""
  first = True 
  for key in dic:
    #p.log ("KEY:"+key)
    value = str(dic[key])
    if  not first:
      strlist = strlist + ", "
    else: 
      first = False 
    strlist = strlist + key + "="+value
  p.fn_end()
  return strlist
    

