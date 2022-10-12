from turtle import width
import pretty as p
import pymssql  
import time as clock 
import pandas as pd 

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
    res = cursor.fetchall() 
    lapsed = p.timer_stop("querysql-start")
    p.log ("SQL Query done, [[Lapsed time: "+lapsed+"s]")
    p.fn_end()
    return res 

def get_table_in_chunks(table,columns,csv_target,cxcn): 
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
    pause_time = 0 
    pause_now  = 100

    short_run = False
    return_now = 200
    if (short_run):
      p.log("Short run is active. Run will be stopped after "+str(return_now)+" chunks")
    for chunk in range(maxchunks):
      p.timer_start("this-chunk")
      res = cursor.fetchmany(size=chunksize)
      start_row = chunk * chunksize       
      df = pd.DataFrame(res, columns=columns) 
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
    p.log("Lapsed time for table "+table+": "+p.timer_stop("chunks_routine")+" s")
    p.fn_end()
    return 


def run_query_in_chunks(query,cxcn):

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
  p.log("dic is a :"+str(type(dic)))
  if (dic["password"] != None ):
    dic["password"] = '*******'
  strlist = ""
  first = True 
  for key in dic:
    p.log ("KEY:"+key)
    value = str(dic[key])
    if  not first:
      strlist = strlist + ", "
    else: 
      first = False 
    strlist = strlist + key + "="+value
  p.fn_end()
  return strlist
    

