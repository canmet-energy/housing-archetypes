
import sys
from colorama import Fore
import time as clock
from inspect import getframeinfo, stack
import re 
from os import get_terminal_size

timers=dict()
term_w = int(get_terminal_size().columns) 

file_short = re.compile(r"^.+[\\|//]")
buffer_ = re.compile(r"^.+[\\|//]")
buffer_rule = re.compile(r"(?:~)( +)(?:~)")
sep = "    "

def get_caller(level=2):
  # returns a formatted dict with details 
  # on a higher function in the stack.
  # By default, queries funciton 2 levels
  # in the stack, allowing functions to 
  # call this one to figure who called 
  # them.
  caller = getframeinfo(stack()[level][0])
  call_details = {
    "func": caller.function,
    "line": str(caller.lineno),
    "file": file_short.sub("",caller.filename)
  }
  return call_details

def log_to_screen_on():
  # Toggles logging to the log screen : ON 
  global log_to_screen 
  log_to_screen= True 
  caller = get_caller()
  log("Log to screen turned on by "+caller["func"]+ " / "+caller["file"]+":"+caller["line"])
  return

def log_to_screen_off():
  # Toggles log messages to screen : OFF
  global log_to_screen 
  caller = get_caller()
  log("Log to screen turned on by "+caller["func"]+ " / "+caller["file"]+":"+caller["line"])
  log_to_screen = False 
  return

def header(text):
  # Streams a formatted header with arbitrary text
  # to screen. 
  global term_w
  rule = "="*term_w 
  stream(rule)
  stream(text)
  stream(rule)
  stream("")
  return

def fn_start():
  # Simple function to log the start of a routine
  caller = sys._getframe(1).f_code.co_name
  msg = "START"
  log(msg, caller)
  return 

def fn_end():
  # Simple function to log the end of a routine
  caller = sys._getframe(1).f_code.co_name
  msg = "END"
  log(msg, caller)
  return   

def log(msg, caller = None ):
  # General purpose logging function. Currently 
  # supports formatted log messages to screen. 
  global log_to_screen
  if ( log_to_screen == False ):
    return 

  line=str()
  file = str()

  c = get_caller()
  line = c["line"]
  file = c["file"]
  if ( caller == None):
    caller = c["func"]
  
  if log_to_screen:
    #buffer = "{:<"+str(log_left_buffer)+"}%{:>"+str(log_right_buffer)+"}"
    l_txt = "["+caller+"] "+ msg
    if (int(len(l_txt)) % 2) != 0:
      l_txt = l_txt + " "
    r_txt = "{"+file+":"+line+"}"
    l_len = len(l_txt)
    r_len = len(r_txt)
    rule_len = int((term_w-l_len-r_len) / 2)
    if (rule_len*2 + l_len + r_len) < term_w: 
      r_txt = "."+r_txt 
    txt = l_txt + ". "*(rule_len)+r_txt
    stream(Fore.RED +txt+Fore.RESET+"")
  
  return

def stream(msg, LE="\n"):
  txt=msg+Fore.RESET+""
  print (txt, end=LE)
  return

def pause(interval,msg):
  global sep 
  for secs in range(interval):
    bar = sep+msg+" ["+"."*secs+" "*(interval-secs)+"]"
    spaces=term_w - len(bar)
    stream("\r"+bar+" "*spaces, LE="")
    clock.sleep(1)

def timer_start(code):
  # gets the current time and saves it a dict with key = passed arguement
  # code. Use with timer_stop(code) to get the lapsed time associted with 
  # an arbitrary interval.
  global timers
  time1 = clock.time()
  timers[code] = time1
  return 

def timer_stop(code):
  # gets the current time, subtacts the previously stored time indicated
  # by arguement 'code'. Formats the result as a string with 4 decimels.
  global timers
  time2 = clock.time()
  lapsed_time = time2 - timers[code]
  txt = "{:.4f}".format(lapsed_time)
  return txt 
