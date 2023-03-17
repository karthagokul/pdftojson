import os
import subprocess
import json
import sys
import logging
from logging import handlers
log_file_name = "autotester_log.txt"
def setup_custom_logger(name):
    '''
    We strictly logs only into a file, not to stdout, because the stdout is used by other program and only json is allowed for stdout
    '''
    formatter = logging.Formatter(fmt='%(asctime)s %(levelname)-8s %(message)s',
                                  datefmt='%Y-%m-%d %H:%M:%S')
    handler = logging.handlers.RotatingFileHandler(log_file_name, mode='a', maxBytes=5000000, backupCount=5)
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)
    return logger

logger = setup_custom_logger('datainsights-engine_autotester')

directory = "."
if len(sys.argv) > 1:
    directory = sys.argv[1]


def printlist(l):
    for item in l:
        print(item)

result = [os.path.join(dp, f) for dp, dn, filenames in os.walk(directory) for f in filenames if os.path.splitext(f)[1] == '.pdf']
failed_files=[]
total_files=0
total_success=0
total_fails=0
print("Please wait ... Processing PDFs from " + directory)
for file in result:
    print("processing "+file,end =" ")
    output = subprocess.run(['python', 'datainsights_engine.py', '-i',file], stdout=subprocess.PIPE)
    #print(output.stdout)
    #print(json.loads(output.stdout))
    data=json.loads(output.stdout)
    if data['error_code']==0:
        total_success=total_success+1
        print("[OK] -> Configuration : "+  os.path.basename(data['configuration']))
        logger.info(file+"[OK] -> Configuration : "+  os.path.basename(data['configuration']))
    else:
        total_fails=total_fails+1
        file_data={}
        file_data["file"]=file
        file_data["error"]=data['error_message']
        failed_files.append(file_data)
        print("[NOK] -> Error : " +data['error_message'])
    total_files=total_files+1


print("Summary")
print("Files: " , total_files)
print("Passed: " , total_success)
print("Failures: ",total_fails)
if total_fails > 0 :
    print("Failed to process files below")
    printlist(failed_files)
