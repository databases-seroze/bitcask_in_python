from datafile import DataFile 
from keydir import KeyDir 

# Startup logic, isolated from main class 
# Scans directory, finds data files and hint files 
# for files without hint files; scans data files, validates CRC, builds keydir and hint files 
# Handles corrupt trailing records in the active file (truncate)
# 
def recover(directory) -> (KeyDir, list[DataFile], active_file_id):
    pass
   
 
    