
# Parallel to a short data file 
# Has it's own binary format [key_size|val_size|offset|timestamp|key], this is used to compute keyDir quickly on startup

class HintFile:
    def __init__(self):
        pass 
        
    def write_hint_file(self, file_id, entries, directory):
        pass 
        
    def read_hint_file(self, file_id, directory):
        pass 
        
    