
# The orchestrator, delegates everything 
# 
# 

class Bitcask:
    def __init__(self, directory):
        
        # TODO: call recovery.recover() on init
        self.directory = directory
        
    def get(self, key):
        pass 
        
    def put(self, key, val):
        pass 
        
    def delete(self, key):
        pass 
        
    def compact(self):
        # delegates to comparator 
        pass 
        
    def close(self):
        pass 
        
    