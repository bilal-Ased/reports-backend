import os 

class Config:
  BEARER_TOKEN = os.getenv("BEARER_TOKEN")
    
cfg = Config()