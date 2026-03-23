import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

DATA_RAW_ITR = os.path.join(BASE_DIR, "data", "raw", "itr")
DATA_RAW_DFP = os.path.join(BASE_DIR, "data", "raw", "dfp")
DATA_OUTPUT = os.path.join(BASE_DIR, "data", "processed")

LOG_DIR = os.path.join(BASE_DIR, "logs")

os.makedirs(DATA_RAW_ITR, exist_ok=True)
os.makedirs(DATA_RAW_DFP, exist_ok=True)
os.makedirs(DATA_OUTPUT, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)