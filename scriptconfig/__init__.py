import json

with open("import_scripts.conf", "r") as f:
    config = json.load(f)

URL = config['url']
DB = config['db']
UID = config['uid']
PSW = config['password']
WORKERS: int = int(config['workers'])
DB_DSN = config['db_dsn']

url = URL
db = DB
pwd = PSW
