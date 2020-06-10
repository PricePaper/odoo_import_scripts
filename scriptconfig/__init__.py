import json
import sys

with open("import_scripts.conf", "r") as f:
    config = json.load(f)

# Append odoo to path
sys.path.append(config['odoo_path'])

URL = config['url']
DB = config['db']
UID = config['uid']
PSW = config['password']
WORKERS: int = int(config['workers'])

url = URL
db = DB
pwd = PSW
