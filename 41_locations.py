#!/usr/bin/env python3
from xmlrpc import client as xmlrpclib
import ssl
import csv

from scriptconfig import URL, DB, UID, PSW, WORKERS


socket = xmlrpclib.ServerProxy(URL,context=ssl._create_unverified_context(), allow_none=True)

input_file = 'files/ivlioh.csv'
input_file = csv.DictReader(open(input_file))

all_locations = socket.execute(DB, UID, PSW, 'stock.location', 'search_read', [('usage', '=', 'internal')], ['id','name'])
all_locations = {ele['name']:ele['id'] for ele in all_locations}

stock_location = all_locations.get('Stock', '')
if not stock_location:
    print('WH\Stock location not found')
else:
    for line in input_file:
        try:
            loc = line.get('BIN-CODE', '')
            vals = {
                   'name':loc,
                   'location_id':stock_location,
                   'usage':'internal',
                   'active':True
                   }
            if loc and loc not in all_locations:
                id = socket.execute(DB, UID, PSW, 'stock.location', 'create', vals)
                print('Created', loc, id)
                all_locations[loc] = id
            else:
                id = all_locations.get(loc)
                res = socket.execute(DB, UID, PSW, 'stock.location', 'write', id, vals)
                print("Updated:", loc, id)
        except Exception as e:
            print(e)
