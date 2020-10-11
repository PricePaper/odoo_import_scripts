#!/usr/bin/env python3
from xmlrpc import client as xmlrpclib
import ssl
import csv

from scriptconfig import URL, DB, UID, PSW, WORKERS


socket = xmlrpclib.ServerProxy(URL,context=ssl._create_unverified_context())

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
            if loc and loc not in all_locations:
                vals = {
                       'name':loc,
                       'parent_id':stock_location,
                       'usage':'internal',
                       'active':True
                       }
                id = socket.execute(DB, UID, PSW, 'stock.location', 'create', vals)
                print(id)
                all_locations[loc] = id
        except Exception as e:
            print(e)
