#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import xmlrpc.client
import ssl
import csv

from scriptconfig import url, db, pwd

socket = xmlrpc.client.ServerProxy(url,context=ssl._create_unverified_context())



categs = socket.execute(db, 2, pwd, 'res.partner.category', 'search_read', [], ['id','name'])
categories = {categ['name']: categ['id'] for categ in categs}



input_file = csv.DictReader(open("files/rclcust1.csv"))

with open("Catg_ERROR.csv", "w") as f:
    vals={}
    for line in input_file:
        vals[line.get('CLASS-CODE').strip()] = 1

    for val in vals:
        try:
            if val not in categories:
                print (val)
                status = socket.execute(db, 2, pwd, 'res.partner.category', 'create', {'name': val})
                print (status)
        except:
            f.write(line.get('CLASS-CODE').strip())
            f.write('\n')

# {'category_id': [(6,0,[categories.get(line.get('CLASS-CODE'))])]}
