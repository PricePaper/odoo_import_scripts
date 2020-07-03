#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import xmlrpc.client
import ssl
import csv

from scriptconfig import url, db, pwd

socket = xmlrpc.client.ServerProxy(url,context=ssl._create_unverified_context())



categs = socket.execute(db, 2, pwd, 'res.partner.category', 'search_read', [], ['id','code'])
categories = {categ['code']: categ['id'] for categ in categs}



input_file = csv.DictReader(open("files/rclcust1.csv"))

with open("Catg_ERROR.csv", "w") as f:
    for line in input_file:
        try:
            code = line.get('CLASS-CODE').strip()
            if code not in categories:
                status = socket.execute(db, 2, pwd, 'res.partner.category', 'create', {'name': code,'code':code})
                categories[code] = status
                print (status)
        except:
            f.write(line.get('CLASS-CODE').strip())
            f.write('\n')

# {'category_id': [(6,0,[categories.get(line.get('CLASS-CODE'))])]}
