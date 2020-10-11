#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import xmlrpc.client
import ssl
import csv


from scriptconfig import url, db, pwd

socket = xmlrpc.client.ServerProxy(url,context=ssl._create_unverified_context())



input_file = csv.DictReader(open("files/omlhist2.csv"))

uoms = socket.execute(db, 2, pwd, 'uom.uom', 'search_read', [], ['id','name'])
uoms = {uom['name']: uom['id'] for uom in uoms}
count = 0

with open("ERROR.csv", "wb") as f:
    for line in input_file:
        code = str(line.get('ORDERING-UOM')) + '_' + str(line.get('QTY-IN-ORDERING-UM'))
        code = code
        factor = line.get('QTY-IN-ORDERING-UM')
        factor = float(factor)
        uom_type = 'bigger' if factor > 0 else 'smaller'
        if code not in uoms:
            try:
                vals={'name': code,
                      'category_id': 1,
                      'active': True,
                      'factor_inv': factor,
                      'uom_type': uom_type
                      }
                status = socket.execute(db, 2, pwd, 'uom.uom', 'create', vals)
                uoms.update({code:status})
                print (code, "   :", status)
            except:
                print ('.....', count)
                print (count, code)
                # f.write(code)
                # f.write('\n')
        count+=1

#305
