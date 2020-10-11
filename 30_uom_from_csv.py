#!/usr/bin/env python3
import xmlrpc.client
import ssl
import csv

from scriptconfig import url, db, pwd

socket = xmlrpc.client.ServerProxy(url,context=ssl._create_unverified_context())



input_file = csv.DictReader(open("files/ivlitum1.csv"))
input_file1 = csv.DictReader(open("files/iclitem1.csv"))

uoms = socket.execute(db, 2, pwd, 'uom.uom', 'search_read', [], ['id','name'])
uoms = {uom['name']: uom['id'] for uom in uoms}

with open("ERROR.csv", "w") as f:
    for line in input_file:
        code = str(line.get('UOM')) + '_' + str(line.get('QTY'))
        code = code
        factor = line.get('QTY')
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
                print (vals, "   :", status)
            except:
                f.write(code)
                f.write('\n')
    for line in input_file1:
        code = str(line.get('ITEM-STOCK-UOM')) + '_' + str(line.get('ITEM-QTY-IN-STOCK-UM'))
        code = code
        factor = line.get('ITEM-QTY-IN-STOCK-UM')
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
                print (vals, "   :", status)
            except:
                f.write(code)
                f.write('\n')
