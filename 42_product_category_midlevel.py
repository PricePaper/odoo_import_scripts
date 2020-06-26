#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import xmlrpc.client
import ssl
import csv

from scriptconfig import url, db, pwd

input_file = 'files/ivinmj.csv'


socket = xmlrpc.client.ServerProxy(url,context=ssl._create_unverified_context())

categories = socket.execute(db, 2, pwd, 'product.category', 'search_read', [], ['id','categ_code'])
categories = {category['categ_code']: category['id'] for category in categories}


input_file = csv.DictReader(open(input_file))

with open("ERROR_mj.csv", "w") as f, open("parent_missing_mj.csv", "w") as f1:
    for line in input_file:
        if line.get('CATEGORY').strip() in categories:
            try:
                vals={'categ_code': line.get('MAJOR-CLASS', '').strip(),
                      'name': line.get('MAJOR-DESC').strip().title(),
                      'parent_id': categories.get(line.get('CATEGORY').strip()),
                      }
                if line.get('MAJOR-CLASS').strip() not in categories:
                    status = socket.execute(db, 2, pwd, 'product.category', 'create', vals)
                    print (status)
                else:
                    category_id = categories.get(line.get('MAJOR-CLASS').strip())
                    status = socket.execute(db, 2, pwd, 'product.category', 'write', category_id, vals)
                    print (status)
            except:
                print ('Exception')
                f.write(line.get('MAJOR-CLASS').strip())
                f.write('\n')
        else:
            print(line)
            f1.write(line.get('MAJOR-CLASS').strip())
            f1.write('\n')
