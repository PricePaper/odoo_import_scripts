#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import xmlrpc.client
import ssl
import csv

from scriptconfig import url, db, pwd

input_file = 'files/ivincl.csv'


socket = xmlrpc.client.ServerProxy(url,context=ssl._create_unverified_context())

categories = socket.execute(db, 2, pwd, 'product.category', 'search_read', [], ['id','categ_code'])
categories = {category['categ_code']: category['id'] for category in categories}


input_file = csv.DictReader(open(input_file))

with open("ERROR_cl.csv", "w") as f, open("parent_missing_cl.csv", "w") as f1:
    for line in input_file:
        if line.get('MAJOR-CLASS') in categories:
            try:
                vals={'categ_code': line.get('PROD-CODE'),
                      'name': line.get('CLASS-DESCRIPTION').title(),
                      'parent_id': categories.get(line.get('MAJOR-CLASS')),
                      'standard_price': line.get('LIST-PRICE-PCT'),
                      'repacking_upcharge': line.get('UPCHARGE'),
                      'class_margin': line.get('MIN-GTM')
                      }
                if line.get('PROD-CODE') not in categories:
                    status = socket.execute(db, 2, pwd, 'product.category', 'create', vals)
                    print (status)
                else:
                    category_id = categories.get(line.get('PROD-CODE'))
                    status = socket.execute(db, 2, pwd, 'product.category', 'write', category_id, vals)
                    print (status)
            except:
                print ('Exception')
                f.write(line.get('PROD-CODE', ''))
                f.write('\n')
        else:
            print(line)
            f1.write(line.get('PROD-COD'))
            f1.write('\n')
