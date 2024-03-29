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

accounts = socket.execute(db, 2, pwd, 'account.account', 'search_read', [], ['id','code'])
accounts = {account['code']: account['id'] for account in accounts}

stock_valuation_account = accounts.get('13000', False)


input_file = csv.DictReader(open(input_file))

with open("ERROR_mj.csv", "w") as f, open("parent_missing_mj.csv", "w") as f1:
    for line in input_file:
        if line.get('CATEGORY') in categories:
            try:
                vals={'categ_code': line.get('MAJOR-CLASS', ''),
                      'name': line.get('MAJOR-DESC').title(),
                      'parent_id': categories.get(line.get('CATEGORY')),
                      'property_cost_method': 'fifo',
                      'property_valuation':'real_time',
                      }
                if line.get('MAJOR-CLASS') not in categories:
                    status = socket.execute(db, 2, pwd, 'product.category', 'create', vals)
                    print (status)
                else:
                    category_id = categories.get(line.get('MAJOR-CLASS'))
                    status = socket.execute(db, 2, pwd, 'product.category', 'write', category_id, vals)
                    print (status)
            except:
                print ('Exception')
                f.write(line.get('MAJOR-CLASS'))
                f.write('\n')
        else:
            print(line)
            f1.write(line.get('MAJOR-CLASS'))
            f1.write('\n')
