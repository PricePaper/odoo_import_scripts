#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import xmlrpc.client
import ssl
import csv

from scriptconfig import url, db, pwd

socket = xmlrpc.client.ServerProxy(url,context=ssl._create_unverified_context())

categories = socket.execute(db, 2, pwd, 'product.category', 'search_read', [], ['id','categ_code'])
categories = {category['categ_code']: category['id'] for category in categories}

accounts = socket.execute(db, 2, pwd, 'account.account', 'search_read', [], ['id','code'])
accounts = {account['code']: account['id'] for account in accounts}

stock_valuation_account = accounts.get('13000', False)

input_file = csv.DictReader(open('files/ivinct.csv'))

with open("ERROR_ct.csv", "w") as f:
    for line in input_file:
        try:
            vals={'categ_code': line.get('CATEGORY').strip(),
                  'name': line.get('CATEGORY-DESC').strip().title(),
                  'property_cost_method': 'fifo',
                  'property_valuation':'real_time',
                  }
            if stock_valuation_account:
                vals['property_stock_valuation_account_id'] = stock_valuation_account
            if line.get('CATEGORY').strip() not in categories:
                status = socket.execute(db, 2, pwd, 'product.category', 'create', vals)
                print (status)
            else:
                category_id = categories.get(line.get('CATEGORY').strip())
                status = socket.execute(db, 2, pwd, 'product.category', 'write', category_id, vals)
                print (status)
        except:
            print ('Exception')
            f.write(line.get('CATEGORY').strip())
            f.write('\n')
