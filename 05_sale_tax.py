#!/usr/bin/env python3
import xmlrpc.client
import ssl
import csv

from scriptconfig import url, db, pwd

socket = xmlrpc.client.ServerProxy(url,context=ssl._create_unverified_context())


taxes = socket.execute(db, 2, pwd, 'account.tax', 'search_read', [('name', '!=', 'Sale Tax' )], ['id','amount'])
tax1 = {float(tax['amount']): tax['id'] for tax in taxes}
input_file = csv.DictReader(open("files/omltxau1.csv"))

file_taxes = []

with open("ERROR.csv", "wb") as f:
    for line in input_file:
        tax = float(line.get('TAX-AUTH-PCT'))
        if tax not in file_taxes:
            file_taxes.append(tax)
for tax in file_taxes:
    try:
        vals = {
                'name': 'Tax '+str(tax)+'%',
                'amount_type':'percent',
                'type_tax_use': 'sale',
                'amount': tax,
                'tax_group_id': 1,
                'description':str(tax)+'%',
                # 'account_id': ,
                # 'refund_account_id':
               }
        if tax not in tax1:
            status = socket.execute(db, 2, pwd, 'account.tax', 'create', vals)
            print (status)

    except Exception as e:
        print (e)
        continue
