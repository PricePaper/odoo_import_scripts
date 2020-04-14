#!/usr/bin/env python3
import xmlrpc.client
import ssl
import csv

from scriptconfig import url, db, pwd
#url = "http://localhost:8069/xmlrpc/object"
#db = 'pricepaper'
#pwd = 'confianzpricepaper'


socket = xmlrpc.client.ServerProxy(url,context=ssl._create_unverified_context())


taxes = socket.execute(db, 2, pwd, 'account.tax', 'search_read', [('name', '!=', 'Sales Tax' )], ['id','amount'])
tax1 = {float(tax['amount']): tax['id'] for tax in taxes}
input_file = csv.DictReader(open("fiscal.csv"))

with open("ERROR.csv", "wb") as f, open("Missing.csv", "wb") as f1:
    count=0
    for line in input_file:
        try:
            if float(line.get('TAX-AUTH-PCT').strip()) in tax1:
                vals = {
                        'name':line['TAX-AUTH-DESC'].strip(),
                        'active':True,
                        'code':line['TAX-AUTH-CODE'].strip(),
                        'tax_ids': [(0, 0, {'tax_src_id':3,
                                            'tax_dest_id':tax1.get(float(line.get('TAX-AUTH-PCT')))})]
                       }
                status = socket.execute(db, 2, pwd, 'account.fiscal.position', 'create', vals)
                print (status)
            else:
                f1.write(line.get('TAX-AUTH-DESC').strip())
                f1.write('\n')
        except:
            f.write(line.get('TAX-AUTH-DESC').strip())
            f.write('\n')
