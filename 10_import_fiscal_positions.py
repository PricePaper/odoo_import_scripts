#!/usr/bin/env python3
import xmlrpc.client
import ssl
import csv

from scriptconfig import url, db, pwd

socket = xmlrpc.client.ServerProxy(url,context=ssl._create_unverified_context())


taxes = socket.execute(db, 2, pwd, 'account.tax', 'search_read', [('name', '!=', 'Sales Tax' )], ['id','amount'])
tax1 = {float(tax['amount']): tax['id'] for tax in taxes}

fiscal_position = socket.execute(db, 2, pwd, 'account.fiscal.position', 'search_read', [], ['id','name'])
fiscal_positions = {pos['name']: pos['id'] for pos in fiscal_position}

input_file = csv.DictReader(open("files/omltxau1.csv"))

with open("ERROR.csv", "w") as f, open("Missing.csv", "w") as f1:
    count=0
    for line in input_file:
        try:
            tax = float(line.get('TAX-AUTH-PCT        '))
            if tax in tax1:
                name = line['TAX-AUTH-DESC'].strip()
                if name not in fiscal_positions:
                    vals = {
                            'name':line['TAX-AUTH-DESC'].strip(),
                            'active':True,
                            'code':line['TAX-AUTH-CODE'].strip(),
                            'tax_ids': [(0, 0, {'tax_src_id':3,
                                                'tax_dest_id':tax1.get(tax)})]
                           }
                    status = socket.execute(db, 2, pwd, 'account.fiscal.position', 'create', vals)
                    print (status)
            else:
                f1.write(line.get('TAX-AUTH-DESC').strip())
                f1.write('\n')
        except:
            f.write(line.get('TAX-AUTH-DESC').strip())
            f.write('\n')
