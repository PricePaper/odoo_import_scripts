#!/usr/bin/env python3
import xmlrpc.client
import ssl
import csv

from scriptconfig import url, db, pwd

socket = xmlrpc.client.ServerProxy(url,context=ssl._create_unverified_context())


taxes = socket.execute(db, 2, pwd, 'account.tax', 'search_read', [], ['id','name'])
tax1 = {tax['name']: tax['id'] for tax in taxes}

fiscal_position = socket.execute(db, 2, pwd, 'account.fiscal.position', 'search_read', [], ['id','name'])
fiscal_positions = {pos['name']: pos['id'] for pos in fiscal_position}

input_file = csv.DictReader(open("files/omltxau1.csv"))


with open("ERROR.csv", "wb") as f:
    for line in input_file:
        name = line.get('TAX-AUTH-DESC').strip()
        tax = float(line.get('TAX-AUTH-PCT'))
        try:
            vals = {
                    'name': name,
                    'amount_type':'percent',
                    'type_tax_use': 'sale',
                    'amount': tax,
                    'code': line.get('TAX-AUTH-CODE').strip(),
                    'tax_group_id': 1,
                    'description':str(tax)+'%',
                   }
            if name not in tax1:
                tax_rec = socket.execute(db, 2, pwd, 'account.tax', 'create', vals)
                print (name, 'Tax Created. ID:', tax_rec)
            else:
                tax_rec = tax1.get(name)
                staus = socket.execute(db, 2, pwd, 'account.tax', 'write', tax_rec, vals)
                print (name, 'Tax Updated. ID:', tax_rec)
            vals = {
                    'name':name,
                    'active':True,
                    'code':line['TAX-AUTH-CODE'].strip(),
                    'tax_ids':[(0, 0, {'tax_src_id':3,
                                       'tax_dest_id':tax_rec})]
                    }
            if name in fiscal_positions:
                fpos = fiscal_positions.get(name)
                status = socket.execute(db, 2, pwd, 'account.fiscal.position', 'write', fpos, {'tax_ids':[(5,)],})
                status = socket.execute(db, 2, pwd, 'account.fiscal.position', 'write', fpos, vals)
                print (name, ' FPOS Update. ID:', fpos)
            else:
                status = socket.execute(db, 2, pwd, 'account.fiscal.position', 'create', vals)
                print (name, 'FPOS Created. ID:', status)

        except Exception as e:
            print (e)
