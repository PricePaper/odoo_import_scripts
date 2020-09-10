#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import xmlrpc.client as xmlrpclib
from scriptconfig import URL, DB, UID, PSW, WORKERS

def sync_invoices():

    sock = xmlrpclib.ServerProxy(URL, allow_none=True)
    res = sock.execute(DB, UID, PSW, 'sale.order', 'search_read', [('state', 'in' , ('draft', 'sent'))], ['name'])
    order_ids = {rec['name']: rec['id'] for rec in res}


    orders = []
    with open('files/omlcinv1.csv', newline='') as f:
        csv_reader = csv.DictReader(f)
        for vals in csv_reader:
            try:
                name = vals.get('1ST-NAME', '').strip()
                if name == 'VOID':
                    continue
                else:
                    order_no = vals.get('ORDER-NO', '').strip()
                    order_id = order_ids.get(order_no)
                    if order_id:
                        res = sock.execute(DB, UID, PSW, 'sale.order', 'action_confirm', order_id,)
                        print('Confirm - order', order_id, order_no)
                        inv_id = sock.execute(DB, UID, PSW, 'account.invoice', 'search_read', [('origin', '=', order_no)], ['id'])
                        inv_id = inv_id[0]['id']
                        sock.execute(DB, UID, PSW, 'account.invoice', 'write', inv_id, {'name': vals.get('INVOICE-NO', '').strip()})
            except Exception as e:
                print(e)


if __name__ == "__main__":
    # Invoice
    sync_invoices()
