#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
from xmlrpc import client as xmlrpclib
import multiprocessing as mp

from scriptconfig import URL, DB, UID, PSW, WORKERS

# =================================== C U S T O M E R ========================================

def update_customer(pid, data_pool, write_ids, term_ids):
    sock = xmlrpclib.ServerProxy(URL, allow_none=True)
    while data_pool:
        try:
            customer_code = ''
            data = data_pool.pop()
            customer_code = data.get('VEND-CODE')
            vals = {
                'name': data.get('VEND-NAME', '').title(),
                'street': data.get('VEND-ADDR1', '').title(),
                'street2': data.get('VEND-ADDR2', '').title(),
                'city': data.get('VEND-CITY', '').title(),
                'active': True,
                'customer': False,
                'supplier': True,
                'property_supplier_payment_term_id': term_ids.get(data.get('TERM-CODE')),
                'zip': data.get('VEND-ZIP-CODE'),
                'phone': data.get('VEND-PHONE'),
                'vat': data.get('VEND-TAX-ID'),
                'customer_code': data.get('VEND-CODE'),
            }

            res = write_ids.get(customer_code, [])
            if res:
                sock.execute(DB, UID, PSW, 'res.partner', 'write', res, vals)
                print(pid, 'UPDATE - VENDOR', res)
            else:
                res = sock.execute(DB, UID, PSW, 'res.partner', 'create', vals)
                print(pid, 'CREATE - VENDOR', res)
        except:
            error_ids.apppend(customer_code)


def sync_customers():
    manager = mp.Manager()
    data_pool = manager.list()
    error_ids = manager.list()
    write_ids = manager.dict()
    categ_ids = manager.dict()
    term_ids = manager.dict()
    fiscal_ids = manager.dict()
    carrier_ids = manager.dict()
    process_Q = []

    fp = open('files/aplvend1.csv', 'r')
    csv_reader = csv.DictReader(fp)

    customer_codes = []
    for vals in csv_reader:
        data_pool.append(vals)
        customer_code = vals['VEND-CODE']
        customer_codes.append(customer_code)

    fp.close()

    domain = [('customer_code', 'in', customer_codes)]
    sock = xmlrpclib.ServerProxy(URL, allow_none=True)

    res = sock.execute(DB, UID, PSW, 'res.partner', 'search_read', domain, ['customer_code'])
    write_ids = {rec['customer_code']: rec['id']  for rec in res}

    terms = sock.execute(DB, UID, PSW, 'account.payment.term', 'search_read', [('order_type', '=', 'purchase')], ['id','code'])
    term_ids = {rec['code']: rec['id']  for rec in terms}

    res = None
    customer_codes = None

    for i in range(WORKERS):
        pid = "Worker-%d" % (i + 1)
        worker = mp.Process(name=pid, target=update_customer, args=(pid, data_pool, write_ids, term_ids))
        process_Q.append(worker)
        worker.start()

    for worker in process_Q:
        worker.join()


if __name__ == "__main__":

    # PARTNER
    sync_customers()
