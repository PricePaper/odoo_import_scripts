#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import multiprocessing as mp
from xmlrpc import client as xmlrpclib

from scriptconfig import URL, DB, UID, PSW, WORKERS


# =================================== C U S T O M E R ========================================

def update_customer(pid, data_pool, write_ids, fiscal_ids, term_ids, carrier_ids, error_ids, delivery_notes):
    sock = xmlrpclib.ServerProxy(URL, allow_none=True)
    while data_pool:

        try:
            customer_code = ''
            parent=''
            data = data_pool.pop()
            customer_code = data.get('CUSTOMER-CODE', False)+'-'+data.get('SHIP-TO-CODE', False)
            parent = write_ids.get(data.get('CUSTOMER-CODE', False))
            city, state = data['SHIP-CITY-STATE'].strip().split(',')
            state = state.strip()
            vals = {
                'name': data['SHIP-1ST-NAME'].strip().title(),
                'type': 'delivery',
                'corp_name': data['SHIP-2ND-NAME'].strip().title(),
                'street': data['SHIP-STREET'].strip().title(),
                'city': city.title(),
                'active': True,
                'customer': True,
                'zip': data['SHIP-ZIP-CODE'],
                'phone': data['SHIP-PHONE'],
                'customer_code': customer_code,
                'parent_id': parent,
                'vat': data['SHIP-RESALE-NUMBER'].strip(),
                'property_account_position_id': fiscal_ids.get(data.get('TAX-AUTH-CODE').strip()),
                'property_delivery_carrier_id': carrier_ids.get(data.get('CARRIER-CODE')),
                'delivery_notes': delivery_notes.get(customer_code, '')
            }

            res = write_ids.get(customer_code, [])
            if res:
                sock.execute(DB, UID, PSW, 'res.partner', 'write', res, vals)
                print(pid, 'UPDATE - CUSTOMER', res, customer_code)
            else:
                res = sock.execute(DB, UID, PSW, 'res.partner', 'create', vals)
                print(pid, 'CREATE - CUSTOMER', res, customer_code)
        except Exception as e:
            print(customer_code)
            print(e)


def sync_customers():
    manager = mp.Manager()
    data_pool = manager.list()
    error_ids = manager.list()
    write_ids = manager.dict()
    term_ids = manager.dict()
    fiscal_ids = manager.dict()
    carrier_ids = manager.dict()
    delivery_notes = manager.dict()

    process_Q = []


    customer_codes = []
    with open('files/omlshpt1.csv', 'r') as fp:
        csv_reader = csv.DictReader(fp)
        for vals in csv_reader:
            data_pool.append(vals)

    with open('files/omlcsin1.csv', 'r') as fp5:
        csv_reader5 = csv.DictReader(fp5)
        for vals in csv_reader5:
            customer_code = vals['CUSTOMER-CODE'].strip()
            if customer_code and vals['SHIP-TO-CODE'].strip():
                customer_code = customer_code + '-' + vals['SHIP-TO-CODE'].strip()
            note=''
            if vals['LINE-1']:
                note += vals['LINE-1']+'\n'
            if vals['LINE-2']:
                note += vals['LINE-2']+'\n'
            if vals['LINE-3']:
                note += vals['LINE-3']+'\n'
            if vals['LINE-4']:
                note += vals['LINE-4']+'\n'
            if vals['LINE-5']:
                note += vals['LINE-5']+'\n'
            if vals['LINE-6']:
                note += vals['LINE-6']
            delivery_notes[customer_code] = note

    domain = ['|',('active', '=', False), ('active', '=', True)]
    sock = xmlrpclib.ServerProxy(URL, allow_none=True)

    res = sock.execute(DB, UID, PSW, 'res.partner', 'search_read', domain, ['customer_code'])
    write_ids = {rec['customer_code']: rec['id'] for rec in res}

    fiscal = sock.execute(DB, UID, PSW, 'account.fiscal.position', 'search_read', [], ['id', 'code'])
    fiscal_ids = {rec['code']: rec['id'] for rec in fiscal}


    terms = sock.execute(DB, UID, PSW, 'account.payment.term', 'search_read', [('order_type', '=', 'sale')],
                         ['id', 'code'])
    term_ids = {rec['code']: rec['id'] for rec in terms}

    carriers = sock.execute(DB, UID, PSW, 'delivery.carrier', 'search_read', [], ['id', 'name'])
    carrier_ids = {rec['name']: rec['id'] for rec in carriers}

    res = None
    customer_codes = None

    for i in range(WORKERS):
        pid = "Worker-%d" % (i + 1)
        worker = mp.Process(name=pid, target=update_customer, args=(
            pid, data_pool, write_ids, fiscal_ids, term_ids,
            carrier_ids, error_ids, delivery_notes))
        process_Q.append(worker)
        worker.start()

    for worker in process_Q:
        worker.join()


if __name__ == "__main__":
    # PARTNER
    sync_customers()
