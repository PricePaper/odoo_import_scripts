#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import multiprocessing as mp
from xmlrpc import client as xmlrpclib

from scriptconfig import URL, DB, UID, PSW, WORKERS


# ==================================== P R O D U C T S ====================================

def update_product(pid, data_pool, product_ids, location_ids):
    sock = xmlrpclib.ServerProxy(URL, allow_none=True)
    while data_pool:
        try:
            data = data_pool.pop()

            default_code = data.get('ITEM-CODE').strip()
            location = data.get('BIN-CODE').strip()
            new_quantity = float(data.get('ON-HAND-QTY'))
            if product_ids.get(default_code) and location_ids.get(location) and new_quantity>0:

                vals = {'product_id': product_ids.get(default_code),
                        'location_id': location_ids.get(location),
                        'new_quantity': round(new_quantity,2)
                        }

                id = sock.execute(DB, UID, PSW, 'stock.change.product.qty', 'create', vals)
                print(pid, 'create - line', id)
                res = sock.execute(DB, UID, PSW, 'stock.change.product.qty', 'change_product_qty', id, vals)
                print(pid, 'Upadted - QTY', res)

        except Exception as e:
            print(e)
            break


def sync_products():
    manager = mp.Manager()
    data_pool = manager.list()
    product_ids = manager.dict()
    location_ids = manager.dict()

    process_Q = []

    fp1 = open('files/ivlioh.csv', 'r')
    # fp1 = open('ivlioh.csv', 'r')
    csv_reader1 = csv.DictReader(fp1)
    sock = xmlrpclib.ServerProxy(URL, allow_none=True)

    all_locations = sock.execute(DB, UID, PSW, 'stock.location', 'search_read', [('usage', '=', 'internal')], ['id','name'])
    location_ids = {ele['name']:ele['id'] for ele in all_locations}

    default_codes = []
    for vals in csv_reader1:
        data_pool.append(vals)
    fp1.close()

    domain = ['|', ('active', '=', False), ('active', '=', True)]
    res = sock.execute(DB, UID, PSW, 'product.product', 'search_read', domain, ['default_code'])
    product_ids = {rec['default_code']: rec['id'] for rec in res}

    res = None
    for i in range(WORKERS):
        pid = "Worker-%d" % (i + 1)
        worker = mp.Process(name=pid, target=update_product,
                            args=(pid, data_pool, product_ids, location_ids))
        process_Q.append(worker)
        worker.start()

    for worker in process_Q:
        worker.join()


if __name__ == "__main__":
    # PRODUCTS
    sync_products()
