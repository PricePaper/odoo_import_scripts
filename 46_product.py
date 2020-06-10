#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import multiprocessing as mp
from xmlrpc import client as xmlrpclib

from scriptconfig import URL, DB, UID, PSW, WORKERS


# ==================================== P R O D U C T S ====================================

def update_product(pid, data_pool, create_ids, write_ids, uom_ids, category_ids):
    sock = xmlrpclib.ServerProxy(URL, allow_none=True)
    while data_pool:
        try:
            data = data_pool.pop()

            default_code = data.get('ITEM-CODE').strip()
            code = str(data.get('ITEM-STOCK-UOM')).strip() + '_' + str(data.get('ITEM-QTY-IN-STOCK-UM')).strip()
            active = True
            purchase_ok = True
            #  Comment out because inactive items break order imports
            # # # # if data.get('ITEM-STATUS').strip() and data.get('ITEM-STATUS').strip() == 'D':
            # # #     purchase_ok = False,
            # #     if float(data.get('ITEM-QTY-ON-HAND')) <= 0.0:
            #         active = False

            vals = {'name': data.get('ITEM-DESC').strip().title(),
                    'description_sale': data.get('ITEM-DESC').strip().lower(),
                    'description_purchase': data.get('ITEM-DESCR2').strip().lower(),
                    'default_code': default_code,
                    'categ_id': category_ids.get(data.get('PROD-CODE').strip()),
                    'active': active,
                    'type': 'product',
                    # 'burden_percent': data.get('ITEM-BURDEN-PERCENT').strip(),
                    # 'standard_price':data.get('ITEM-UNIT-COST'),
                    'sale_ok': True,
                    'lst_price': data.get('ITEM-AVG-SELL-PRC').strip(),
                    'purchase_ok': purchase_ok,
                    'sale_uoms': [(6, 0, [uom_ids.get(code)])],
                    'uom_id': uom_ids.get(code),
                    'uom_po_id': uom_ids.get(code),
                    'lst_price': data.get('ITEM-AVG-SELL-PRC').strip(),
                    }

            res = write_ids.get(default_code, [])
            if res:
                sock.execute(DB, UID, PSW, 'product.product', 'write', res, vals)
                print(pid, 'UPDATE - PRODUCT', res)
            else:
                res = sock.execute(DB, UID, PSW, 'product.product', 'create', vals)
                print(pid, 'CREATE - PRODUCT', res)

        except Exception as e:
            print(e)
            break


def sync_products():
    manager = mp.Manager()
    data_pool = manager.list()
    create_ids = manager.dict()
    write_ids = manager.dict()
    uom_ids = manager.dict()
    category_ids = manager.dict()

    process_Q = []

    fp = open('files/iclitem1.csv', 'r')
    csv_reader = csv.DictReader(fp)

    default_codes = []
    for vals in csv_reader:
        data_pool.append(vals)
        default_code = vals['ITEM-CODE'].strip()
        default_codes.append(default_code)

    fp.close()

    domain = [('default_code', 'in', default_codes), '|', ('active', '=', False), ('active', '=', True)]
    sock = xmlrpclib.ServerProxy(URL, allow_none=True)
    res = sock.execute(DB, UID, PSW, 'product.product', 'search_read', domain, ['default_code'])
    write_ids = {rec['default_code']: rec['id'] for rec in res}

    uoms = sock.execute(DB, UID, PSW, 'uom.uom', 'search_read', [], ['id', 'name'])
    uom_ids = {uom['name']: uom['id'] for uom in uoms}

    categories = sock.execute(DB, UID, PSW, 'product.category', 'search_read', [], ['id', 'categ_code'])
    category_ids = {category['categ_code']: category['id'] for category in categories}

    res = None
    default_codes = None
    uoms = None
    categories = None

    for i in range(WORKERS):
        pid = "Worker-%d" % (i + 1)
        worker = mp.Process(name=pid, target=update_product,
                            args=(pid, data_pool, create_ids, write_ids, uom_ids, category_ids))
        process_Q.append(worker)
        worker.start()

    for worker in process_Q:
        worker.join()


if __name__ == "__main__":
    # PRODUCTS
    sync_products()
