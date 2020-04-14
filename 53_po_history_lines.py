#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
from xmlrpc import client as xmlrpclib
import multiprocessing as mp

from scriptconfig import URL, DB, UID, PSW, WORKERS

# ==================================== PURCHASE ORDER LINE ====================================

def update_purchase_order_line(pid, data_pool, error_ids, product_ids, uom_ids):
    sock = xmlrpclib.ServerProxy(URL, allow_none=True)
    while data_pool:
        # try:
            order_line_ids = ''
            data = data_pool.pop()
            order_id = data.get('order_id')
            order_lines = sock.execute(DB, UID, PSW, 'purchase.order.line', 'search_read', [('order_id','=',order_id)], ['product_id'])
            order_line_ids = [rec['product_id'][0] for rec in order_lines]


            for line in data.get('lines', []):
                product_id = product_ids.get(line.get('ITEM-CODE', '').strip())
                code1 = str(line.get('ORDR-UOM')).strip() + '_' + str(line.get('ORDR-VAL-QTY')).strip()
                code = uom_ids.get(code1)
                if not product_id:
                    error_ids.append((line.get('ORDR-NUM', '').strip(), vals.get('ITEM-CODE', '').strip()))
                    continue
                if not code:
                    error_ids.append((line.get('ORDR-NUM', '').strip(), code))
                    continue
                if product_id in order_line_ids:
                    print('Duplicate')
                    continue
                vals={'product_id': product_id,
                      'product_uom': code,
                      'price_unit': line.get('ORDR-UNIT-COST').strip(),
                      'product_qty': line.get('ORDR-QTY').strip(),
                      'name': line.get('ITEM-DESC', ' ').strip()+line.get('ITEM-DESCR2         ', ' ').strip(),
                      'order_id': order_id,
                      'date_planned': line.get('ORDR-LINE-REQD-DATE ', '').strip()
                      }
                # print (vals)

                res = sock.execute(DB, UID, PSW, 'purchase.order.line', 'create', vals)
                print(pid, 'Create - PURCHASE ORDER LINE', order_id)

        # except:
        #     break


def sync_purchase_order_lines():
    manager = mp.Manager()
    data_pool = manager.list()
    error_ids = manager.list()
    process_Q = []

    sock = xmlrpclib.ServerProxy(URL, allow_none=True)

    res = sock.execute(DB, UID, PSW, 'purchase.order', 'search_read', [], ['id','name'])
    order_ids = {order['name']: order['id'] for order in res}

    fp = open('polhist2.csv', 'rb')
    csv_reader = csv.DictReader(fp)

    order_lines = {}
    for vals in csv_reader:
        order_no = vals.get('ORDR-NUM', '').strip()
        order_id = order_ids.get(order_no)
        if order_id:
            lines = order_lines.setdefault(order_id, [])
            lines.append(vals)

    fp.close()

    data_pool = manager.list([{'order_id': order, 'lines': order_lines[order]} for order in order_lines])

    res = sock.execute(DB, UID, PSW, 'product.product', 'search_read', ['|', ('active', '=', False), ('active', '=', True)], ['default_code'])
    products = {rec['default_code']: rec['id']  for rec in res}
    product_ids = manager.dict(products)

    uoms = sock.execute(DB, UID, PSW, 'uom.uom', 'search_read', [], ['id','name'])
    uom_ids = {uom['name']:uom['id'] for uom in uoms}

    res = None
    order_ids = None
    order_lines = None
    products = None

    for i in range(WORKERS):
        pid = "Worker-%d" % (i + 1)
        worker = mp.Process(name=pid, target=update_purchase_order_line, args=(pid, data_pool, error_ids, product_ids, uom_ids))
        process_Q.append(worker)
        worker.start()

    for worker in process_Q:
        worker.join()



if __name__ == "__main__":


    sync_purchase_order_lines()
