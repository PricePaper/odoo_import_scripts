#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# -*- coding: utf-8 -*-

import csv
import xmlrpc.client as xmlrpclib
import multiprocessing as mp

from scriptconfig import URL, DB, UID, PSW, WORKERS

# ==================================== SALE ORDER LINE ====================================

def update_sale_order_line(pid, data_pool, error_ids, product_ids, uom_ids, tax_code_ids, tax_ids, order_tax_code_ids):
    sock = xmlrpclib.ServerProxy(URL, allow_none=True)
    while data_pool:
        try:
            data = data_pool.pop()
            order_id = data.get('order_id')
            order_lines = sock.execute(DB, UID, PSW, 'sale.order.line', 'search_read', [('order_id','=',order_id)], ['product_id', 'product_uom'])
            order_line_ids = {rec['product_id'][0]: rec['product_uom'][0]  for rec in order_lines}


            for line in data.get('lines', []):
                product_id = product_ids.get(line.get('ITEM-CODE', '').strip())
                code = str(line.get('ORDERING-UOM')).strip() + '_' + str(line.get('QTY-IN-ORDERING-UM')).strip()
                code = uom_ids.get(code)

                if not product_id and not code:
                    error_ids.append()
                    continue
                if product_id in order_line_ids and code == order_line_ids[product_id]:
                    continue

                vals = {
                        'order_id': order_id,
                        'product_id': product_id,
                        'name': line.get('ITEM-DESC').strip(),
                        'price_unit': line.get('PRICE-DISCOUNTED').strip(),
                        'product_uom_qty': line.get('QTY-ORDERED').strip(),
                        'is_last': False,
                        'working_cost':line.get('TRUE-FIXED-COST').strip(),
                        'lst_price':line.get('PRICE-DISCOUNTED').strip(),
                        'product_uom': code,
                        }

                tax = ''
                if line.get('TAX-CODE') == '1':
                    tax = tax_ids.get(float(tax_code_ids.get(order_tax_code_ids.get(line.get('ORDER-NO')))))
                    vals['tax_id'] = [(6, 0, [tax])]

                res = sock.execute(DB, UID, PSW, 'sale.order.line', 'create', vals)
                print(pid, 'Create - SALE ORDER LINE', order_id , res)

        except Exception as e:
            print(e)


def sync_sale_order_lines():
    manager = mp.Manager()
    data_pool = manager.list()
    error_ids = manager.list()
    process_Q = []

    sock = xmlrpclib.ServerProxy(URL, allow_none=True)
    res = sock.execute(DB, UID, PSW, 'sale.order', 'search_read', [], ['name'])
    order_ids = {rec['name'] : rec['id']  for rec in res}

    fp = open('files/omlordr2.csv', 'r')
    csv_reader = csv.DictReader(fp)

    order_lines = {}
    for vals in csv_reader:
        ord_no = vals.get('ORDER-NO', '').strip()
        order_id = order_ids.get(ord_no)
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

    taxes = sock.execute(DB, UID, PSW, 'account.tax', 'search_read', [('name', '!=', 'Sale Tax' )], ['id','amount'])
    tax1 = {float(tax['amount']): tax['id'] for tax in taxes}
    tax_ids = manager.dict(tax1)

    fp1 = open('files/fiscal.csv', 'r')
    csv_reader1 = csv.DictReader(fp1)
    tax_codes={}
    for line in csv_reader1:
        tax_codes[line.get('TAX-AUTH-CODE').strip()] = line.get('TAX-AUTH-PCT')
    tax_code_ids = manager.dict(tax_codes)

    fp2 = open('files/omlordr1.csv', 'r')
    csv_reader2 = csv.DictReader(fp2)
    oder_tax_codes={}
    for line in csv_reader2:
        oder_tax_codes[line.get('ORDER-NO').strip()] = line.get('TAX-AUTH-CODE')
    order_tax_code_ids = manager.dict(oder_tax_codes)

    res = None
    order_ids = None
    taxes = None
    tax1 = None
    order_lines = None
    products = None

    for i in range(WORKERS):
        pid = "Worker-%d" % (i + 1)
        worker = mp.Process(name=pid, target=update_sale_order_line, args=(pid, data_pool, error_ids, product_ids, uom_ids, tax_code_ids, tax_ids, order_tax_code_ids))
        process_Q.append(worker)
        worker.start()

    for worker in process_Q:
        worker.join()



if __name__ == "__main__":

    # SALE ORDER LINE
    sync_sale_order_lines()
