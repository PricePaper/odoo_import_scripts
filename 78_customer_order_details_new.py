#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import logging.handlers
import math
import multiprocessing as mp
import os
import xmlrpc.client as xmlrpclib

import multiprocessing_logging

from scriptconfig import URL, DB, UID, PSW, WORKERS

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
# create file handler which logs even debug messages
filename = os.path.basename(__file__)
logfile = os.path.splitext(filename)[0] + '.log'
fh = logging.FileHandler(logfile, mode='w')
fh.setLevel(logging.DEBUG)
# create console handler with a higher log level
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
fh.setFormatter(formatter)
# add the handlers to logger
logger.addHandler(ch)
logger.addHandler(fh)
multiprocessing_logging.install_mp_handler(logger=logger)

# ==================================== SALE ORDER LINE ====================================

def update_sale_order_line(pid, data_pool, product_ids, uom_ids, order_tax_code_ids, no_tax_id):
    sock = xmlrpclib.ServerProxy(URL, allow_none=True)
    while data_pool:
        try:
            data = data_pool.pop()
            order_id = data.get('order_id')
            order_lines = sock.execute(DB, UID, PSW, 'sale.order.line', 'search_read', [('order_id','=',order_id)], ['product_id', 'product_uom'])
            order_line_ids = {rec['product_id'][0]: rec['product_uom'][0]  for rec in order_lines}


            for line in data.get('lines', []):
                product_id = product_ids.get(line.get('ITEM-CODE', ''))
                code1 = str(line.get('ORDERING-UOM')) + '_' + str(line.get('QTY-IN-ORDERING-UM'))
                code = uom_ids.get(code1)
                order_no = line.get('ORDER-NO', '')

                if not product_id:
                    logger.error('Product Missing - {0} {1}'.format(line.get('ITEM-CODE', ''), order_no, code1))
                    continue
                if not code:
                    logger.error('UOM Missing - {0} {1}'.format(order_no, line.get('ITEM-CODE', '')))
                    continue
                if product_id in order_line_ids and code == order_line_ids[product_id]:
                    continue

                uom_factor = float(line.get('ITEM-QTY-IN-STOCK-UM')) / float(line.get('QTY-IN-ORDERING-UM'))
                quantity_ordered = float(line.get('QTY-ORDERED')) * uom_factor

                if uom_factor > 1 or math.isclose(1.0, uom_factor):
                    quantity_ordered = round(quantity_ordered, 0)
                else:
                    quantity_ordered = round(quantity_ordered, 3)

                vals = {
                    'order_id': order_id,
                    'product_id': product_id,
                    'name': line.get('ITEM-DESC'),
                    'price_unit': line.get('PRICE-DISCOUNTED'),
                    'product_uom_qty': quantity_ordered,
                    'is_last': False,
                    'working_cost': line.get('BURDEN-COST'),
                    'lst_price': line.get('PRICE-DISCOUNTED'),
                    'product_uom': code,
                    'tax_id': False
                }

                tax = ''
                if line.get('TAX-CODE') == '0':
                    if line.get('TAX-AMT') != '0':
                        tax = order_tax_code_ids.get(line.get('ORDER-NO', ''))
                        if not tax or not tax[1]:
                            logger.error('Error Tax missing: Invoice:{0} Item:{1}'.format(line.get('ORDER-NO', ''),line.get('ITEM-CODE', '')))
                            continue
                    else:
                        tax=[False, no_tax_id]

                    vals['tax_id'] = [(6, 0, [tax[1]])]

                res = sock.execute(DB, UID, PSW, 'sale.order.line', 'create', vals, {'context':{'from_import': True}})
                print(pid, 'Create - SALE ORDER LINE', order_id , res)

        except Exception as e:
            logger.error('Exception {}'.format(e))
            # data_pool.append(data)


def sync_sale_order_lines():
    manager = mp.Manager()
    data_pool = manager.list()
    process_Q = []

    sock = xmlrpclib.ServerProxy(URL, allow_none=True)
    res = sock.execute(DB, UID, PSW, 'sale.order', 'search_read', [], ['name'])
    order_ids = {rec['name'] : rec['id']  for rec in res}


    fp2 = open('files/omlcinv1.csv', 'r')
    csv_reader2 = csv.DictReader(fp2)

    order_dict = {}

    for vals in csv_reader2:
        ord_no = vals.get('ORDER-NO', '')
        inv_no = vals.get('INVOICE-NO', '')
        order_dict[inv_no] = ord_no

    fp1 = open('files/omlcinv2.csv', 'r')
    csv_reader1 = csv.DictReader(fp1)

    order_lines = {}
    order_inv = []

    for vals in csv_reader1:
        inv_no = vals.get('INVOICE-NO', '')
        ord_no = order_dict.get(inv_no)
        order_inv.append(ord_no)
        order_id = order_ids.get(ord_no)
        if order_id:
            vals['ORDER-NO'] = ord_no
            lines = order_lines.setdefault(order_id, [])
            lines.append(vals)

    fp1.close()

    fp = open('files/omlordr2.csv', 'r')
    csv_reader = csv.DictReader(fp)

    for vals in csv_reader:
        ord_no = vals.get('ORDER-NO', '')
        order_id = order_ids.get(ord_no)
        if order_id and ord_no not in order_inv:
            lines = order_lines.setdefault(order_id, [])
            lines.append(vals)

    fp.close()



    data_pool = manager.list([{'order_id': order, 'lines': order_lines[order]} for order in order_lines])


    res = sock.execute(DB, UID, PSW, 'product.product', 'search_read', ['|', ('active', '=', False), ('active', '=', True)], ['default_code'])
    products = {rec['default_code']: rec['id']  for rec in res}
    product_ids = manager.dict(products)

    uoms = sock.execute(DB, UID, PSW, 'uom.uom', 'search_read', [], ['id','name'])
    uom_ids = {uom['name']:uom['id'] for uom in uoms}

    taxes = sock.execute(DB, UID, PSW, 'account.tax', 'search_read', [], ['id','code'])
    tax1 = {tax['code']: tax['id'] for tax in taxes}
    tax_ids = manager.dict(tax1)

    no_tax_id = tax_ids.get('*1')
    print(no_tax_id)

    fiscal_position = sock.execute(DB, UID, PSW,  'account.fiscal.position', 'search_read', [], ['id','code'])
    fiscal_positions = {pos['id']: pos['code'] for pos in fiscal_position}

    domain = ['|',('active', '=', False), ('active', '=', True)]
    res = sock.execute(DB, UID, PSW, 'res.partner', 'search_read', domain, ['customer_code', 'property_account_position_id'])
    partner_tax_ids = {rec['customer_code']: rec['property_account_position_id'][0] for rec in res if rec['property_account_position_id']}

    fp2 = open('files/omlordr1.csv', 'r')
    csv_reader2 = csv.DictReader(fp2)
    oder_tax_codes={}
    for line in csv_reader2:
        ship_to_code = line.get('SHIP-TO-CODE', False)
        if  ship_to_code and ship_to_code != 'SAME':
            ship_code = line.get('CUSTOMER-CODE', False) and line.get('CUSTOMER-CODE', False)+'-'+line.get('SHIP-TO-CODE', False)
            fpos_code = partner_tax_ids.get(ship_code, False)
            tax_id = tax_ids.get(fiscal_positions.get(fpos_code, False))
            oder_tax_codes[line.get('ORDER-NO', '')] = [ship_code, tax_id]
        else:
            oder_tax_codes[line.get('ORDER-NO', '')] = [line.get('CUSTOMER-CODE', False), tax_ids.get(line.get('TAX-AUTH-CODE', False), False)]

    fp3 = open('files/omlcinv1.csv', 'r')
    csv_reader3 = csv.DictReader(fp3)
    for line in csv_reader3:
        inv_no = line.get('INVOICE-NO', '')
        ord_no = order_dict.get(inv_no)
        ship_to_code = line.get('SHIP-TO-CODE', False)
        if  ship_to_code and ship_to_code != 'SAME':
            ship_code = line.get('CUSTOMER-CODE', False) and line.get('CUSTOMER-CODE', False)+'-'+line.get('SHIP-TO-CODE', False)
            fpos_code = partner_tax_ids.get(ship_code, False)
            tax_id = tax_ids.get(fiscal_positions.get(fpos_code, False))
            oder_tax_codes[ord_no] = [ship_code, tax_id]
        else:
            oder_tax_codes[ord_no] = [line.get('CUSTOMER-CODE', False), tax_ids.get(line.get('TAX-AUTH-CODE', False), False)]

    order_tax_code_ids = manager.dict(oder_tax_codes)


    res = None
    order_ids = None
    taxes = None
    tax1 = None
    order_lines = None
    products = None

    for i in range(WORKERS):
        pid = "Worker-%d" % (i + 1)
        worker = mp.Process(name=pid, target=update_sale_order_line,
            args=(pid, data_pool, product_ids, uom_ids, order_tax_code_ids, no_tax_id))
        process_Q.append(worker)
        worker.start()

    for worker in process_Q:
        worker.join()



if __name__ == "__main__":

    # SALE ORDER LINE
    sync_sale_order_lines()
