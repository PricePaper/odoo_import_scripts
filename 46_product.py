#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import multiprocessing as mp
from xmlrpc import client as xmlrpclib

from scriptconfig import URL, DB, UID, PSW, WORKERS

import logging.handlers
import os
import time
import multiprocessing_logging
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

# ==================================== P R O D U C T S ====================================

def update_product(pid, data_pool, create_ids, write_ids, uom_ids, category_ids, location_ids, sale_uoms):
    sock = xmlrpclib.ServerProxy(URL, allow_none=True)
    while data_pool:
        try:
            data = data_pool.pop()

            default_code = data.get('ITEM-CODE').strip()
            code = str(data.get('ITEM-STOCK-UOM')).strip() + '_' + str(data.get('ITEM-QTY-IN-STOCK-UM')).strip()
            active = True
            purchase_ok = True
            #  Comment out because inactive items break order imports
            if data.get('ITEM-STATUS').strip() and data.get('ITEM-STATUS').strip() == 'D':
                purchase_ok = False,
                if float(data.get('ITEM-QTY-ON-HAND')) <= 0.0:
                    active = False

            sale_uom_ids = []
            if default_code in sale_uoms:
                for uom in sale_uoms[default_code]:
                    sale_uom_id = uom_ids.get(uom)
                    if sale_uom_id:
                        sale_uom_ids.append(sale_uom_id)
                    else:
                        logger.debug('SALE UOM missing uom:{0} product:{1}'.format(uom,default_code))
            uom_id = uom_ids.get(code)
            if uom_id:
                sale_uom_ids.append(uom_id)
            else:
                logger.error('UOM missing uom:{0} product:{1}'.format(uom,default_code))
                continue


            vals = {'name': data.get('ITEM-DESC').strip().title(),
                    'description_sale': data.get('ITEM-DESC').strip().lower(),
                    'description_purchase': data.get('ITEM-DESCR2').strip().lower(),
                    'default_code': default_code,
                    'categ_id': category_ids.get(data.get('PROD-CODE').strip()),
                    'active': active,
                    'type': 'product',
                    'standard_price': data.get('ITEM-UNIT-COST').strip(),
                    'sale_ok': True,
                    'taxes_id':[(6, 0, [3])],
                    'lst_price': data.get('ITEM-AVG-SELL-PRC').strip(),
                    'purchase_ok': purchase_ok,
                    'sale_uoms': [(6, 0, sale_uom_ids)],
                    'uom_id': uom_ids.get(code),
                    'uom_po_id': uom_ids.get(code),
                    'volume': data.get('ITEM-CUBE').strip(),
                    'weight': data.get('ITEM-WEIGHT').strip(),
                    'property_stock_location': location_ids.get(default_code)
                    }


            res = write_ids.get(default_code, [])
            if res:
                sock.execute(DB, UID, PSW, 'product.product', 'write', res, vals)
                print(pid, 'UPDATE - PRODUCT', res)
            else:
                res = sock.execute(DB, UID, PSW, 'product.product', 'create', vals)
                print(pid, 'CREATE - PRODUCT', res)


        except Exception as e:
            logger.error('Error {0} {1}'.format(vals, e))

def sync_products():
    manager = mp.Manager()
    data_pool = manager.list()
    create_ids = manager.dict()
    write_ids = manager.dict()
    uom_ids = manager.dict()
    category_ids = manager.dict()
    location_ids = manager.dict()


    process_Q = []

    fp = open('files/iclitem1.csv', 'r')
    csv_reader = csv.DictReader(fp)

    fp1 = open('files/ivlioh.csv', 'r')
    csv_reader1 = csv.DictReader(fp1)
    sock = xmlrpclib.ServerProxy(URL, allow_none=True)

    all_locations = sock.execute(DB, UID, PSW, 'stock.location', 'search_read', [('usage', '=', 'internal')], ['id','name'])
    all_locations = {ele['name']:ele['id'] for ele in all_locations}

    for vals in csv_reader1:
        if vals['BIN-CODE'] and vals['BIN-CODE'] in all_locations:
            location_ids[vals['ITEM-CODE'].strip()] = all_locations[vals['BIN-CODE']]

    default_codes = []
    for vals in csv_reader:
        data_pool.append(vals)
        default_code = vals['ITEM-CODE'].strip()
        default_codes.append(default_code)

    sale_uoms={}

    fp2 = open('files/ivlitum1.csv', 'r')
    csv_reader2 = csv.DictReader(fp2)
    for line in csv_reader2:
        product = line.get('ITEM-CODE', '').strip()
        code = str(line.get('UOM')).strip() + '_' + str(line.get('QTY')).strip()
        if product in sale_uoms:
            sale_uoms[product].append(code)
        else:
            sale_uoms[product] = [code]

    sale_uoms = manager.dict(sale_uoms)

    fp.close()

    domain = [('default_code', 'in', default_codes), '|', ('active', '=', False), ('active', '=', True)]

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
                            args=(pid, data_pool, create_ids, write_ids, uom_ids, category_ids, location_ids, sale_uoms))
        process_Q.append(worker)
        worker.start()

    for worker in process_Q:
        worker.join()


if __name__ == "__main__":
    # PRODUCTS
    sync_products()
