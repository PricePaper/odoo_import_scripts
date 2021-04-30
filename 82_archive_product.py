#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import logging.handlers
import math
import multiprocessing as mp
import os
import ssl
from xmlrpc import client as xmlrpclib

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


# ==================================== P R O D U C T S ====================================

def update_product(pid, data_pool, odoo_products):
    sock = xmlrpclib.ServerProxy(URL, allow_none=True, context=ssl._create_unverified_context())
    while data_pool:
        try:
            data = data_pool.pop()

            default_code = data.get('ITEM-CODE')
            active = True
            purchase_ok = False
            sale_okay = True
            qty_available = odoo_products[default_code]['qty_available']

            if math.isclose(0.0, qty_available) or qty_available < 0.0:
                active = False
                sale_okay = False

            vals = {
                'active': active,
                'purchase_ok': purchase_ok,
                'sale_okay': sale_okay
            }

            product_id = odoo_products[default_code]['id']
            sock.execute(DB, UID, PSW, 'product.product', 'write', product_id, vals)
            logger.info(
                f'{pid} UPDATE {default_code} qty={qty_available} sale_okay={sale_okay} purchase_ok={purchase_ok} active={active}')

        except Exception as e:
            logger.error(f'{pid} {e}', f'{data}')


def sync_products():
    manager = mp.Manager()
    data_pool = manager.list()

    process_Q = []

    default_codes = []
    with open('files/iclitem1.csv', 'r') as fp:
        csv_reader = csv.DictReader(fp)

        for vals in csv_reader:
            if vals.get("DEF-WHSE-CODE") == "PRI001":
                if vals.get('ITEM-STATUS') and vals.get('ITEM-STATUS') == 'D':
                    data_pool.append(vals)
                    default_code = vals['ITEM-CODE']
                    default_codes.append(default_code)

    domain = [('default_code', 'in', default_codes), '|', ('active', '=', False), ('active', '=', True)]

    # Get product ids from Odoo
    sock = xmlrpclib.ServerProxy(URL, allow_none=True, context=ssl._create_unverified_context())
    res = sock.execute(DB, UID, PSW, 'product.product', 'search_read', domain, ('default_code', 'qty_available'))
    odoo_products = {rec['default_code']: {'id': rec['id'], 'qty_available': rec['qty_available']} for rec in res}

    # Free up resources
    del res, default_codes, csv_reader

    for i in range(WORKERS):
        pid = "Worker-%d" % (i + 1)
        worker = mp.Process(name=pid, target=update_product,
                            args=(pid, data_pool, odoo_products))
        process_Q.append(worker)
        worker.start()

    for worker in process_Q:
        worker.join()


if __name__ == "__main__":
    # PRODUCTS
    sync_products()
