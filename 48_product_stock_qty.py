#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import csv
import multiprocessing as mp
import xmlrpc.client
import logging.handlers
import math

# Get this using pip
import multiprocessing_logging

from scriptconfig import URL, DB, UID, PSW, WORKERS
# Too many workers makes Odoo angry
WORKERS = 4

# Set up logging
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
# create file handler which logs warn/errors to file
errorlogfile = os.path.splitext(filename)[0] + '-error.log'
eh = logging.FileHandler(errorlogfile, mode='w')
eh.setLevel(logging.WARNING)
# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
fh.setFormatter(formatter)
eh.setFormatter(formatter)
# add the handlers to logger
logger.addHandler(ch)
logger.addHandler(fh)
logger.addHandler(eh)
multiprocessing_logging.install_mp_handler(logger=logger)

# ==================================== P R O D U C T S ====================================

def update_product(pid, data_pool, product_ids, location_ids, uoms):
    sock = xmlrpc.client.ServerProxy(URL, allow_none=True)
    while data_pool:
        try:
            data = data_pool.pop()

            default_code = data.get('ITEM-CODE')
            location = data.get('BIN-CODE')
            new_quantity = float(data.get('ON-HAND-QTY'))
            if product_ids.get(default_code) and location_ids.get(location) and new_quantity>0:
                product_id = product_ids.get(default_code)[0]

                uom_rounding = uoms.get(product_ids.get(default_code)[1])

                new_qty = new_quantity / uom_rounding
                new_qty = round(new_qty)
                new_qty = new_qty * uom_rounding

                vals = {'product_id': product_id,
                        'location_id': location_ids.get(location),
                        'new_quantity': new_qty
                        }

                id = sock.execute(DB, UID, PSW, 'stock.change.product.qty', 'create', vals)
                logger.debug(f'{pid}: Create stock move {default_code}')
                res = sock.execute(DB, UID, PSW, 'stock.change.product.qty', 'change_product_qty', id, vals)
                logger.debug(f'{pid}: Update {default_code} qty {new_quantity}')

        except xmlrpc.client.Fault as fault:
            if fault.faultString.count('serialize'):
                logger.warning('TransactionRollbackError - adding back to queue: ' + str(fault))
                data_pool.append(data)
            elif fault.faultString.count('Missing required fields'):
                logger.error('Validation Error: {0}\n{1}'.format(fault, data))
            else:
                logger.error('Unknown XMLRPC Fault: {0}\nOffending line: {1}\n'.format(fault, data))
                continue
        except Exception as e:
            logger.critical('Unexpected exception: {0}\nOffending line:{1}\n'.format(e, data))
            continue


def sync_products():
    manager = mp.Manager()
    data_pool = manager.list()
    product_ids = manager.dict()
    location_ids = manager.dict()

    process_Q = []

    sock = xmlrpc.client.ServerProxy(URL, allow_none=True)

    all_locations = sock.execute(DB, UID, PSW, 'stock.location', 'search_read', [('usage', '=', 'internal')], ['id','name'])
    location_ids = {ele['name']:ele['id'] for ele in all_locations}

    default_codes = []

    with open('files/ivlioh.csv', newline='') as fp1:
        csv_reader1 = csv.DictReader(fp1)
        for vals in csv_reader1:
            # Skip the junk
            whs, product = vals["WHSE-CODE"], vals["ITEM-CODE"]
            if whs != "ZDEAD" and whs != "ZDEAD1":
                data_pool.append(vals)
            else:
                logger.info(f'Skipping item {product}')

    domain = ['|', ('active', '=', False), ('active', '=', True)]
    res = sock.execute(DB, UID, PSW, 'product.product', 'search_read', domain, ['default_code', 'uom_id'])
    product_ids = {rec['default_code']: [rec['id'], rec['uom_id'][1]] for rec in res}

    uoms = sock.execute(DB, UID, PSW, 'uom.uom', 'search_read', [], ['rounding','name'])
    uoms = {uom['name']: uom['rounding'] for uom in uoms}

    res = None
    for i in range(WORKERS):
        pid = "Worker-%d" % (i + 1)
        worker = mp.Process(name=pid, target=update_product,
                            args=(pid, data_pool, product_ids, location_ids, uoms))
        process_Q.append(worker)
        worker.start()

    for worker in process_Q:
        worker.join()


if __name__ == "__main__":
    # PRODUCTS
    sync_products()
