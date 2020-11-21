#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import logging.handlers
import os
import time
import multiprocessing as mp
import xmlrpc.client as xmlrpclib
import queue

import multiprocessing_logging

from scriptconfig import URL, DB, UID, PSW, WORKERS

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
# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
fh.setFormatter(formatter)
# add the handlers to logger
logger.addHandler(ch)
logger.addHandler(fh)
multiprocessing_logging.install_mp_handler(logger=logger)


# ==================================== PRIMARY VENDOR ====================================

def update_product_vendor(pid, data_pool, product_ids, partner_ids, supplier_price_ids):
    sock = xmlrpclib.ServerProxy(URL, allow_none=True)
    while data_pool:
        vals={}
        try:
            data = data_pool.pop()
            product = product_ids.get(data.get('ITEM-CODE'))
            product_id = product and product[0]
            vendor_id =  partner_ids.get(data.get('PRIME-VEND-CODE'))
            if product_id and vendor_id:
                vals={'name': vendor_id,
                    'product_id': product_id,
                    'product_tmpl_id':product[1][0],
                    'price': data.get('ITEM-UNIT-COST', 1000000.00),
                    'sequence':0
                    }
                if product_id in supplier_price_ids and vendor_id in supplier_price_ids[product_id]:
                    status = sock.execute(DB, UID, PSW, 'product.supplierinfo', 'write', supplier_price_ids[product_id][vendor_id], vals)
                    logger.info('Updated Vendor info ID:{0}, Product:{1}, Vendor:{2}'.format(supplier_price_ids[product_id][vendor_id], product_id, vendor_id))
                else:
                    status = sock.execute(DB, UID, PSW, 'product.supplierinfo', 'create', vals)
                    logger.info('Created Vendor info ID:{0}, Product:{1}, Vendor:{2}'.format(status, product_id, vendor_id))
            else:
                logger.error('Vendor or Product Missing in DB vendor: {0}, product{1}'.format(data.get('ITEM-CODE'), data.get('PRIME-VEND-CODE')))
        except Exception as e:
            logger.error(e)


def sync_primary_vendor():
    manager = mp.Manager()
    data_pool = manager.list()
    product_ids = manager.dict()
    partner_ids = manager.dict()
    items_tmpl_ids = manager.dict()

    process_Q = []

    sock = xmlrpclib.ServerProxy(URL, allow_none=True)

    fp = open('files/iclitem1.csv', 'r')
    csv_reader = csv.DictReader(fp)

    for vals in csv_reader:
        data_pool.append(vals)

    fp.close()

    domain = ['|', ('active', '=', False), ('active', '=', True)]

    products =  sock.execute(DB, UID, PSW, 'product.product', 'search_read', domain, ['default_code', 'product_tmpl_id'])
    product_ids = {rec['default_code']: [rec['id'], rec['product_tmpl_id']] for rec in products}

    vendors = sock.execute(DB, UID, PSW, 'res.partner', 'search_read', [('supplier', '=', True ), '|', ('active', '=', False), ('active', '=', True)], ['id','customer_code'])
    partner_ids = {vendor['customer_code']: vendor['id'] for vendor in vendors}

    supplier_info = sock.execute(DB, UID, PSW, 'product.supplierinfo', 'search_read', [('product_id', '!=', False)], ['id','name','product_id'])
    supplier_price_ids ={}
    for info in supplier_info:
        if info['product_id'][0] in supplier_price_ids:
            if info['name'][0] not in supplier_price_ids[info['product_id'][0]]:
                supplier_price_ids[info['product_id'][0]][info['name'][0]] = info['id']
        else:
            supplier_price_ids[info['product_id'][0]] = {info['name'][0]: info['id']}
    supplier_price_ids = manager.dict(supplier_price_ids)

    products = None
    vendors = None
    supplier_info = None

    for i in range(WORKERS):
        pid = "Worker-%d" % (i + 1)
        worker = mp.Process(name=pid, target=update_product_vendor,
                            args=(pid, data_pool, product_ids, partner_ids, supplier_price_ids))
        process_Q.append(worker)
        worker.start()

    for worker in process_Q:
        worker.join()

if __name__ == "__main__":
    # PRIMARY_VENDOR
    sync_primary_vendor()


#     WB THEH01
# SURGICAL MASK SOHO01
# STCR21151/4 AMAZ01
# N95 MASK SOHO01
# 1PUMP WRIS01
