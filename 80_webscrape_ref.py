#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
from xmlrpc import client as xmlrpclib
import multiprocessing as mp

from scriptconfig import URL, DB, UID, PSW, WORKERS

# ============================= P R O D U C T - C R O S S - R E F ===============================

def update_cross_ref(pid, data_pool, product_ids, config_id, product_sku_ref_ids, file_header):
    sock = xmlrpclib.ServerProxy(URL, allow_none=True)
    while data_pool:
        try:
            data = data_pool.pop()

            default_code = data.get('PPT CODE').strip()


            vals={'product_id': product_ids.get(default_code),
                  'web_config': config_id,
                  'competitor_sku': data.get(file_header[0]).strip(),
                  'competitor_desc': data.get(file_header[1]).strip(),
                  'qty_in_uom': data.get('COUNT').strip(),
                  }

            res = product_sku_ref_ids.get(product_ids.get(default_code, ''), '')
            if res:
                sock.execute(DB, UID, PSW, 'product.sku.reference', 'write', res, vals)
                print(pid, 'UPDATE - PRODUCT', res)
            else:
                res = sock.execute(DB, UID, PSW, 'product.sku.reference', 'create', vals)
                print(pid, 'CREATE - PRODUCT', res)
        except Exception as e:
            print(e)
            break


def sync_cross_ref(file, competitor, file_header):
    manager = mp.Manager()
    data_pool = manager.list()
    product_ids = manager.dict()
    product_sku_ref_ids = manager.dict()

    process_Q = []

    fp = open(file, 'r')

    csv_reader = csv.DictReader(fp)

    default_codes = []
    for vals in csv_reader:
        data_pool.append(vals)
        default_code = vals['PPT CODE'].strip()
        default_codes.append(default_code)

    fp.close()

    domain = [('default_code', 'in', default_codes), '|', ('active', '=', False), ('active', '=', True)]
    sock = xmlrpclib.ServerProxy(URL, allow_none=True)
    products = sock.execute(DB, UID, PSW, 'product.product', 'search_read', domain, ['default_code'])
    product_ids = {product['default_code']: product['id']  for product in products}


    config = sock.execute(DB, UID, PSW, 'website.scraping.cofig', 'search_read', [('competitor', '=', competitor)], ['competitor'])

    if not config:
    	print('There is no configuration for %s', competitor)
    else:


        config_id = config[0]['id']
        print (config_id, config)

        domain = [('web_config', '=', config_id)]
        product_sku_refs = sock.execute(DB, UID, PSW, 'product.sku.reference', 'search_read', domain, ['product_id'])
        product_sku_ref_ids = {product_sku_ref['product_id'][0]: product_sku_ref['id']  for product_sku_ref in product_sku_refs}
        print (product_sku_ref_ids)


        default_codes = None

        for i in range(WORKERS):
            pid = "Worker-%d" % (i + 1)
            worker = mp.Process(name=pid, target=update_cross_ref, args=(pid, data_pool, product_ids, config_id, product_sku_ref_ids, file_header))
            process_Q.append(worker)
            worker.start()

        for worker in process_Q:
            worker.join()

def wdepot():
    file = 'files/WEBSTAURANT SCRUBBER - Sheet1.csv'
    competitor = 'wdepot'
    comp_sku = ['COMP CODE', 'COMP DESCRIPTION']
    sync_cross_ref(file, competitor, comp_sku)

def rdepot():
    file = 'files/Depot Scrubber Update - Sheet1.csv'
    competitor = 'rdepot'
    file_header = ['COMP SKU', 'COMP DESC']
    sync_cross_ref(file, competitor, file_header)


if __name__ == "__main__":



    # webstaurant
    wdepot()

    # restaurant
    rdepot()
