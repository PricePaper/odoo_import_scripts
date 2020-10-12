#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import multiprocessing as mp
from xmlrpc import client as xmlrpclib
from datetime import datetime, timedelta

from scriptconfig import URL, DB, UID, PSW, WORKERS


# ==================================== PRIMARY VENDOR ====================================
def _get_price_expire_date(price_date):
    """Compare date of price to current date. If price is current, return an empty string else 
    return an expire date"""

    try:
        expire_date = price_date + timedelta(days=365)

        if expire_date < datetime.now().date():
            return expire_date.strftime("%Y-%m-%d")
        
        return ''
    except Exception as e:
        print(e)
        return '2000-01-01'
def update_product_vendor(pid, data_pool, product_ids, partner_ids, supplier_price_ids, items_tmpl_ids):
    sock = xmlrpclib.ServerProxy(URL, allow_none=True)
    while data_pool:
        try:
            data = data_pool.pop()
            product_id = product_ids.get(data.get('ITEM-CODE'))
            vendor_id =  partner_ids.get(data.get('VEND-CODE'))

            # Get price date and see if we need to expire it or if it is current
            price_date = data.get('ITEM-LAST-PURCH-DATE', '')
            if not price_date:
                price_date = data.get('ITEM-VEND-QUOT-DATE', '')

            price_date = datetime.strptime(price_date, '%m/%d/%y').date()
            price_expire_date = _get_price_expire_date(price_date)

            if product_id and vendor_id:
                vals={'name': vendor_id,
                    'product_id': product_id,
                    'product_tmpl_id':items_tmpl_ids.get(data.get('ITEM-CODE')),
                    'date_start': price_date.strftime('%Y-%m-%d')
                    }
                # If there is an expire date, set it
                if price_expire_date:
                    vals['date_end'] = price_expire_date

                # See if there is a price in the file, if yes, set it
                price = data.get('ITEM-LAST-PURCH-COST','')
                if price:
                    vals['price'] = price

                if product_id in supplier_price_ids and vendor_id == supplier_price_ids[product_id][0]:
                    status = sock.execute(DB, UID, PSW, 'product.supplierinfo', 'write', supplier_price_ids[product_id][1], vals)
                    print (pid, 'Update - Vendor - info', supplier_price_ids[product_id][1], product_id, vendor_id)
                else:
                    status = sock.execute(DB, UID, PSW, 'product.supplierinfo', 'create', vals)
                    print (pid, 'Create - Vendor - info', status, product_id, vendor_id)
            else:
                print(data.get('ITEM-CODE'), data.get('VEND-CODE'))
        except Exception as e:
            print(e)


def sync_vendor_price():
    manager = mp.Manager()
    data_pool = manager.list()
    product_ids = manager.dict()
    partner_ids = manager.dict()
    supplier_price_ids = manager.dict()
    items_tmpl_ids = manager.dict()

    process_Q = []

    sock = xmlrpclib.ServerProxy(URL, allow_none=True)

    fp = open('files/iclitem5.csv', 'r')
    csv_reader = csv.DictReader(fp)

    for vals in csv_reader:
        data_pool.append(vals)

    fp.close()

    domain = ['|', ('active', '=', False), ('active', '=', True)]

    products =  sock.execute(DB, UID, PSW, 'product.product', 'search_read', domain, ['default_code'])
    product_ids = {rec['default_code']: rec['id'] for rec in products}

    vendors = sock.execute(DB, UID, PSW, 'res.partner', 'search_read', [('supplier', '=', True ), '|', ('active', '=', False), ('active', '=', True)], ['id','customer_code'])
    partner_ids = {vendor['customer_code']: vendor['id'] for vendor in vendors}

    supplier_info = sock.execute(DB, UID, PSW, 'product.supplierinfo', 'search_read', [('product_id', '!=', False)], ['id','name','product_id'])
    supplier_price_ids = {info['product_id'][0]: [info['name'][0], info['id']] for info in supplier_info}

    products_tmpl = sock.execute(DB, UID, PSW, 'product.template', 'search_read', ['|', ('active', '=', False), ('active', '=', True)], ['id','default_code'])
    items_tmpl_ids = {product['default_code']: product['id'] for product in products_tmpl}



    products = None
    vendors = None
    supplier_info = None

    for i in range(WORKERS):
        pid = "Worker-%d" % (i + 1)
        worker = mp.Process(name=pid, target=update_product_vendor,
                            args=(pid, data_pool, product_ids, partner_ids, supplier_price_ids, items_tmpl_ids))
        process_Q.append(worker)
        worker.start()

    for worker in process_Q:
        worker.join()

if __name__ == "__main__":
    # PRIMARY_VENDOR
    sync_vendor_price()
