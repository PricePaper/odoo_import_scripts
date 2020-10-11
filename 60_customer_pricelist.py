#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import logging.handlers
import multiprocessing as mp
import os
import random
import time
import xmlrpc.client
from xmlrpc import client as xmlrpclib
import datetime

# Get this using pip
import multiprocessing_logging

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

from scriptconfig import URL, DB, UID, PSW, WORKERS


# =================================== PRICE LIST ========================================

def update_price_list(pid, data_pool, write_ids, uom_ids, partner_ids, pricelist_ids, shared_list, shared_dict,
                      product_ids, broken_uom):
    sock = xmlrpclib.ServerProxy(URL, allow_none=True)
    while data_pool:
        # try:
        data = data_pool.pop()
        price_list = data.get('pricelist_id', '')
        pricelist_id = write_ids.get(price_list, '')
        if not pricelist_id:
            vals = {
                'name': price_list,
                'type': 'customer'
            }
            if price_list in shared_list:
                vals['type'] = 'shared'
            pricelist_id = sock.execute(DB, UID, PSW, 'product.pricelist', 'create', vals)
            partner = partner_ids.get(price_list, '')
            partner_vals = {
                'partner_id': partner,
                'pricelist_id': pricelist_id
            }
            sock.execute(DB, UID, PSW, 'customer.pricelist', 'create', partner_vals)
            logger.info('{} CREATE - PRICELIST {}'.format(pid, price_list))
            write_ids[price_list] = pricelist_id

        line_ids = sock.execute(DB, UID, PSW, 'customer.product.price', 'search_read',
                                [('pricelist_id', '=', pricelist_id),('pricelist_id', '!=', False)],
                                ['product_id', 'product_uom', 'price'])

        pricelist_line_ids = {}
        for rec in line_ids:
            if pricelist_id in pricelist_line_ids:
                if rec['product_id'][0] in pricelist_line_ids[pricelist_id]:
                    if rec['product_uom'][0] not in pricelist_line_ids[pricelist_id][rec['product_id'][0]]:
                        pricelist_line_ids[pricelist_id][rec['product_id'][0]][rec['product_uom'][0]] = [rec['price'], rec['id']]
                else:
                    pricelist_line_ids[pricelist_id][rec['product_id'][0]] = {rec['product_uom'][0]: [rec['price'], rec['id']]}
            else:
                pricelist_line_ids[pricelist_id] = {rec['product_id'][0]: {rec['product_uom'][0]: [rec['price'], rec['id']]}}

        lines = data.get('lines', [])
        line = ''
        while len(lines) > 0:
            try:
                line = lines.pop()
                product_code = line.get('ITEM-CODE', '')
                product = product_ids.get(product_code)
                product_id = product and product[0]
                uom = line.get('PRICING-UOM')
                if product and uom == product[1][1].split('_')[0]:
                    uom_id = product[1][0]
                elif product_code in broken_uom and uom in broken_uom[product_code]:
                    uom_code = uom + '_' + broken_uom[product_code][uom]
                    uom_id = uom_ids.get(uom_code)
                else:
                    logger.error('UOM mismatch: {0} {1} {2}'.format(price_list, product_code, uom))
                    continue

                if product_id:
                    if uom_id:
                        vals = {
                            'pricelist_id': pricelist_id,
                            'product_id': product_id,
                            'product_uom': uom_id,
                            'price': line.get('CURRENT-PRICE-IN-STK', 0),
                            'price_last_updated': line.get('LAST-PRICE-CHANGE-DA')
                        }
                        # if price_list not in shared_list:
                        #     vals['partner_id'] = partner_ids.get(line.get('CUSTOMER-CODE').strip())
                        status = ''
                        # if pricelist_id in pricelist_line_ids:
                        #     if product_id in pricelist_line_ids[pricelist_id]:
                        #         if uom_id in pricelist_line_ids[pricelist_id][product_id]:
                        #             logger.debug('{0} {1} {2} Duplicate Value - LINE'.format(price_list, product_code, uom))
                        #             continue
                        status = sock.execute(DB, UID, PSW, 'customer.product.price', 'create', vals)
                        if pricelist_id in pricelist_line_ids:
                            if product_id in pricelist_line_ids[pricelist_id]:
                                if uom_id not in pricelist_line_ids[pricelist_id][product_id]:
                                    pricelist_line_ids[pricelist_id][product_id][uom_id] = [vals.get('price', ''), status]
                            else:
                                pricelist_line_ids[pricelist_id][product_id] = {uom_id: [vals.get('price', ''), status]}
                        else:
                            pricelist_line_ids[pricelist_id] = {product_id : {uom_id: [vals.get('price', ''), status]}}
                        if status % 100 != 0:
                            logger.debug('CREATE - LINE'.format(pid, status))
                        else:
                            logger.info('CREATE - LINE'.format(pid, status))
                    else:
                        logger.warning("UOM Missing: {0} {1} {2}".format(uom, price_list, product_code))
                else:
                    logger.warning("Product Missing: {0} {1}".format(product_code, price_list))
            except xmlrpc.client.ProtocolError:
                logger.warning("ProtocolError: adding {} back to the work queue".format(vals))
                lines.append(line)
                time.sleep(random.randint(1, 3))
                continue
            except xmlrpc.client.Fault as err:
                if err.faultCode[:67] == 'Already a record with same product and same UOM exists in Pricelist':
                    res = sock.execute(DB, UID, PSW, 'customer.product.price', 'search_read',
                    [('pricelist_id', '=', pricelist_id),
                    ('product_id', '=', product_id), ('product_uom', '=', uom_id)], ['price_last_updated'])
                    if res:
                        val_date = vals['price_last_updated']
                        list_date = res[0].get('price_last_updated', False)
                        format_str = '%m/%d/%y'
                        val_date = datetime.datetime.strptime(val_date, format_str).date()
                        format_str = '%Y-%m-%d'
                        if list_date:
                            list_date = datetime.datetime.strptime(list_date, format_str).date()
                        if not list_date or list_date < val_date:
                            id = res[0]['id']
                            status = sock.execute(DB, UID, PSW, 'customer.product.price', 'write', id, vals)
                            logger.debug('Update - LINE'.format(pid, status))
                    else:
                        logger.debug('{0} {1} {2} Duplicate Value - LINE'.format(price_list, product_code, uom))
                else:
                    logger.error(" Error {0} {1}".format(vals, e))

            except Exception as e:
                logger.error(" Error {0} {1}".format(vals, e))


def sync_price_list():
    manager = mp.Manager()

    process_Q = []

    fp = open('files/omlphist.csv', 'r')
    fp1 = open('files/rclcust2.csv', 'r')
    csv_reader = csv.DictReader(fp)
    csv_reader1 = csv.DictReader(fp1)  # line.get('PRICING-ACCT-NO').strip()
    fp2 = open('files/ivlitum1.csv', 'r')
    csv_reader2 = csv.DictReader(fp2)

    broken_uom = {}
    for vals in csv_reader2:
        product_code = vals.get('ITEM-CODE')
        if product_code not in broken_uom:
            broken_uom[product_code] = {vals.get('UOM'): vals.get('QTY')}
        else:
            if vals.get('UOM') not in broken_uom[product_code]:
                broken_uom[product_code][vals.get('UOM')] = vals.get('QTY')
    broken_uom = manager.dict(broken_uom)

    shared_dict = {}
    shared_list = []

    for vals in csv_reader1:
        if vals.get('PRICING-ACCT-NO', ''):
            shared_dict[vals.get('CUSTOMER-CODE')] = vals.get('PRICING-ACCT-NO', '')
            if vals.get('PRICING-ACCT-NO', '') not in shared_list:
                shared_list.append(vals.get('PRICING-ACCT-NO', ''))
    shared_list = manager.list(shared_list)
    shared_dict = manager.dict(shared_dict)

    price_lists = {}
    for vals in csv_reader:
        customer_code = vals.get('CUSTOMER-CODE')
        if customer_code in shared_dict:
            continue
        lines = price_lists.setdefault(customer_code, [])
        lines.append(vals)

    data_pool = manager.list(
        [{'pricelist_id': price_list, 'lines': price_lists[price_list]} for price_list in price_lists])

    fp.close()

    sock = xmlrpclib.ServerProxy(URL, allow_none=True)

    res = sock.execute(DB, UID, PSW, 'product.pricelist', 'search_read', [], ['name'])
    pricelist_ids = {rec['name']: rec['id'] for rec in res}

    write_ids = manager.dict(pricelist_ids)

    res = ''

    res = sock.execute(DB, UID, PSW, 'res.partner', 'search_read', ['|', ('active', '=', False), ('active', '=', True)],
                       ['customer_code'])
    customers = {rec['customer_code']: rec['id'] for rec in res}
    partner_ids = manager.dict(customers)

    res = sock.execute(DB, UID, PSW, 'product.product', 'search_read',
                       ['|', ('active', '=', False), ('active', '=', True)], ['default_code', 'sale_uoms', 'uom_id'])
    products = {rec['default_code']: [rec['id'], rec['uom_id'], rec['sale_uoms']] for rec in res}
    product_ids = manager.dict(products)

    uoms = sock.execute(DB, UID, PSW, 'uom.uom', 'search_read', [], ['id', 'name'])
    uom_ids = manager.dict({uom['name']: uom['id'] for uom in uoms})

    res = None
    customer_codes = None

    for i in range(WORKERS):
        pid = "Worker-%d" % (i + 1)
        worker = mp.Process(name=pid, target=update_price_list, args=(
            pid, data_pool, write_ids, uom_ids, partner_ids, pricelist_ids, shared_list, shared_dict, product_ids,
            broken_uom))
        process_Q.append(worker)
        worker.start()

    for worker in process_Q:
        worker.join()


def sync_partner_pricelist():
    sock = xmlrpclib.ServerProxy(URL, allow_none=True)

    res = sock.execute(DB, UID, PSW, 'res.partner', 'search_read',
                       [('customer', '=', True), '|', ('active', '=', False), ('active', '=', True)], ['customer_code'])
    partner_ids = {rec['customer_code']: rec['id'] for rec in res}

    res = sock.execute(DB, UID, PSW, 'product.pricelist', 'search_read', [], ['name'])
    pricelist_ids = {rec['name']: rec['id'] for rec in res}

    res = ''
    customer_price_list = {}
    res = sock.execute(DB, UID, PSW, 'customer.pricelist', 'search_read', [], ['partner_id', 'id'])
    for rec in res:
        if rec['partner_id']:
            lines = customer_price_list.setdefault(rec['partner_id'][0], [])
            lines.append(rec['id'])

    fp1 = open('files/rclcust2.csv', 'r')
    csv_reader1 = csv.DictReader(fp1)

    shared_dict = {}

    for vals in csv_reader1:
        if vals.get('PRICING-ACCT-NO', ''):
            shared_dict[vals.get('CUSTOMER-CODE')] = vals.get('PRICING-ACCT-NO', '')

    for rec in shared_dict:
        partner_id = partner_ids.get(rec, '')
        shared_id = pricelist_ids.get(shared_dict.get(rec, ''))
        if partner_id in customer_price_list:
            unlink_list = customer_price_list.get(partner_id, '')
            shared_id = pricelist_ids.get(shared_dict.get(rec, ''))
            if unlink_list:
                sock.execute(DB, UID, PSW, 'customer.pricelist', 'unlink', unlink_list)
                logger.debug('Deleted {}'.format(unlink_list))
        vals = {
            'partner_id': partner_id,
            'pricelist_id': shared_id
        }
        status = sock.execute(DB, UID, PSW, 'customer.pricelist', 'create', vals)
        logger.debug('Updated Customer {} '.format(status))


if __name__ == "__main__":
    # price_list
    sync_price_list()

    # pricelist_lines
    sync_partner_pricelist()
