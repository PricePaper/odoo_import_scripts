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


# ==================================== PURCHASE ORDER LINE ====================================

def update_purchase_order_line(pid, data_pool, error_ids, product_ids, uom_ids):
    sock = xmlrpclib.ServerProxy(URL, allow_none=True)
    while data_pool:
        try:
            order_line_ids = ''
            data = data_pool.pop()
            order_id = data.get('order_id')
            order_lines = sock.execute(DB, UID, PSW, 'purchase.order.line', 'search_read',
                                       [('order_id', '=', order_id)], ['product_id'])
            order_line_ids = [rec['product_id'][0] for rec in order_lines]

            lines = data.get('lines', [])
            line = ''
            while len(lines) > 0:
                try:
                    line = lines.pop()
                    product_name = line.get('ITEM-CODE', '')
                    product_id = product_ids.get(product_name)
                    code1 = str(line.get('ORDR-UOM')) + '_' + str(line.get('ORDR-VAL-QTY'))
                    code = uom_ids.get(code1)
                    if not product_id:
                        error_ids.append((line.get('ORDR-NUM', ''), product_name))
                        continue
                    if not code:
                        error_ids.append((line.get('ORDR-NUM', ''), code))
                        continue
                    if product_id in order_line_ids:
                        logger.debug('Duplicate - {}'.format(line))
                        continue
                    vals = {'product_id': product_id,
                            'product_uom': code,
                            'price_unit': line.get('ORDR-UNIT-COST'),
                            'product_qty': line.get('ORDR-QTY'),
                            'name': line.get('ITEM-DESC', ' ') + line.get('ITEM-DESCR', ' '),
                            'order_id': order_id,
                            'date_planned': line.get('ORDR-LINE-REQD-DATE', '')
                            }

                    res = sock.execute(DB, UID, PSW, 'purchase.order.line', 'create', vals)
                    if res % 100 != 0:
                        logger.debug('Worker {0} Create - PURCHASE ORDER LINE {1} {2}'.format(pid, order_id, res))
                    else:
                        logger.info('Worker {0} Create - PURCHASE ORDER LINE {1} {2}'.format(pid, order_id, res))
                except xmlrpc.client.ProtocolError:
                    logger.warning("ProtocolError: adding {} back to the work queue".format(order_id))
                    lines.append(line)
                    time.sleep(random.randint(1, 3))
                    continue
        except xmlrpc.client.Fault as fault:
            if fault.faultString.count('serialize'):
                logger.warning('TransactionRollbackError - adding back to queue: ' + str(fault))
                lines.append(line)
            elif fault.faultString.count('Missing required fields on accountable sale order line'):
                logger.error('Validation Error: {0}\n{1}'.format(fault, line))
            else:
                logger.error('Unknown XMLRPC Fault: {}'.format(fault))
            continue
        except Exception as e:
            logger.critical('Unexpected exception: {}'.format(e))
            continue

        except xmlrpc.client.ProtocolError:
            logger.warning("ProtocolError: adding {} back to the work queue".format(order_id))
            time.sleep(random.randint(1, 3))
            data_pool.append(data)
            continue


def sync_purchase_order_lines():
    manager = mp.Manager()
    data_pool = manager.list()
    error_ids = manager.list()
    process_Q = []

    sock = xmlrpclib.ServerProxy(URL, allow_none=True)

    res = sock.execute(DB, UID, PSW, 'purchase.order', 'search_read', [], ['id', 'name'])
    order_ids = {order['name']: order['id'] for order in res}

    fp = open('files/polhist2.csv')
    csv_reader = csv.DictReader(fp)

    order_lines = {}
    for vals in csv_reader:
        order_no = vals.get('ORDR-NUM', '')
        order_id = order_ids.get(order_no)
        if order_id:
            lines = order_lines.setdefault(order_id, [])
            lines.append(vals)

    fp.close()

    data_pool = manager.list([{'order_id': order, 'lines': order_lines[order]} for order in order_lines])

    res = sock.execute(DB, UID, PSW, 'product.product', 'search_read',
                       ['|', ('active', '=', False), ('active', '=', True)], ['default_code'])
    products = {rec['default_code']: rec['id'] for rec in res}
    product_ids = manager.dict(products)

    uoms = sock.execute(DB, UID, PSW, 'uom.uom', 'search_read', [], ['id', 'name'])
    uom_ids = {uom['name']: uom['id'] for uom in uoms}

    res = None
    order_ids = None
    order_lines = None
    products = None

    for i in range(WORKERS):
        pid = "Worker-%d" % (i + 1)
        worker = mp.Process(name=pid, target=update_purchase_order_line,
                            args=(pid, data_pool, error_ids, product_ids, uom_ids))
        process_Q.append(worker)
        worker.start()

    for worker in process_Q:
        worker.join()


if __name__ == "__main__":
    sync_purchase_order_lines()
