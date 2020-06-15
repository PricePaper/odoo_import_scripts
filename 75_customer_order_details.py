#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# -*- coding: utf-8 -*-

import csv
import logging.handlers
import multiprocessing as mp
import os
import xmlrpc.client

# Get this using pip
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


# ==================================== SALE ORDER LINE ====================================

def update_sale_order_line(pid, data_pool, error_ids, product_ids, uom_ids):
    sock = xmlrpc.client.ServerProxy(URL, allow_none=True)
    while data_pool:
        try:
            data = data_pool.pop()
            order_id = data.get('order_id')
            order_lines = sock.execute(DB, UID, PSW, 'sale.order.line', 'search_read', [('order_id', '=', order_id)],
                                       ['product_id', 'product_uom'])
            order_line_ids = {rec['product_id'][0]: rec['product_uom'][0] for rec in order_lines}

            for line in data.get('lines', []):
                product_id = product_ids.get(line.get('ITEM-CODE', '').strip())
                code = str(line.get('ORDERING-UOM')).strip() + '_' + str(line.get('QTY-IN-ORDERING-UM')).strip()
                code = uom_ids.get(code)

                if not product_id and not code:
                    error_ids.append()
                    continue
                if product_id in order_line_ids and code == order_line_ids[product_id]:
                    logger.debug('Duplicate - {}'.format(line))
                    continue

                vals = {
                    'order_id': order_id,
                    'product_id': product_id,
                    'name': line.get('ITEM-DESC').strip(),
                    'price_unit': line.get('PRICE-DISCOUNTED').strip(),
                    'product_uom_qty': line.get('QTY-ORDERED').strip(),
                    'qty_delivered': line.get('QTY-SHIPPED').strip(),
                    'is_last': False,
                    'working_cost': line.get('TRUE-FIXED-COST').strip(),
                    'lst_price': line.get('PRICE-DISCOUNTED').strip(),
                    'product_uom': code,
                }

                res = sock.execute(DB, UID, PSW, 'sale.order.line', 'create', vals)
                if res % 100 != 0:
                    logger.debug('Create - SALE ORDER LINE {0} {1}'.format(order_id, res))
                else:
                    logger.info('Create - SALE ORDER LINE {0} {1}'.format(order_id, res))


        except xmlrpc.client.ProtocolError:
            logger.warning("ProtocolError: adding {} back to the work queue".format(order_id))
            data_pool.append(data)
            continue
        except xmlrpc.client.Fault as fault:
            if fault.faultString.find('ValidationError'):
                logger.error('Validation Error: {0}\n{1}'.format(fault, line))
            elif fault.faultString.find('TransactionRollbackError'):
                logger.warning('TransactionRollbackError - adding back to queue')
                data_pool.append(data)
            else:
                logger.error('Unknown XMLRPC Fault: {}'.format(fault))
            continue
        except Exception as e:
            logger.critical('Unexpected exception: {}'.format(e))
            continue


def sync_sale_order_lines():
    manager = mp.Manager()
    data_pool = manager.list()
    error_ids = manager.list()
    process_Q = []

    sock = xmlrpc.client.ServerProxy(URL, allow_none=True)
    res = sock.execute(DB, UID, PSW, 'sale.order', 'search_read', [], ['note'])
    order_ids = {inv_no: rec['id'] for rec in res for inv_no in (rec['note'] or '').split(',')}

    fp = open('files/omlhist2.csv', 'r')
    csv_reader = csv.DictReader(fp)

    logger.debug('Opened File {}'.format(fp.name))

    order_lines = {}
    for vals in csv_reader:
        inv_no = vals.get('INVOICE-NO', '').strip()
        order_id = order_ids.get(inv_no)
        if order_id:
            lines = order_lines.setdefault(order_id, [])
            lines.append(vals)

    fp.close()
    logger.debug('Closed File {}'.format(fp.name))

    data_pool = manager.list([{'order_id': order, 'lines': order_lines[order]} for order in order_lines])

    res = sock.execute(DB, UID, PSW, 'product.product', 'search_read', ['|', ('active', '=', False), ('active', '=', True)], ['default_code'])
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
        worker = mp.Process(name=pid, target=update_sale_order_line,
                            args=(pid, data_pool, error_ids, product_ids, uom_ids))
        process_Q.append(worker)
        worker.start()

    for worker in process_Q:
        worker.join()


if __name__ == "__main__":
    # SALE ORDER LINE
    sync_sale_order_lines()
