#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import multiprocessing as mp
import xmlrpc.client as xmlrpclib
import queue
import math

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


def create_do_invoice(pid, data_pool, product_ids, inv_vals):
    sock = xmlrpclib.ServerProxy(URL, allow_none=True)
    while data_pool:
        try:
            data = data_pool.pop()
            order_id = data.get('order_id')

            do_lines=[]
            invoice_vals={}
            for line in data.get('lines', []):
                inv_no = line.get('INVOICE-NO', '')
                invoice_vals = inv_vals.get(inv_no)
                product_id = product_ids.get(line.get('ITEM-CODE', ''))
                uom_factor = float(line.get('ITEM-QTY-IN-STOCK-UM')) / float(line.get('QTY-IN-ORDERING-UM'))
                quantity_ordered = float(line.get('QTY-ORDERED')) * uom_factor
                quantity_shipped = float(line.get('QTY-SHIPPED')) * uom_factor

                if uom_factor > 1 or math.isclose(1.0, uom_factor):
                    quantity_ordered = round(quantity_ordered, 0)
                    quantity_shipped = round(quantity_shipped, 0)
                else:
                    quantity_ordered = round(quantity_ordered, 3)
                    quantity_shipped = round(quantity_shipped, 3)
                vals={'product_id':product_id,
                      'quantity_ordered':quantity_ordered,
                      'quantity_shipped':quantity_shipped,

                      }
                do_lines.append(vals)
            do_line_dict = {'name':invoice_vals.get('name'),'date': invoice_vals.get('date'),'do_lines':do_lines}
            inv_amt = float(invoice_vals.get('inv_amt'))
            tax_amt = float(invoice_vals.get('tax_amt'))
            freight_amt = float(invoice_vals.get('freight_amt'))
            misc_amt = float(invoice_vals.get('misc_amt'))
            total = round(inv_amt+tax_amt+freight_amt+misc_amt, 2)

            order = sock.execute(DB, UID, PSW, 'sale.order', 'import_draft_invoice', order_id, do_line_dict, {'context':{'from_import': True}})

            if order.get('invoice_amount', '') and order.get('invoice_amount') != total:
                logger.error('Amount Mismatch in CSV and invoice --- INVOICE : {0},CSV amt:{1}, invoice Amount: {2}'.format(invoice_vals.get('name'), total, order['invoice_amount']))
            if order.get('missing_msg'):
                logger.error('Invoice not created please check DO. Missing move line in DO. {0}'.format(order.get('missing_msg')))
            else:
                logger.info('Created Invoice')
        except Exception as e:
            logger.error('Invoice :{0} Exception {1}'.format(invoice_vals.get('name'), e))
            # data_pool.append(data)



def sync_invoices():
    manager = mp.Manager()
    process_Q = []
    sock = xmlrpclib.ServerProxy(URL, allow_none=True)
    res = sock.execute(DB, UID, PSW, 'sale.order', 'search_read', [], ['note'])
    order_ids = {rec['note']: rec['id'] for rec in res}

    res = sock.execute(DB, UID, PSW, 'product.product', 'search_read',
                       ['|', ('active', '=', False), ('active', '=', True)], ['default_code'])
    products = {rec['default_code']: rec['id'] for rec in res}
    product_ids = manager.dict(products)

    inv_vals={}

    fp1 = open('files/omlcinv1.csv', 'r')
    csv_reader1 = csv.DictReader(fp1)
    for vals in csv_reader1:
        inv_no = vals.get('INVOICE-NO', '')
        inv_vals[inv_no] = {'name':inv_no,
                            'date':vals.get('INVOICE-DATE', ''),
                            'inv_amt':vals.get('INVOICE-AMT', ''),
                            'tax_amt':vals.get('TAX-AMT', ''),
                            'freight_amt':vals.get('FREIGHT-AMT', ''),
                            'misc_amt':vals.get('MISC-CHARGE', '')
                            }

    order_lines = {}

    fp = open('files/omlcinv2.csv', 'r')
    csv_reader = csv.DictReader(fp)
    for vals in csv_reader:
        inv_no = vals.get('INVOICE-NO', '')
        order_id = order_ids.get(inv_no)
        if order_id:
            lines = order_lines.setdefault(order_id, [])
            lines.append(vals)

    data_pool = manager.list([{'order_id': order, 'lines': order_lines[order]} for order in order_lines])

    for i in range(WORKERS):
        pid = "Worker-%d" % (i + 1)
        worker = mp.Process(name=pid, target=create_do_invoice,
            args=(pid, data_pool, product_ids, inv_vals))
        process_Q.append(worker)
        worker.start()

    for worker in process_Q:
        worker.join()

if __name__ == "__main__":
    # Invoice
    sync_invoices()
