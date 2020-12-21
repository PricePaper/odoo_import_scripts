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


# ==================================== SALE ORDER ====================================

def update_invoice(pid, orders):
    while True:
        try:
            sock = xmlrpclib.ServerProxy(URL, allow_none=True)
            data = orders.get_nowait()
        except:
            break
        try:
            inv = sock.execute(DB, UID, PSW, 'sale.order', 'action_create_open_invoice_xmlrpc', data['ref'], data['invoice'][2])
            logger.info('invoice_id:{0} '.format(inv[0]))
            if inv[1]['sale_amount'] == data['invoice'][1] and (inv[1]['sale_amount'] == inv[1]['invoice_amount'] or inv[1]['sale_amount'] == -inv[1]['invoice_amount']):
                continue
            logger.error('Amount Mismatch in CSV and invoice --- INVOICE : {0}, ORDER id:{1}, {2}, CSV amt:{3}'.format(data['invoice'][0],data['ref'], inv[1], data['invoice'][1]))

        except Exception as e:
            logger.error('Exception --- order id {0} error:{1}'.format(data,e))



def sync_invoices():
    manager = mp.Manager()
    orders = manager.JoinableQueue()
    process_Q = []
    missing_invoices=[]
    invoices=[]
    duplicate={}

    sock = xmlrpclib.ServerProxy(URL, allow_none=True)
    res = sock.execute(DB, UID, PSW, 'sale.order', 'search_read', [], ['note'])
    order_ids = {inv_no: rec['id'] for rec in res for inv_no in (rec['note'] or '').split(',')}

    orders_dict ={}

    with open('files/rclopen1.csv', newline='') as f:
        csv_reader = csv.DictReader(f)
        for vals in csv_reader:
            inv_no = vals.get('INVOICE-NO', '')
            if inv_no not in invoices:
                invoices.append(inv_no)
            elif inv_no in duplicate:
                duplicate[inv_no] += 1
            else:
                duplicate[inv_no] = 2
            order_id = order_ids.get(inv_no, False)
            if order_id:
                if order_id not in orders_dict:

                    orders_dict[order_id] = [inv_no, float(vals.get('NET-AMT', '0')), vals.get('INVOICE-DATE', '')]
                else:
                    amt = orders_dict[order_id][2] + float(vals.get('NET-AMT', '0'))
                    orders_dict[order_id] = [inv_no, amt]
            else:
                if inv_no not in missing_invoices:
                    missing_invoices.append(inv_no)
    for ref in orders_dict:
        orders.put({'ref': ref, 'invoice': orders_dict[ref]})
    logger.info('Number of orders to process:{0} '.format(orders.qsize()))
    logger.info('Number of Orders Missing:{0} '.format(len(missing_invoices)))
    logger.error('Missing Order invoice numbers:{0} '.format(missing_invoices))
    logger.info('Repeated Invoices:{0} '.format(duplicate))


    for i in range(WORKERS):
        pid = "Worker-%d" % (i + 1)
        worker = mp.Process(name=pid, target=update_invoice,
                            args=(pid, orders))
        process_Q.append(worker)
        worker.start()

    for worker in process_Q:
        worker.join()

if __name__ == "__main__":
    # Invoice
    sync_invoices()
    # A76620
