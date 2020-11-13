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
            inv = sock.execute(DB, UID, PSW, 'sale.order', 'action_create_open_invoice_xmlrpc', data)
            logger.info('Invoice created worker:{0} invoice_id:{1} '.format(pid,inv))
        except Exception as e:
            logger.error('Exception --- order id {0} error:{1}'.format(data,e))


def sync_invoices():
    manager = mp.Manager()
    # orders = manager.list()
    orders = manager.JoinableQueue()
    process_Q = []

    sock = xmlrpclib.ServerProxy(URL, allow_none=True)
    res = sock.execute(DB, UID, PSW, 'sale.order', 'search_read', [], ['note'])
    order_ids = {inv_no: rec['id'] for rec in res for inv_no in (rec['note'] or '').split(',')}

    with open('files/rclopen1.csv', newline='') as f:
        csv_reader = csv.DictReader(f)
        for vals in csv_reader:
            inv_no = vals.get('INVOICE-NO', '')
            order_id = order_ids.get(inv_no)
            if order_id:
                orders.put(order_id)


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
