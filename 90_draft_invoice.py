#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import logging.handlers
import os
import time
import multiprocessing as mp
import xmlrpc.client as xmlrpclib
import queue

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


def sync_invoices():

    sock = xmlrpclib.ServerProxy(URL, allow_none=True)
    res = sock.execute(DB, UID, PSW, 'sale.order', 'search_read', [('state', 'in' , ('draft', 'sent'))], ['name'])
    order_ids = {rec['name']: rec['id'] for rec in res}


    orders = []
    with open('files/omlcinv1.csv', newline='') as f:
        csv_reader = csv.DictReader(f)
        for vals in csv_reader:
            try:
                name = vals.get('1ST-NAME', '')
                if name == 'VOID':
                    continue
                else:
                    order_no = vals.get('ORDER-NO', '')
                    order_id = order_ids.get(order_no)
                    if order_id:
                        inv_id = sock.execute(DB, UID, PSW, 'sale.order', 'action_create_draft_invoice_xmlrpc', order_id)
                        logger.info('Created Invoice:{0}  Order:{1}',.format(pid,order_no) )
            except Exception as e:
                print(e)


if __name__ == "__main__":
    # Invoice
    sync_invoices()
