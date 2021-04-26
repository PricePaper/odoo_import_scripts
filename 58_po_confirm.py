#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import logging.handlers
import multiprocessing as mp
import os
import random
import time
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

def update_purchase_order_line(pid, data_pool):
    sock = xmlrpclib.ServerProxy(URL, allow_none=True)
    while data_pool:
        try:
            data = data_pool.pop()
            print(data)
            res = sock.execute(DB, UID, PSW, 'purchase.order', 'button_confirm', data)
            logger.info('PURCHASE ORDER Validated Order_id:{0} '.format(data))
        except Exception as e:
            logger.error(f'Exception : {e}\n Order_id: {data}')


def sync_purchase_order_lines():
    manager = mp.Manager()
    data_pool = manager.list()
    process_Q = []

    sock = xmlrpclib.ServerProxy(URL, allow_none=True)

    res = sock.execute(DB, UID, PSW, 'purchase.order', 'search_read', [('state', '=', 'draft')], ['id', 'name'])
    order_ids = {order['name']: order['id'] for order in res}

    fp = open('files/polordr1.csv')
    csv_reader = csv.DictReader(fp)

    order_lines = {}
    for vals in csv_reader:
        order_no = vals.get('ORDR-NUM', '')
        order_id = order_ids.get(order_no)
        if order_id:
            data_pool.append(order_id)
    print(len(data_pool))

    fp.close()

    res = None
    order_ids = None


    for i in range(WORKERS):
        pid = "Worker-%d" % (i + 1)
        worker = mp.Process(name=pid, target=update_purchase_order_line,
                            args=(pid, data_pool))
        process_Q.append(worker)
        worker.start()

    for worker in process_Q:
        worker.join()


if __name__ == "__main__":
    sync_purchase_order_lines()
