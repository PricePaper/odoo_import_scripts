#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import multiprocessing as mp
import xmlrpc.client as xmlrpclib
import queue

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



def order_confirmation(pid, data_pool):
    sock = xmlrpclib.ServerProxy(URL, allow_none=True)
    while data_pool:
        try:
            data = data_pool.pop()
            order = sock.execute(DB, UID, PSW, 'sale.order', 'import_action_confirm', data)
            print(order)
        except Exception as e:
            logger.error('Error {0} {1}'.format(data, e))



def confirm_sale_orders():
    manager = mp.Manager()
    data_pool = manager.list()
    process_Q = []

    sock = xmlrpclib.ServerProxy(URL, allow_none=True)
    res = sock.execute(DB, UID, PSW, 'sale.order', 'search_read', [], ['name'])
    order_ids = {rec['name'] : rec['id']  for rec in res}


    with open('files/omlordr1.csv', newline='') as f:
         csv_reader = csv.DictReader(f)
         for vals in csv_reader:
             order_no = vals['ORDER-NO']
             order_id = order_ids.get(order_no)
             if order_id:
                 data_pool.append(order_id)
    with open('files/omlcinv1.csv', newline='') as f:
         csv_reader = csv.DictReader(f)
         for vals in csv_reader:
             name = vals.get('1ST-NAME', '')
             if name == 'VOID':
                 continue
             order_no = vals['ORDER-NO']             
             order_id = order_ids.get(order_no)
             if order_id and order_id not in data_pool:
                 data_pool.append(order_id)
    # data_pool.append(98828)
    for i in range(WORKERS):
        pid = "Worker-%d" % (i + 1)
        worker = mp.Process(name=pid, target=order_confirmation, args=(pid, data_pool))
        process_Q.append(worker)
        worker.start()

    for worker in process_Q:
        worker.join()



if __name__ == "__main__":
    # SALE ORDER
    confirm_sale_orders()
