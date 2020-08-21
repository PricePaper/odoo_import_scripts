#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import multiprocessing as mp
import xmlrpc.client as xmlrpclib
import queue

from scriptconfig import URL, DB, UID, PSW, WORKERS


# ==================================== SALE ORDER ====================================

def update_invoice(pid, orders):
    while True:
        try:
            sock = xmlrpclib.ServerProxy(URL, allow_none=True)
            data = orders.pop()
        except:
            break
        try:
            res = sock.execute(DB, UID, PSW, 'sale.order', 'action_invoice_create', data, True)
            print(pid, 'create - INVOICE', data, res)
        except Exception as e:

            print (e)
            # orders.append(data)
            continue


def sync_invoices():
    manager = mp.Manager()
    orders = manager.list()
    process_Q = []

    sock = xmlrpclib.ServerProxy(URL, allow_none=True)
    res = sock.execute(DB, UID, PSW, 'sale.order', 'search_read', [], ['note'])
    order_ids = {inv_no: rec['id'] for rec in res for inv_no in (rec['note'] or '').split(',')}

    orders = []
    with open('rclopen1.csv', newline='') as f:
        csv_reader = csv.DictReader(f)
        for vals in csv_reader:
            inv_no = vals.get('INVOICE-NO', '').strip()
            order_id = order_ids.get(inv_no)
            if order_id:
                orders.append(order_id)


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
