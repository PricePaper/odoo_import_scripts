#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import multiprocessing as mp
import xmlrpc.client as xmlrpclib
import queue

URL = "https://ean-linux.pricepaper.com/xmlrpc/object"
DB = 'pricepaper'
UID = 2
PSW = 'confianzpricepaper'
WORKERS = 2


# ==================================== SALE ORDER ====================================

def update_sale_order(pid, data_pool, error_ids, partner_ids, term_ids):
    while True:
        try:
            sock = xmlrpclib.ServerProxy(URL, allow_none=True)
            data = data_pool.get_nowait()
        except queue.Empty:
            break
        try:
            order_no = data.get('ref', '')
            order_list = data.get('orders', [])

            partner_id = partner_ids.get(order_list[0].get('CUSTOMER-CODE', '').strip())
            term_id = term_ids.get(order_list[0].get('TERM-CODE', '').strip())
            if not partner_id or not term_id:
                error_ids.append(order_no)
                continue

            inv_no = ','.join(order.get('INVOICE-NO', '').strip() for order in order_list)
            vals = {
                'name': order_list[0].get('ORDER-NO', '').strip(),
                'partner_id': partner_id,
                'note': inv_no,
                'payment_term_id': term_id,
                'date_order': order_list[0].get('ORDER-DATE', '').strip(),
            }

            try:
                # Check if order exists
                res = sock.execute(DB, UID, PSW, 'sale.order', 'search_read', [('name', '=', order_no)])
                if res:
                    order_id = res[0]['id']
                    try:
                        # If order exists, remove followers to prevent Odoo from trying to duplicate
                        message_partner_ids = res[0]['message_partner_ids']
                        if message_partner_ids:
                            sock.execute(DB, UID, PSW, 'mail.followers', 'unlink', message_partner_ids)
                    except ValueError as e:
                        print(e)
                        pass
                    # Update the DB
                    sock.execute(DB, UID, PSW, 'sale.order', 'write', order_id, vals)
                    print(pid, 'UPDATE - SALE ORDER', order_id, res[0]['name'])
                else:
                    res = sock.execute(DB, UID, PSW, 'sale.order', 'create', vals)
                    print(pid, 'CREATE - SALE ORDER', res, order_no)
            except Exception as e:
                data_pool.put(data)
        finally:
            data_pool.task_done()

def sync_sale_orders():
    manager = mp.Manager()
    data_pool = manager.JoinableQueue()
    error_ids = manager.list()

    orders = {}
    with open('omlhist1.csv', newline='') as f:
        csv_reader = csv.DictReader(f)
        for vals in csv_reader:
            order_no = vals['ORDER-NO'].strip()
            orders.setdefault(order_no, [])
            orders[order_no].append(vals)

    for ref in orders:
        data_pool.put({'ref': ref, 'orders': orders[ref]})

    sock = xmlrpclib.ServerProxy(URL, allow_none=True)
    res = sock.execute(DB, UID, PSW, 'res.partner', 'search_read', ['|', ('active', '=', False), ('active', '=', True)],
                       ['customer_code'])
    customers = {rec['customer_code']: rec['id'] for rec in res}
    partner_ids = manager.dict(customers)

    payment_terms = sock.execute(DB, UID, PSW, 'account.payment.term', 'search_read', [('order_type', '=', 'sale')],
                                 ['id', 'code'])
    term_ids = {term['code']: term['id'] for term in payment_terms}
    workers = []
    for i in range(WORKERS):
        pid = "Worker-%d" % (i + 1)
        worker = mp.Process(name=pid, target=update_sale_order,
                            args=(pid, data_pool, error_ids, partner_ids, term_ids))
        worker.start()
        workers.append(worker)

    data_pool.join()

if __name__ == "__main__":
    # SALE ORDER
    sync_sale_orders()
