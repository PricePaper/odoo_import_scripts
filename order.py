# -*- coding: utf-8 -*-

import csv
import multiprocessing as mp
import xmlrpclib

URL = "http://localhost:8069/xmlrpc/object"
DB = 'price_paper'
UID = 2
PSW = 'confianzpricepaper'
WORKERS = 10


# ==================================== SALE ORDER ====================================

def update_sale_order(pid, data_pool, error_ids, partner_ids, term_ids):
    sock = xmlrpclib.ServerProxy(URL, allow_none=True)
    while data_pool:
        # try:
        data = data_pool.pop()
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

        # Check if order exists
        res = sock.execute(DB, UID, PSW, 'search_read', [('name', '=', vals['name'])])
        if res:
            # If order exists, remove followers to prevent Odoo from trying to duplicate
            message_partner_ids = res[0]['message_partner_ids']
            sock.execute(DB, UID, PSW, 'mail.followers', 'unlink', message_partner_ids)
            # Update the DB
            sock.execute(DB, UID, PSW, 'sale.order', 'write', res, vals)
            print(pid, 'UPDATE - SALE ORDER', res)
        else:
            res = sock.execute(DB, UID, PSW, 'sale.order', 'create', vals)
            print(pid, 'CREATE - SALE ORDER', res, order_no)
    # except:
    #     if order_no:
    #         error_ids.append(order_no)
    #     break


def sync_sale_orders():
    manager = mp.Manager()
    data_pool = manager.list()
    error_ids = manager.list()
    write_ids = manager.dict()
    process_Q = []

    fp = open('omlhist1.csv', 'rb')
    csv_reader = csv.DictReader(fp)

    orders = {}
    for vals in csv_reader:
        order_no = vals['ORDER-NO'].strip()
        recs = orders.setdefault(order_no, [])
        recs.append(vals)

    fp.close()

    data_pool = manager.list([{'ref': ref, 'orders': orders[ref]} for ref in orders])

    sock = xmlrpclib.ServerProxy(URL, allow_none=True)
    res = sock.execute(DB, UID, PSW, 'res.partner', 'search_read', ['|', ('active', '=', False), ('active', '=', True)],
                       ['customer_code'])
    customers = {rec['customer_code']: rec['id'] for rec in res}
    partner_ids = manager.dict(customers)

    payment_terms = sock.execute(DB, UID, PSW, 'account.payment.term', 'search_read', [('order_type', '=', 'sale')],
                                 ['id', 'code'])
    term_ids = {term['code']: term['id'] for term in payment_terms}

    orders = None
    customers = None
    res = None
    payment_terms = None

    for i in range(WORKERS):
        pid = "Worker-%d" % (i + 1)
        worker = mp.Process(name=pid, target=update_sale_order,
                            args=(pid, data_pool, error_ids, write_ids, partner_ids, term_ids))
        process_Q.append(worker)
        worker.start()

    for worker in process_Q:
        worker.join()


if __name__ == "__main__":
    # SALE ORDER
    sync_sale_orders()
