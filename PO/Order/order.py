# -*- coding: utf-8 -*-

import csv
import xmlrpclib
import multiprocessing as mp

URL = "http://localhost:8069/xmlrpc/object"
DB = 'pricepaper'
UID = 2
PSW = 'confianzpricepaper'
WORKERS = 10

# ==================================== Purchase ORDER ====================================

def update_purchase_order(pid, data_pool, error_ids, write_ids, partner_ids, term_ids):
    sock = xmlrpclib.ServerProxy(URL, allow_none=True)
    while data_pool:
        try:
            data = data_pool.pop()
            order_no = data.get('ORDR-NUM', '')

            partner_id = partner_ids.get(data.get('VEND-CODE', '').strip())
            term_id = term_ids.get(data.get('TERM-CODE', '').strip())
            if not partner_id or not term_id:
                error_ids.append(order_no)
                continue

            vals={'name': data.get('ORDR-NUM', '').strip(),
                  'partner_id': partner_id,
                  'date_order': data.get('ORDR-DATE').strip(),
                  'release_date': data.get('ORDR-RELEASE-DATE').strip(),
                  'payment_term_id': term_id,
                  # 'state': 'purchase'
                  }

            res = write_ids.get(order_no, [])
            if res:

                sock.execute(DB, UID, PSW, 'purchase.order', 'write', res, vals)
                print(pid, 'UPDATE - SALE ORDER', res)
            else:
                res = sock.execute(DB, UID, PSW, 'purchase.order', 'create', vals)
                print(pid, 'CREATE - SALE ORDER', res, order_no)
            if not data_pool:
                break
        except:
            break


def sync_purchase_orders():
    manager = mp.Manager()
    data_pool = manager.list()
    error_ids = manager.list()
    write_ids = manager.dict()
    process_Q = []

    fp = open('polhist1.csv', 'rb')
    csv_reader = csv.DictReader(fp)

    for vals in csv_reader:
        data_pool.append(vals)

    fp.close()

    sock = xmlrpclib.ServerProxy(URL, allow_none=True)
    res = sock.execute(DB, UID, PSW, 'res.partner', 'search_read', ['|', ('active', '=', False), ('active', '=', True)], ['customer_code'])
    vendor = {rec['customer_code']: rec['id']  for rec in res}
    partner_ids = manager.dict(vendor)

    res = sock.execute(DB, UID, PSW, 'purchase.order', 'search_read', [], ['name'])
    write_ids = {rec['name']: rec['id']  for rec in res}

    payment_terms = sock.execute(DB, UID, PSW, 'account.payment.term', 'search_read', [('order_type','=','purchase')], ['id','code'])
    term_ids = {term['code']: term['id'] for term in payment_terms}


    orders = None
    vendor = None
    res = None
    payment_terms = None

    for i in range(WORKERS):
        pid = "Worker-%d" % (i + 1)
        worker = mp.Process(name=pid, target=update_purchase_order, args=(pid, data_pool, error_ids, write_ids, partner_ids, term_ids))
        process_Q.append(worker)
        worker.start()

    for worker in process_Q:
        worker.join()


if __name__ == "__main__":

    sync_purchase_orders()
