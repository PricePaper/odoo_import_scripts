# -*- coding: utf-8 -*-

import csv
import xmlrpclib
import multiprocessing as mp

URL = "http://localhost:8069/xmlrpc/object"
DB = 'pricepaper'
UID = 2
PSW = 'confianzpricepaper'
WORKERS = 10


# =================================== C U S T O M E R ========================================

def update_customer(pid, data_pool, write_ids, term_ids):
    sock = xmlrpclib.ServerProxy(URL, allow_none=True)
    while data_pool:
        try:
            customer_code = ''
            data = data_pool.pop()
            customer_code = data.get('VEND-CODE').strip()
            vals = {
                'name': data.get('VEND-NAME', '').strip().title(),
                'street': data.get('VEND-ADDR1', '').strip().title(),
                'street2': data.get('VEND-ADDR2', '').strip().title(),
                'city': data.get('VEND-CITY', '').strip().title(),
                'active': True,
                'customer': False,
                'supplier': True,
                'property_supplier_payment_term_id': term_ids.get(data.get('TERM-CODE').strip()),
                'zip': data.get('VEND-ZIP-CODE').strip(),
                'phone': data.get('VEND-PHONE').strip(),
                'vat': data.get('VEND-TAX-ID').strip(),
                'customer_code': data.get('VEND-CODE').strip(),
            }

            res = write_ids.get(customer_code, [])
            if res:
                sock.execute(DB, UID, PSW, 'res.partner', 'write', res, vals)
                print(pid, 'UPDATE - CUSTOMER', res)
            else:
                res = sock.execute(DB, UID, PSW, 'res.partner', 'create', vals)
                print(pid, 'CREATE - CUSTOMER', res)
        except:
            error_ids.apppend(customer_code)


def sync_customers():
    manager = mp.Manager()
    data_pool = manager.list()
    error_ids = manager.list()
    write_ids = manager.dict()
    categ_ids = manager.dict()
    term_ids = manager.dict()
    fiscal_ids = manager.dict()
    carrier_ids = manager.dict()
    process_Q = []

    fp = open('aplvend1.csv', 'rb')
    csv_reader = csv.DictReader(fp)

    customer_codes = []
    for vals in csv_reader:
        data_pool.append(vals)
        customer_code = vals['VEND-CODE'].strip()
        customer_codes.append(customer_code)

    fp.close()

    domain = [('customer_code', 'in', customer_codes)]
    sock = xmlrpclib.ServerProxy(URL, allow_none=True)

    res = sock.execute(DB, UID, PSW, 'res.partner', 'search_read', domain, ['customer_code'])
    write_ids = {rec['customer_code']: rec['id']  for rec in res}

    terms = sock.execute(DB, UID, PSW, 'account.payment.term', 'search_read', [('order_type', '=', 'purchase')], ['id','code'])
    term_ids = {rec['code']: rec['id']  for rec in terms}

    res = None
    customer_codes = None

    for i in range(WORKERS):
        pid = "Worker-%d" % (i + 1)
        worker = mp.Process(name=pid, target=update_customer, args=(pid, data_pool, write_ids, term_ids))
        process_Q.append(worker)
        worker.start()

    for worker in process_Q:
        worker.join()


if __name__ == "__main__":

    # PARTNER
    sync_customers()
