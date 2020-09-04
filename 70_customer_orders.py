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

def update_sale_order(pid, data_pool, partner_ids, term_ids, user_ids, sale_rep_ids, misc_product_id, delivery_product_id, carrier_ids):
    while True:
        try:
            sock = xmlrpclib.ServerProxy(URL, allow_none=True)
            data = data_pool.get_nowait()
        except queue.Empty:
            break
        try:
            order_no = data.get('ref', '')
            order_list = data.get('orders', [])

            partner_code = order_list[0].get('CUSTOMER-CODE', '').strip()
            partner_id = partner_ids.get(partner_code)
            shipping_id = partner_id
            order_name = order_list[0].get('ORDER-NO', '').strip()
            ship_to_code = order_list[0].get('SHIP-TO-CODE', False)
            user_id = user_ids.get(sale_rep_ids.get(order_list[0].get('SALESMAN-CODE')))
            if  ship_to_code and ship_to_code != 'SAME':
                shipping_code = order_list[0].get('CUSTOMER-CODE', False)+'-'+order_list[0].get('SHIP-TO-CODE', False)
                shipping_id = partner_ids.get(shipping_code)
                if not shipping_id:
                    logger.error('Shipping id Missing - Order NO:{0} Shipping_code Code:{1}'.format(order_name, shipping_code))
                    continue
            term_id = term_ids.get(order_list[0].get('TERM-CODE', '').strip())
            if not partner_id:
                logger.error('Partner Missing - Order NO:{0} Partner Code:{1}'.format(order_name, partner_code))
                continue

            inv_no = ','.join(order.get('INVOICE-NO', '').strip() for order in order_list)
            vals = {
                'name': order_list[0].get('ORDER-NO', '').strip(),
                'partner_id': partner_id,
                'partner_shipping_id':shipping_id,
                'note': inv_no,
                'payment_term_id': term_id,
                'date_order': order_list[0].get('ORDER-DATE', '').strip(),
                'confirmation_date': order_list[0].get('ORDER-DATE', '').strip(),
                'user_id': user_id,
                'carrier_id': carrier_ids.get(order_list[0].get('CARRIER-CODE').strip())
            }

            try:
                res = sock.execute(DB, UID, PSW, 'sale.order', 'create', vals)
                print(pid, 'CREATE - SALE ORDER', res, order_no)
                misc_charge = order_list[0].get('MISC-CHARGE', 0).strip()
                freight_charge = order_list[0].get('FREIGHT-AMT', 0)
                if misc_charge !='0':
                    misc_vals = {
                    'order_id': res,
                    'product_id': misc_product_id,
                    'name': 'MISC CHARGES',
                    'price_unit': order_list[0].get('MISC-CHARGE', 0).strip(),
                    'product_uom_qty': 1,
                    'is_delivery': True
                    }
                    sock.execute(DB, UID, PSW, 'sale.order.line', 'create', misc_vals)
                if misc_charge !='0':
                    frieght_vals = {
                    'order_id': res,
                    'product_id': delivery_product_id,
                    'name': 'Frieght CHARGES',
                    'price_unit': order_list[0].get('FREIGHT-AMT', 0),
                    'product_uom_qty': 1,
                    'is_delivery': True
                    }
                    sock.execute(DB, UID, PSW, 'sale.order.line', 'create', frieght_vals)

            except Exception as e:
                logger.error('Exception --- Order No:{0} error:{1}'.format(order_name, e))
                # data_pool.put(data)
        except Exception as e:
            logger.error('Exception --- error:{}'.format(e))
        finally:
            print('finally')
            data_pool.task_done()

def sync_sale_orders():
    manager = mp.Manager()
    data_pool = manager.JoinableQueue()
    sale_rep_ids = manager.dict()
    user_ids = manager.dict()

    sock = xmlrpclib.ServerProxy(URL, allow_none=True)

    orders = {}
    existing_orders = sock.execute(DB, UID, PSW, 'sale.order', 'search_read', [], ['name'])
    existing_orders = [rec['name'] for rec in existing_orders]
    print(len(existing_orders))


    with open('files/omlhist1.csv', newline='') as f:
        csv_reader = csv.DictReader(f)
        for vals in csv_reader:
            if vals['CUSTOMER-CODE'].strip() == 'VOID':
                continue
            if vals['ORDER-NO'].strip() in existing_orders:
                continue
            else:
                order_no = vals['ORDER-NO'].strip()
                orders.setdefault(order_no, [])
                orders[order_no].append(vals)
    print(len(orders))

    for ref in orders:
        data_pool.put({'ref': ref, 'orders': orders[ref]})

    existing_orders = None



    res = sock.execute(DB, UID, PSW, 'res.partner', 'search_read', ['|', ('active', '=', False), ('active', '=', True)],
                       ['customer_code'])
    customers = {rec['customer_code']: rec['id'] for rec in res}
    partner_ids = manager.dict(customers)

    sale_rep = sock.execute(DB, UID, PSW, 'res.partner', 'search_read',
                            [('is_sales_person', '=', True), '|', ('active', '=', False), ('active', '=', True)],
                            ['id', 'sales_person_code'])
    sale_rep_ids = {rec['sales_person_code']: rec['id'] for rec in sale_rep}

    users = sock.execute(DB, UID, PSW, 'res.users', 'search_read',
                            [],
                            ['id', 'partner_id'])
    user_ids = {rec['partner_id'][0]: rec['id'] for rec in users}

    carriers = sock.execute(DB, UID, PSW, 'delivery.carrier', 'search_read', [], ['id', 'name'])
    carrier_ids = {rec['name']: rec['id'] for rec in carriers}
    carrier_ids = manager.dict(carrier_ids)

    misc_product_id = sock.execute(DB, UID, PSW, 'product.product', 'search_read', [('default_code', '=', 'misc' )], ['id'])
    if not misc_product_id:
        pro_vals = {'name':'Misc Charge', 'default_code':'misc','type': 'service'}
        misc_product_id = sock.execute(DB, UID, PSW, 'product.product', 'create', pro_vals)
    else:
        misc_product_id = misc_product_id[0]['id']
    delivery_product_id = sock.execute(DB, UID, PSW, 'product.product', 'search_read', [('default_code', '=', 'delivery_008' )], ['id'])
    if not delivery_product_id:
        del_pro_vals = {'name':'Delivery Charge', 'default_code':'delivery_008','type': 'service'}
        delivery_product_id = sock.execute(DB, UID, PSW, 'product.product', 'create', del_pro_vals)
    else:
        delivery_product_id = delivery_product_id[0]['id']
    res = sock.execute(DB, UID, PSW, 'product.product', 'search_read', ['|', ('active', '=', False), ('active', '=', True)], ['default_code'])

    payment_terms = sock.execute(DB, UID, PSW, 'account.payment.term', 'search_read', [('order_type', '=', 'sale')],
                                 ['id', 'code'])
    term_ids = {term['code']: term['id'] for term in payment_terms}
    workers = []
    for i in range(WORKERS):
        pid = "Worker-%d" % (i + 1)
        worker = mp.Process(name=pid, target=update_sale_order,
                            args=(pid, data_pool, partner_ids, term_ids, user_ids, sale_rep_ids, misc_product_id, delivery_product_id, carrier_ids))
        worker.start()
        workers.append(worker)

    data_pool.join()

if __name__ == "__main__":
    # SALE ORDER
    sync_sale_orders()
