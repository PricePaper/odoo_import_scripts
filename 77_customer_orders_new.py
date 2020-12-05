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

            partner_code = order_list[0].get('CUSTOMER-CODE', '')
            partner_id = partner_ids.get(partner_code)
            shipping_id = partner_id
            user_id = user_ids.get(sale_rep_ids.get(order_list[0].get('SALESMAN-CODE')))
            term_id = term_ids.get(order_list[0].get('TERM-CODE', ''))
            ship_to_code = order_list[0].get('SHIP-TO-CODE', False)
            if  ship_to_code and ship_to_code != 'SAME':
                shipping_code = partner_code+'-'+ship_to_code
                shipping_id = partner_ids.get(shipping_code, False)
                if not shipping_id:
                    logger.error('Shipping id Missing - Order NO:{0} Shipping_code Code:{1}'.format(order_no, shipping_code))
                    continue
            if not partner_id:
                logger.error('Partner Missing - Order NO:{0} Partner Code:{1}'.format(order_no, partner_code))
                continue
            inv_no = ','.join(order.get('INVOICE-NO', '') for order in order_list)

            vals = {
                'name': order_list[0].get('ORDER-NO', ''),
                'partner_id': partner_id,
                'partner_shipping_id':shipping_id,
                'payment_term_id': term_id,
                'date_order': order_list[0].get('ORDER-DATE', ''),
                'user_id': user_id,
                'note': inv_no,
                'carrier_id': carrier_ids.get(order_list[0].get('CARRIER-CODE'))
            }

            try:
                # Check if order exists
                res = sock.execute(DB, UID, PSW, 'sale.order', 'search_read', [('name', '=', order_no)])
                if res:
                    continue
                else:
                    res = sock.execute(DB, UID, PSW, 'sale.order', 'create', vals)
                    print(pid, 'CREATE - SALE ORDER', res, order_no)
                    misc_charge = order_list[0].get('MISC-CHARGE', 0)
                    freight_charge = order_list[0].get('FREIGHT-AMT', 0)
                    if misc_charge !='0':
                        misc_vals = {
                        'order_id': res,
                        'product_id': misc_product_id,
                        'name': 'MISC CHARGES',
                        'price_unit': order_list[0].get('MISC-CHARGE', 0),
                        'product_uom_qty': 1,
                        'is_delivery': True
                        }
                        sock.execute(DB, UID, PSW, 'sale.order.line', 'create', misc_vals)
                    if freight_charge !='0':
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
                logger.error('Exception --- Order No:{0} error:{1}'.format(order_no, e))
                # data_pool.put(data)
        except Exception as e:
            logger.error('Exception --- error:{}'.format(e))
        finally:
            data_pool.task_done()

def sync_sale_orders():
    manager = mp.Manager()
    data_pool = manager.JoinableQueue()

    orders = {}
    with open('files/omlordr1.csv', newline='') as f:
        csv_reader = csv.DictReader(f)
        for vals in csv_reader:
            order_no = vals['ORDER-NO']
            orders.setdefault(order_no, [])
            orders[order_no].append(vals)

    for ref in orders:
        data_pool.put({'ref': ref, 'orders': orders[ref]})

    sock = xmlrpclib.ServerProxy(URL, allow_none=True)
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
        pro_vals = {'name':'Misc Charge', 'default_code':'misc','type': 'service',}
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
