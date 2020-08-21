#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import multiprocessing as mp
import xmlrpc.client as xmlrpclib
import queue

from scriptconfig import URL, DB, UID, PSW, WORKERS


# ==================================== SALE ORDER ====================================

def update_sale_order(pid, data_pool, error_ids, partner_ids, term_ids, user_ids, sale_rep_ids, misc_product_id, delivery_product_id):
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
            shipping_id = partner_ids.get(order_list[0].get('CUSTOMER-CODE', '').strip())
            ship_to_code = order_list[0].get('SHIP-TO-CODE', False)
            user_id = user_ids.get(sale_rep_ids.get(order_list[0].get('SALESMAN-CODE')))
            if  ship_to_code and ship_to_code != 'SAME':
                shipping_code = order_list[0].get('CUSTOMER-CODE', False)+'-'+order_list[0].get('SHIP-TO-CODE', False)
                shipping_id = partner_ids.get(shipping_code)
            term_id = term_ids.get(order_list[0].get('TERM-CODE', '').strip())
            if not partner_id or not term_id:
                error_ids.append(order_no)
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
                'user_id': user_id
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
                    misc_vals = {
                    'order_id': res,
                    'product_id': misc_product_id,
                    'name': 'MISC CHARGES',
                    'price_unit': order_list[0].get('MISC-CHARGE', '').strip(),
                    'product_uom_qty': 1,
                    'is_delivery': True
                    }
                    sock.execute(DB, UID, PSW, 'sale.order.line', 'create', misc_vals)
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
                data_pool.put(data)
        finally:
            data_pool.task_done()

def sync_sale_orders():
    manager = mp.Manager()
    data_pool = manager.JoinableQueue()
    error_ids = manager.list()
    sale_rep_ids = manager.dict()
    user_ids = manager.dict()

    orders = {}
    with open('files/omlhist1.csv', newline='') as f:
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

    sale_rep = sock.execute(DB, UID, PSW, 'res.partner', 'search_read',
                            [('is_sales_person', '=', True), '|', ('active', '=', False), ('active', '=', True)],
                            ['id', 'sales_person_code'])
    sale_rep_ids = {rec['sales_person_code']: rec['id'] for rec in sale_rep}

    users = sock.execute(DB, UID, PSW, 'res.users', 'search_read',
                            [],
                            ['id', 'partner_id'])
    user_ids = {rec['partner_id'][0]: rec['id'] for rec in users}

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
                            args=(pid, data_pool, error_ids, partner_ids, term_ids, user_ids, sale_rep_ids, misc_product_id, delivery_product_id))
        worker.start()
        workers.append(worker)

    data_pool.join()

if __name__ == "__main__":
    # SALE ORDER
    sync_sale_orders()
