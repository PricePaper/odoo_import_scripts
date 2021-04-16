#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import math
import datetime
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
# create file handler which logs warnings and errors
errorlogfile = os.path.splitext(filename)[0] + '-error.log'
eh = logging.FileHandler(errorlogfile, mode='w')
eh.setLevel(logging.WARNING)
# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
fh.setFormatter(formatter)
eh.setFormatter(formatter)
# add the handlers to logger
logger.addHandler(ch)
logger.addHandler(fh)
logger.addHandler(eh)
multiprocessing_logging.install_mp_handler(logger=logger)


# ==================================== SALE ORDER ====================================

def sync_invoices():

    process_Q = []
    missing_invoices=[]
    invoices=[]
    duplicate={}


    credit_inv = []
    fp2 = open('files/rclopen1.csv', 'r')
    csv_reader2 = csv.DictReader(fp2)
    for order in csv_reader2:
        inv_no = order.get('INVOICE-NO', '')
        if inv_no[:2] == 'AC':
            credit_inv.append(inv_no)
    fp2.close()

    order_no = {}
    invoice_header = {}
    fp1 = open('files/omlhist1.csv', 'r')
    csv_reader1 = csv.DictReader(fp1)
    for order in csv_reader1:
        inv_no = order.get('INVOICE-NO', '')
        order_no[inv_no] = order['ORDER-NO']
        if inv_no in credit_inv:
            invoice_header[inv_no] = order

    fp1.close()

    fp3 = open('files/omlhist2.csv', 'r')
    csv_reader3 = csv.DictReader(fp3)
    order_lines = {}
    for details in csv_reader3:
        inv_no = details.get('INVOICE-NO', '')
        if inv_no in credit_inv:
            lines = order_lines.setdefault(inv_no, [])
            lines.append(details)

    sock = xmlrpclib.ServerProxy(URL, allow_none=True)

    misc_product_id = sock.execute(DB, UID, PSW, 'product.product', 'search_read', [('default_code', '=', 'misc' )], ['id'])
    misc_product_id = misc_product_id[0]['id']

    delivery_product_id = sock.execute(DB, UID, PSW, 'product.product', 'search_read', [('default_code', '=', 'delivery_008' )], ['id'])
    delivery_product_id = delivery_product_id[0]['id']

    res = sock.execute(DB, UID, PSW, 'product.product', 'search_read',
                       ['|', ('active', '=', False), ('active', '=', True)], ['default_code'])
    product_ids = {rec['default_code']: rec['id'] for rec in res}


    fiscal_position = sock.execute(DB, UID, PSW, 'account.fiscal.position', 'search_read', [], ['id', 'code'])
    fiscal_positions = {pos['id']: pos['code'] for pos in fiscal_position}

    taxes = sock.execute(DB, UID, PSW, 'account.tax', 'search_read', [], ['id', 'code'])
    tax_ids = {tax['code']: tax['id'] for tax in taxes}

    domain = ['|', ('active', '=', False), ('active', '=', True)]
    res = sock.execute(DB, UID, PSW, 'res.partner', 'search_read', domain,
                       ['customer_code', 'property_account_position_id'])
    partner_tax_ids = {rec['customer_code']: rec['property_account_position_id'][0] for rec in res if
                       rec['property_account_position_id']}
    fp2 = open('files/omlhist1.csv', 'r')
    csv_reader2 = csv.DictReader(fp2)
    order_tax_code_ids = {}
    for line in csv_reader2:
        inv_no = line.get('INVOICE-NO', '')
        if inv_no in credit_inv:
            ship_to_code = line.get('SHIP-TO-CODE', False)
            if ship_to_code and ship_to_code != 'SAME':
                ship_code = line.get('CUSTOMER-CODE', False) and line.get('CUSTOMER-CODE', False) + '-' + line.get(
                    'SHIP-TO-CODE', False)
                fpos_code = partner_tax_ids.get(ship_code, False)
                tax_id = tax_ids.get(fiscal_positions.get(fpos_code, False))
                order_tax_code_ids[line.get('INVOICE-NO')] = [ship_code, tax_id]
            else:
                order_tax_code_ids[line.get('INVOICE-NO')] = [line.get('CUSTOMER-CODE', False),
                                                      tax_ids.get(line.get('TAX-AUTH-CODE', False), False)]


    uoms = sock.execute(DB, UID, PSW, 'uom.uom', 'search_read', [], ['id', 'name'])
    uom_ids = {uom['name']: uom['id'] for uom in uoms}


    res = sock.execute(DB, UID, PSW, 'res.partner', 'search_read', ['|', ('active', '=', False), ('active', '=', True)],
                       ['customer_code'])
    partner_ids = {rec['customer_code']: rec['id'] for rec in res}

    payment_terms = sock.execute(DB, UID, PSW, 'account.payment.term', 'search_read', [('order_type', '=', 'sale')],
                                 ['id', 'code'])
    term_ids = {term['code']: term['id'] for term in payment_terms}

    journal = sock.execute(DB, UID, PSW, 'account.journal', 'search_read', [('name', '=', 'Cash')], ['name'])
    if res:
        cash_journal = journal[0]['id']

    account_account = sock.execute(DB, UID, PSW, 'account.account', 'search_read', [('code', '=', '40100')],
                                 ['code'])


    with open('files/rclopen1.csv', newline='') as f:
        csv_reader = csv.DictReader(f)
        for vals in csv_reader:

            try:

                inv_no = vals.get('INVOICE-NO', '')

                # AO customer payment
                if inv_no[:2] == 'AO' and inv_no[:3] != 'AOD':
                    partner_code = vals.get('CUSTOMER-CODE', '')
                    partner_id = partner_ids.get(partner_code)

                    vals = {
                        'partner_id': partner_id,
                        'payment_type': 'inbound',
                        'partner_type': 'customer',
                        'amount': -float(vals.get('NET-AMT')),
                        'journal_id': cash_journal,
                        'payment_date': vals.get('INVOICE-DATE'),
                        'payment_method_id':1,
                        'communication': inv_no,
                    }
                    res = sock.execute(DB, UID, PSW, 'account.payment', 'create', vals)
                    post = sock.execute(DB, UID, PSW, 'account.payment', 'post', res)
                    logger.debug('Create - Customer Payment {0} {1}'.format(inv_no, res))



                # AC credit note
                if inv_no[:2] == 'AC':

                    order_line_val = order_lines.get(inv_no, False)
                    order_val = invoice_header.get(inv_no, False)
                    term_id = term_ids.get(order_val.get('TERM-CODE', ''))
                    partner_code = order_val.get('CUSTOMER-CODE', '')
                    partner_id = partner_ids.get(partner_code)
                    shipping_id = partner_id
                    ship_to_code = order_val.get('SHIP-TO-CODE', False)

                    # zzzz ship_to_code is garbage, remove it
                    if ship_to_code == "zzzz":
                        ship_to_code = False

                    if  ship_to_code and ship_to_code != 'SAME':
                        shipping_code = order_val.get('CUSTOMER-CODE', False)+'-'+order_val.get('SHIP-TO-CODE', False)
                        shipping_id = partner_ids.get(shipping_code)
                        if not shipping_id:
                            logger.error('Shipping id Missing - Order NO:{0} Shipping_code Code:{1}'.format(order_name, shipping_code))
                            continue
                    inv_date = datetime.datetime.strptime(order_val.get('INVOICE-DATE', ''), "%m/%d/%y").date()
                    inv_date = inv_date.strftime('%Y-%m-%d')
                    invoice_line_ids = []

                    misc_charge = order_val.get('MISC-CHARGE', 0)
                    freight_charge = order_val.get('FREIGHT-AMT', 0)
                    if misc_charge !='0':
                        misc_vals = (0,0,{
                        'account_id':account_account[0]['id'],
                        'product_id': misc_product_id,
                        'name': 'MISC CHARGES',
                        'price_unit': order_val.get('MISC-CHARGE', 0),
                        'quantity': 1,
                        })
                        invoice_line_ids.append(misc_vals)
                    if freight_charge !='0':
                        frieght_vals = (0, 0 , {
                        'account_id':account_account[0]['id'],
                        'product_id': delivery_product_id,
                        'name': 'Frieght CHARGES',
                        'price_unit': order_val.get('FREIGHT-AMT', 0),
                        'quantity': 1,
                        })
                        invoice_line_ids.append(frieght_vals)
                    for line in order_line_val:
                        product_id = product_ids.get(line.get('ITEM-CODE', ''))
                        code1 = str(line.get('ORDERING-UOM')) + '_' + str(line.get('QTY-IN-ORDERING-UM'))
                        code = uom_ids.get(code1)
                        if not product_id:
                            logger.error(
                                'Product Missing - {0} {1}'.format(line.get('ITEM-CODE', ''), line.get('INVOICE-NO', '')))
                            continue
                        if not code:
                            logger.error('UOM Missing - {0} {1} {2}'.format(code1, inv_no, line.get('ITEM-CODE', '')))
                            continue
                        uom_factor = float(line.get('ITEM-QTY-IN-STOCK-UM')) / float(line.get('QTY-IN-ORDERING-UM'))
                        quantity_ordered = float(line.get('QTY-ORDERED')) * uom_factor
                        quantity_shipped = float(line.get('QTY-SHIPPED')) * uom_factor

                        if uom_factor > 1 or math.isclose(1.0, uom_factor):
                            quantity_ordered = round(quantity_ordered, 1)
                            quantity_shipped = round(quantity_shipped, 1)
                        else:
                            quantity_ordered = round(quantity_ordered, 3)
                            quantity_shipped = round(quantity_shipped, 3)

                        line_vals = {
                            'product_id': product_id,
                            'account_id':account_account[0]['id'],
                            'name': line.get('ITEM-DESC'),
                            'price_unit': line.get('PRICE-DISCOUNTED'),
                            'quantity': -quantity_ordered,
                            'working_cost': line.get('TRUE-FIXED-COST'),
                            'lst_price': line.get('PRICE-DISCOUNTED'),
                            'uom_id': code,
                            'invoice_line_tax_ids': False
                        }
                        tax = ''
                        if line.get('TAX-CODE') == '0':
                            tax = order_tax_code_ids.get(line.get('INVOICE-NO'))
                            if not tax or not tax[1]:
                                logger.error('Error Tax missing: Invoice:{0} Item:{1}'.format(line.get('INVOICE-NO'),
                                                                                              line.get('ITEM-CODE', '')))
                                continue
                            line_vals['invoice_line_tax_ids'] = [(6, 0, [tax[1]])]
                        invoice_line_ids.append((0,0,line_vals))

                        inv_vals = {
                            'type': 'out_refund',
                            'partner_id': partner_id,
                            'partner_shipping_id':shipping_id,
                            'comment': order_val.get('ORDER-NO'),
                            'origin': order_no.get(order_val.get('ORDER-NO')),
                            'payment_term_id': term_id,
                            'move_name': inv_no,
                            'date_invoice': inv_date,
                            'invoice_line_ids': invoice_line_ids,

                    }
                    res = sock.execute(DB, UID, PSW, 'account.invoice', 'create', inv_vals)
                    logger.debug(f'Create - Credit Note {partner_code} {inv_no} {res}')


                    amount = sock.execute(DB, UID, PSW, 'account.invoice', 'search_read', [('id', '=', res)], ['amount_total','amount_untaxed', 'amount_tax'])
                    csv_amt = float(vals.get('NET-AMT', '0'))
                    inv_amt = amount[0]['amount_total']

                    if -csv_amt != amount[0]['amount_total']:
                        logger.error(f'Amount mismatch in credit note --- Customer: {partner_code} Invoice amount:{inv_amt} CSV amount:{-csv_amt} Invoice:{inv_no}')
                        continue
                    open_inv = sock.execute(DB, UID, PSW, 'account.invoice', 'action_invoice_open', res)


            except Exception as e:

                logger.error(f'Exception --- Customer: {partner_code} Invoice: {inv_no} error:{e}')
                # break


if __name__ == "__main__":
    # Invoice
    sync_invoices()
