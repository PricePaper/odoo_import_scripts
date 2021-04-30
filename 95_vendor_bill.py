#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import multiprocessing as mp
import xmlrpc.client as xmlrpclib
import queue
import math

from scriptconfig import URL, DB, UID, PSW, WORKERS

import logging.handlers
import datetime
import os
import ssl
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


def create_do_invoice(pid, data_pool, partner_ids, term_ids, inv_account, line_account):
    sock = xmlrpclib.ServerProxy(URL, allow_none=True,context=ssl._create_unverified_context())
    while data_pool:
        try:
            msg=''
            data = data_pool.pop()
            partner_id = partner_ids.get(data.get('partner', ''))
            if not partner_id:
                logger.error('Invoice :{0} missing Vendor '.format(data.get('name')))
                continue
            term_id  = term_ids.get(data.get('term_code', ''))
            if not term_id:
                logger.error('Invoice :{0} missing Term Code '.format(data.get('name')))
                continue

            inv_date = datetime.datetime.strptime(data.get('date', ''), "%m/%d/%y").date()
            invoice_date = inv_date.strftime('%Y-%m-%d')

            type = 'in_invoice'
            inv_amt = float(data.get('inv_amt', ''))
            if inv_amt < 0:
                type = 'in_refund'
                inv_amt = -inv_amt



            vals = {'move_name': data.get('name'),
                    'partner_id': partner_id,
                    'date_invoice': invoice_date,
                    'payment_term_id': term_id,
                    'origin': data.get('comment', ''),
                    'type': type,
                    'account_id': inv_account[0],
                    'invoice_line_ids': [(0, 0, {
                                        'name': 'Vendor Bill',
                                        'account_id': line_account[0],
                                        'price_unit': inv_amt,
                                        'quantity': 1,
                                        'discount': 0.0,
                                        'invoice_line_tax_ids': False
                    })]
                    }

            bill = sock.execute(DB, UID, PSW, 'account.invoice', 'create', vals)
            logger.info('Created Invoice {} id: {}'.format(vals.get('move_name'), bill))
            invoice_open = sock.execute(DB, UID, PSW, 'account.invoice', 'action_invoice_open', bill)
            logger.info('Invoice {} validated'.format(vals.get('move_name'), bill))
        except Exception as e:
            logger.error('Invoice :{0} Exception {1}'.format(data.get('name'), e))




def sync_invoices():
    manager = mp.Manager()
    process_Q = []

    domain = ['|',('active', '=', False), ('active', '=', True)]
    sock = xmlrpclib.ServerProxy(URL, allow_none=True,context=ssl._create_unverified_context())

    partners = sock.execute(DB, UID, PSW, 'res.partner', 'search_read', domain, ['customer_code'])
    partner_ids = {rec['customer_code']: rec['id'] for rec in partners}

    terms = sock.execute(DB, UID, PSW, 'account.payment.term', 'search_read', [('order_type', '=', 'purchase')],
                         ['id', 'code'])
    term_ids = {rec['code']: rec['id'] for rec in terms}

    inv_account = sock.execute(DB, UID, PSW, 'account.account', 'search_read', [('code', '=', '20100')],['id', 'code'])
    inv_account = [rec['id'] for rec in inv_account]

    line_account = sock.execute(DB, UID, PSW, 'account.account', 'search_read', [('code', '=', '101120')],
                         ['id', 'code'])
    line_account = [rec['id'] for rec in line_account]
    #
    bill_vals={}

    bill_exists = sock.execute(DB, UID, PSW, 'account.invoice', 'search_read', [('type', 'in', ('in_invoice', 'in_refund'))], ['number'])
    bills = {bill['number']: bill['id'] for bill in bill_exists}
    print(len(bills))

    fp1 = open('files/aplopen1.csv', 'r')
    csv_reader1 = csv.DictReader(fp1)
    for vals in csv_reader1:
        if vals.get('INV-BALANCE', '') and vals.get('INV-BALANCE', '') !='0':
            bill_no = vals.get('INV-NUM', '')+'-checking'
            if bill_no in bills:
                continue
            bill_vals[bill_no] = {'name':bill_no,
                                  'partner': vals.get('VEND-CODE', ''),
                                  'date': vals.get('INV-DATE', ''),
                                  'inv_amt': vals.get('INV-BALANCE', ''),
                                  'term_code': vals.get('TERM-CODE', ''),
                                  'comment': vals.get('PURC-ORDR-NUM', '')
                                  }

    data_pool = manager.list([bill_vals[bill] for bill in bill_vals])

    for i in range(WORKERS):
        pid = "Worker-%d" % (i + 1)
        worker = mp.Process(name=pid, target=create_do_invoice,
            args=(pid, data_pool, partner_ids, term_ids, inv_account, line_account))
        process_Q.append(worker)
        worker.start()

    for worker in process_Q:
        worker.join()

if __name__ == "__main__":
    # Invoice
    sync_invoices()
