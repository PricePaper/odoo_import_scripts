#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import multiprocessing as mp
from xmlrpc import client as xmlrpclib

from scriptconfig import URL, DB, UID, PSW, WORKERS


# =================================== C U S T O M E R ========================================

def update_customer(pid, data_pool, write_ids, fiscal_ids, categ_ids, term_ids, carrier_ids, sale_rep_ids, rule_ids,
                    additional_salerep, partner_emails, customer_dates):
    sock = xmlrpclib.ServerProxy(URL, allow_none=True)
    while data_pool:

        try:
            customer_code = ''
            data = data_pool.pop()
            customer_code = data['CUSTOMER-CODE'].strip()
            city, state = data['CITY-STATE'].strip().split(',')
            state = state.strip()
            bill_with_goods = True
            if data['BWG'] == 'N':
                bill_with_goods = False
            # Handle bad partner categories by setting as Undefined -- MUST BE in DB already
            categ_name = data.get('CLASS-CODE').strip()
            categ_id = categ_ids.get(categ_name)
            if not categ_id:
                categ_id = categ_ids.get('UNDEF')
            # Check terms code, if '00' then customer is inactive
            active = True
            term_code = data.get('TERM-CODE').strip()
            if term_code == "0":
                active = False
            vals = {
                'name': data['1ST-NAME'].strip().title(),
                'corp_name': data['2ND-NAME'].strip().title(),
                'street': data['STREET'].strip().title(),
                'city': city.title(),
                'active': active,
                'customer': True,
                'zip': data['ZIP-CODE'],
                'phone': data['PHONE-NO'],
                'customer_code': customer_code,
                'credit_limit': data['CREDIT-LIMIT'],
                'vat': data['RESALE-NO'].strip(),
                'customer_ranking': data['CUSTOMER-RANK'].strip(),
                'category_id': [(6, 0, [categ_id])],
                'property_account_position_id': fiscal_ids.get(data.get('TAX-AUTH-CODE').strip()),
                'property_payment_term_id': term_ids.get(term_code),
                'company_type': 'company',
                'bill_with_goods': bill_with_goods,
                'property_delivery_carrier_id': carrier_ids.get(data.get('CARRIER-CODE').strip()),
                'last_paid_date': data.get('DATE-LAST-PYMT')
            }
            if customer_code in customer_dates:
                line = customer_dates.get(customer_code, '')
                vals['established_date'] = line['ESTBL-DATE']
                vals['last_sold_date'] = line['LST-SLS-DATE']
                #print(vals)
            # If we have an email address for the partner, add it to vals
            partner_email = partner_emails.get(customer_code)
            if partner_email:
                vals['email'] = partner_email

            res = write_ids.get(customer_code, [])
            if res:
                # continue
                sock.execute(DB, UID, PSW, 'res.partner', 'write', res, vals)
                print(pid, 'UPDATE - CUSTOMER', res)
            else:
                vals['commission_percentage_ids'] = [
                    (0, 0, {'sale_person_id': sale_rep_ids.get(data.get('SALESMAN-CODE').strip()),
                            'rule_id': rule_ids.get(sale_rep_ids.get(data.get('SALESMAN-CODE').strip()))})]
                if customer_code in additional_salerep:
                    vals['commission_percentage_ids'].append(
                        (0, 0, {'sale_person_id': sale_rep_ids.get(additional_salerep[customer_code]),
                                'rule_id': rule_ids.get(sale_rep_ids.get(additional_salerep[customer_code]))}))
                res = sock.execute(DB, UID, PSW, 'res.partner', 'create', vals)

                print(pid, 'CREATE - CUSTOMER', res)
        except Exception as e:
            print(customer_code)
            print(e)


def sync_customers():
    manager = mp.Manager()
    data_pool = manager.list()
    write_ids = manager.dict()
    categ_ids = manager.dict()
    term_ids = manager.dict()
    fiscal_ids = manager.dict()
    carrier_ids = manager.dict()
    additional_salerep = manager.dict()
    sale_rep_ids = manager.dict()
    rule_ids = manager.dict()
    partner_emails = manager.dict()
    customer_dates = manager.dict()

    process_Q = []

    additional_salerep = {}
    with open('files/rclcsms1.csv', 'r') as fp1:
        csv_reader1 = csv.DictReader(fp1)
        for vals in csv_reader1:
            rep_code = vals.get('CUST-OV-SALESREP-2', '').strip()
            customer_code = vals.get('CUSTOMER-CODE', False)
            if rep_code:
                additional_salerep[customer_code] = rep_code

    customer_codes = []
    with open('files/rclcust1.csv', 'r') as fp:
        csv_reader = csv.DictReader(fp)
        for vals in csv_reader:
            data_pool.append(vals)
            customer_code = vals['CUSTOMER-CODE'].strip()
            customer_codes.append(customer_code)

    with open('files/rclcust2.csv', 'r') as fp3:
        csv_reader3 = csv.DictReader(fp3)
        for vals in csv_reader3:
            customer_code = vals['CUSTOMER-CODE'].strip()
            customer_dates[customer_code] = vals
    #print(customer_dates)

    with open('files/rclemail.csv') as fp2:
        csv_reader2 = csv.DictReader(fp2)
        for vals in csv_reader2:
            customer_code = vals.get('CUSTOMER-CODE')
            customer_email = vals.get('CUST-E-MAIL-ADD')
            if customer_code and customer_email :
                primary_email = vals.get('PRIMARY-EMAIL       ', '').strip()
                if primary_email == "Y":
                    partner_emails[customer_code.strip()] = customer_email.strip()

    domain = [('customer_code', 'in', customer_codes),'|',('active', '=', False), ('active', '=', True)]
    sock = xmlrpclib.ServerProxy(URL, allow_none=True)

    res = sock.execute(DB, UID, PSW, 'res.partner', 'search_read', domain, ['customer_code'])
    write_ids = {rec['customer_code']: rec['id'] for rec in res}

    fiscal = sock.execute(DB, UID, PSW, 'account.fiscal.position', 'search_read', [], ['id', 'code'])
    fiscal_ids = {rec['code']: rec['id'] for rec in fiscal}

    categ = sock.execute(DB, UID, PSW, 'res.partner.category', 'search_read', [], ['id', 'code'])
    categ_ids = {rec['code']: rec['id'] for rec in categ}

    terms = sock.execute(DB, UID, PSW, 'account.payment.term', 'search_read', [('order_type', '=', 'sale')],
                         ['id', 'code'])
    term_ids = {rec['code']: rec['id'] for rec in terms}

    carriers = sock.execute(DB, UID, PSW, 'delivery.carrier', 'search_read', [], ['id', 'name'])
    carrier_ids = {rec['name']: rec['id'] for rec in carriers}

    sale_rep = sock.execute(DB, UID, PSW, 'res.partner', 'search_read',
                            [('is_sales_person', '=', True), '|', ('active', '=', False), ('active', '=', True)],
                            ['id', 'sales_person_code'])
    sale_rep_ids = {rec['sales_person_code']: rec['id'] for rec in sale_rep}

    rules = sock.execute(DB, UID, PSW, 'commission.rules', 'search_read', [], ['id', 'sales_person_id'])
    rule_ids = {rule['sales_person_id'][0]: rule['id'] for rule in rules}

    res = None
    customer_codes = None

    for i in range(WORKERS):
        pid = "Worker-%d" % (i + 1)
        worker = mp.Process(name=pid, target=update_customer, args=(
            pid, data_pool, write_ids, fiscal_ids, categ_ids, term_ids, carrier_ids, sale_rep_ids, rule_ids,
            additional_salerep, partner_emails, customer_dates))
        process_Q.append(worker)
        worker.start()

    for worker in process_Q:
        worker.join()


if __name__ == "__main__":
    # PARTNER
    sync_customers()
