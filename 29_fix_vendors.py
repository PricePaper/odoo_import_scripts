#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import ssl
from xmlrpc import client as xmlrpclib

from scriptconfig import URL, DB, UID, PSW


# =================================== C U S T O M E R ========================================

def update_customer(data, write_ids, states_ids, name=None):
    sock = xmlrpclib.ServerProxy(URL, allow_none=True, context=ssl._create_unverified_context())
    try:
        customer_code = ''
        customer_code = data.get('VEND-CODE')

        active = True
        if data.get('VEND-STATUS') == "D":
            active = False

        # figure out the state and country
        state_code = data.get('VEND-STATE')
        state_id = states_ids[state_code]['state_id']
        country_id = states_ids[state_code]['country_id']

        vals = {
            'active': active,
            'state_id': state_id,
            'country_id': country_id
        }
        if name:
            vals['company_type'] = 'company'
            vals['is_company'] = True
            # vals['name'] = name
            # vals['display_name'] = name
            vals['lastname'] = name
            # vals['firstname'] = None

        res = write_ids.get(customer_code, [])
        if res:
            sock.execute(DB, UID, PSW, 'res.partner', 'write', res, vals)
            print('UPDATE - VENDOR', res, vals)
        else:
            res = sock.execute(DB, UID, PSW, 'res.partner', 'create', vals)
            print('CREATE - VENDOR', res)
    except Exception as e:
        print(f'Exception: {customer_code}')
        print(e)


def sync_customers():
    data_pool = []
    write_ids = {}
    states_ids = {}
    fix_names = {}

    fp = open('files/aplvend1.csv', 'r')
    csv_reader = csv.DictReader(fp)

    customer_codes = []
    for vals in csv_reader:
        data_pool.append(vals)
        customer_code = vals['VEND-CODE']
        customer_codes.append(customer_code)

    fp.close()

    domain = [('customer_code', 'in', customer_codes), '|', ('active', '=', False), ('active', '=', True)]
    sock = xmlrpclib.ServerProxy(URL, allow_none=True, context=ssl._create_unverified_context())

    res = sock.execute(DB, UID, PSW, 'res.partner', 'search_read', domain, ['customer_code'])
    write_ids = {rec['customer_code']: rec['id'] for rec in res}

    # Get names to fix
    domain = [['customer_code', 'in', customer_codes], ['company_type', '=', 'person'], '|', ('active', '=', False),
              ('active', '=', True)]
    res = sock.execute(DB, UID, PSW, 'res.partner', 'search_read', domain, ['customer_code', 'lastname'])
    fix_names = {rec['customer_code']: rec['lastname'] for rec in res}

    # Get state and country ids
    res = sock.execute(DB, UID, PSW, 'res.country.state', 'search_read',
                       ['|', ('country_id', '=', 233), ('country_id', '=', 38)], ['code', 'id', 'country_id'])
    states_ids = {rec['code']: {'state_id': rec['id'], 'country_id': rec['country_id'][0]} for rec in res}
    res = None
    customer_codes = None

    for rec in data_pool:
        customer_code = rec['VEND-CODE']
        name = fix_names.setdefault(customer_code, None)
        update_customer(rec, write_ids, states_ids, name=name)


if __name__ == "__main__":
    # PARTNER
    sync_customers()
