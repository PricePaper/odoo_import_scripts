#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Imports vendor lead time and delivery frequency into Odoo from external csv file"""
import csv
import logging
import os
import xmlrpc.client
from typing import TextIO, List

from scriptconfig import URL, UID, DB, PSW

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


def update_vendors(data: List):
    """Update the vendors in Odoo from a list of dict{}"""
    conn = xmlrpc.client.ServerProxy(URL, allow_none=True)

    for vendor in data:
        try:
            vendor_code: str = vendor['customer_code']
            updates = {
                'delay': vendor['delay'],
                'order_freq': vendor['order_freq']
            }
            # Using the vendor_code from the csv, get the pkey of the res.partner object
            vendor_id: List = conn.execute(DB, UID, PSW, 'res.partner', 'search', [('customer_code', '=', vendor_code)])

            # Make sure we only have one vendor returned from Odoo
            if len(vendor_id) == 1:
                vendor_id: int = vendor_id[0]
            else:
                raise Exception("Duplicate vendor")

            # Update res.partner object with new values from updates{}
            res = conn.execute(DB, UID, PSW, 'res.partner', 'write', vendor_id, updates)

            if res:
                logger.debug(f"Vendor: {vendor_code} with id: {vendor_id} updated")
            else:
                raise Exception("Update failed")
        except Exception as e:
            logger.error(f"Exception {e}\nLine: {vendor}\n")


def load_csv(infile: TextIO) -> List:
    """Return a list of dict{} containing vendors to update"""
    vendors = csv.DictReader(infile)
    # remove inactive vendors from the list we return
    return [v for v in vendors if v['active'] == 'TRUE']


if __name__ == '__main__':
    with open('data/vendors-new.csv', 'r', newline='') as f:
        data = load_csv(f)

    update_vendors(data)
