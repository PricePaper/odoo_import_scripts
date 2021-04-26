#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import multiprocessing as mp
import xmlrpc.client as xmlrpclib
import queue
import math



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


from scriptconfig import URL, DB, UID, PSW, WORKERS

socket = xmlrpclib.ServerProxy(URL)



input_file = csv.DictReader(open("files/omlhist2.csv"))

uoms = socket.execute(DB, 2, PSW, 'uom.uom', 'search_read', [], ['id','name'])
uoms = {uom['name']: uom['id'] for uom in uoms}

data = {}

for line in input_file:
    code = str(line.get('ORDERING-UOM')) + '_' + str(line.get('QTY-IN-ORDERING-UM'))
    code = code
    factor = line.get('QTY-IN-ORDERING-UM')
    factor = float(factor)
    uom_type = 'bigger' if factor > 0 else 'smaller'

    rounding = 1/factor
    vals={'name': code,
          'category_id': 1,
          'active': True,
          'factor_inv': factor,
          'uom_type': uom_type,
          'rounding': rounding
          }
    data[code] = vals

for code in data:
    try:
        if code not in uoms:
            status = socket.execute(DB, 2, PSW, 'uom.uom', 'create', vals)
            uoms.update({code:status})
            logger.info('Created UOM:{0}'.format(code))
        else:
            id = uoms.get(code)
            val={'rounding': vals.get('rounding')}
            status = socket.execute(DB, 2, PSW, 'uom.uom', 'write', id, val)
            logger.info('Updated UOM:{0}'.format(code))
    except Exception as e:
        logger.error('UOM :{0} Exception {1}'.format(code, e))
