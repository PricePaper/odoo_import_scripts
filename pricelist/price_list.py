# -*- coding: utf-8 -*-

import csv
import xmlrpclib
import multiprocessing as mp

URL = "http://localhost:8069/xmlrpc/object"
DB = 'pricepaper'
UID = 2
PSW = 'confianzpricepaper'
WORKERS = 10


# =================================== PRICE LIST ========================================

def update_price_list(pid, data_pool, write_ids, uom_ids, partner_ids, pricelist_ids, shared_list, shared_dict, product_ids, broken_uom):
    sock = xmlrpclib.ServerProxy(URL, allow_none=True)
    while data_pool:
        # try:
            data = data_pool.pop()
            price_list = data.get('pricelist_id', '').strip()
            pricelist_id = write_ids.get(price_list, '')
            vals={
                  'name': price_list,
                  'type': 'customer'
                  }
            if not pricelist_id and price_list not in shared_dict:
                if price_list in shared_list:
                    vals['type'] = 'shared'
                id = sock.execute(DB, UID, PSW, 'product.pricelist', 'create', vals)
                print(pid, 'CREATE - PRICELIST', price_list)
                write_ids[price_list] = id



            line_ids = sock.execute(DB, UID, PSW, 'customer.product.price', 'search_read', [('pricelist_id', '=', pricelist_id)], ['product_id', 'product_uom', 'price'])
            pricelist_line_ids={}
            for rec in line_ids:
                if rec['product_id'][0] in pricelist_line_ids:
                    if rec['product_uom'][0] not in pricelist_line_ids[rec['product_id'][0]]:
                        pricelist_line_ids[rec['product_id'][0]][rec['product_uom'][0]] = [rec['price'], rec['id']]
                else:
                    pricelist_line_ids[rec['product_id'][0]] = {rec['product_uom'][0]: [rec['price'], rec['id']]}

            for line in data.get('lines', []):
                product_code = line.get('ITEM-CODE', '').strip()
                product = product_ids.get(product_code)
                product_id = product and product[0]
                uom = line.get('PRICING-UOM').strip()
                if product and uom == product[1][1].split('_')[0]:
                    uom_id = product[1][0]
                elif product_code in broken_uom and uom in broken_uom[product_code]:
                    uom_code = uom + '_' + broken_uom[product_code][uom]
                    uom_id = uom_ids.get(uom_code)
                else:
                    print('UOM mismatch')


                if product_id:
                    if uom_id:
                        vals = {
                                'pricelist_id':pricelist_id,
                                'product_id': product_id,
                                'product_uom': uom_id,
                                'price': line.get('CURRENT-PRICE-IN-STK', 0),
                                'price_last_updated': line.get('LAST-PRICE-CHANGE-DA').strip()
                             }
                        if price_list not in shared_list:
                            vals['partner_id'] = partner_ids.get(line.get('CUSTOMER-CODE').strip())
                        status=''
                        if product_id in pricelist_line_ids and uom_id in pricelist_line_ids[product_id]:
                            write_id = pricelist_line_ids[product_id][uom_id][1]
                            status = sock.execute(DB, UID, PSW, 'customer.product.price', 'write', write_id, vals)
                            print(pid, 'UPDATE - LINE', status)
                        else:
                            status = sock.execute(DB, UID, PSW, 'customer.product.price', 'create', vals)
                            # print(vals)
                            print(pid, 'CREATE - LINE', status)
                            break

        # except:
        #     break
            # error_ids.apppend(customer_code)

def sync_price_list():
    manager = mp.Manager()
    # error_ids = manager.list()

    process_Q = []

    fp = open('omlphist.csv', 'rb')
    fp1 = open('rclcust2.csv', 'rb')
    csv_reader = csv.DictReader(fp)
    csv_reader1 = csv.DictReader(fp1)   #line.get('PRICING-ACCT-NO').strip()
    fp2 = open('ivlitum1.csv', 'rb')
    csv_reader2 = csv.DictReader(fp2)

    broken_uom = {}
    for vals in csv_reader2:
        product_code = vals.get('ITEM-CODE').strip()
        if product_code not in broken_uom:
            broken_uom[product_code] = {vals.get('UOM').strip(): vals.get('QTY').strip()}
        else:
            if vals.get('UOM').strip() not in broken_uom[product_code]:
                broken_uom[product_code][vals.get('UOM').strip()] = vals.get('QTY').strip()
    broken_uom = manager.dict(broken_uom)




    shared_dict={}
    shared_list=[]

    for vals in csv_reader1:
        if vals.get('PRICING-ACCT-NO', '').strip():
            shared_dict[vals.get('CUSTOMER-CODE').strip()] = vals.get('PRICING-ACCT-NO', '').strip()
            if vals.get('PRICING-ACCT-NO', '').strip() not in shared_list:
                shared_list.append(vals.get('PRICING-ACCT-NO', '').strip())
    shared_list = manager.list(shared_list)
    shared_dict = manager.dict(shared_dict)

    price_lists = {}
    for vals in csv_reader:
        customer_code = vals.get('CUSTOMER-CODE').strip()
        lines = price_lists.setdefault(customer_code, [])
        lines.append(vals)

    data_pool = manager.list([{'pricelist_id': price_list, 'lines': price_lists[price_list]} for price_list in price_lists])


    fp.close()

    sock = xmlrpclib.ServerProxy(URL, allow_none=True)

    res = sock.execute(DB, UID, PSW, 'product.pricelist', 'search_read', [], ['name'])
    pricelist_ids = {rec['name']: rec['id']  for rec in res}

    write_ids = manager.dict(pricelist_ids)

    res = ''


    res = sock.execute(DB, UID, PSW, 'res.partner', 'search_read', ['|', ('active', '=', False), ('active', '=', True)], ['customer_code'])
    customers = {rec['customer_code']: rec['id']  for rec in res}
    partner_ids = manager.dict(customers)

    res = sock.execute(DB, UID, PSW, 'product.product', 'search_read', ['|', ('active', '=', False), ('active', '=', True)], ['default_code', 'sale_uoms', 'uom_id'])
    products = {rec['default_code']: [rec['id'], rec['uom_id'], rec['sale_uoms']]  for rec in res}
    product_ids = manager.dict(products)


    uoms = sock.execute(DB, UID, PSW, 'uom.uom', 'search_read', [], ['id','name'])
    uom_ids = manager.dict({uom['name']: uom['id'] for uom in uoms})


    res = None
    customer_codes = None

    for i in range(WORKERS):
        pid = "Worker-%d" % (i + 1)
        worker = mp.Process(name=pid, target=update_price_list, args=(pid, data_pool, write_ids, uom_ids, partner_ids, pricelist_ids, shared_list, shared_dict, product_ids, broken_uom))
        process_Q.append(worker)
        worker.start()

    for worker in process_Q:
        worker.join()



def sync_partner_pricelist():
    sock = xmlrpclib.ServerProxy(URL, allow_none=True)

    res = sock.execute(DB, UID, PSW, 'res.partner', 'search_read', [('customer', '=', True), '|', ('active', '=', False), ('active', '=', True)], ['customer_code'])
    partner_ids = {rec['customer_code']: rec['id']  for rec in res}

    res = sock.execute(DB, UID, PSW, 'product.pricelist', 'search_read', [], ['name'])
    pricelist_ids = {rec['name']: rec['id']  for rec in res}

    res = ''
    customer_price_list={}
    res = sock.execute(DB, UID, PSW, 'customer.pricelist', 'search_read', [], ['partner_id', 'id'])
    for rec in res:
        if rec['partner_id']:
            lines = customer_price_list.setdefault(rec['partner_id'][0], [])
            lines.append(rec['id'])

    fp1 = open('rclcust2.csv', 'rb')
    csv_reader1 = csv.DictReader(fp1)

    shared_dict={}

    for vals in csv_reader1:
        if vals.get('PRICING-ACCT-NO', '').strip():
            shared_dict[vals.get('CUSTOMER-CODE').strip()] = vals.get('PRICING-ACCT-NO', '').strip()

    for rec in shared_dict:
        partner_id = partner_ids.get(rec, '')
        if partner_id in customer_price_list:
            unlink_list = customer_price_list.get(rec, '')
            shared_id = pricelist_ids.get(vals.get('PRICING-ACCT-NO', '').strip())
            if unlink_list:
                pass
                sock.execute(DB, UID, PSW, 'customer.pricelist', 'unlink', unlink_list)
                print('Deleted')
            vals={
                'partner_id': partner_id,
                'pricelist_id': shared_id
            }
            status = sock.execute(DB, UID, PSW, 'customer.pricelist', 'create', vals)
            print('Updated Customer ', status)



if __name__ == "__main__":

    # price_list
    sync_price_list()

    #pricelist_lines
    sync_partner_pricelist()
