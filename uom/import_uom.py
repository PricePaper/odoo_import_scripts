import xmlrpc.client
import ssl
import csv



url = "http://localhost:8069/xmlrpc/object"
db = 'pricepaper'
pwd = 'confianzpricepaper'


socket = xmlrpc.client.ServerProxy(url,context=ssl._create_unverified_context())



input_file = csv.DictReader(open("ivlitum1.csv"))

uoms = socket.execute(db, 2, pwd, 'uom.uom', 'search_read', [], ['id','name'])
uoms = {uom['name']: uom['id'] for uom in uoms}

with open("ERROR.csv", "wb") as f:
    for line in input_file:
        code = str(line.get('UOM')).strip() + '_' + str(line.get('QTY')).strip()
        code = code.strip()
        factor = line.get('QTY').strip()
        factor = float(factor)
        uom_type = 'bigger' if factor > 0 else 'smaller'
        if code not in uoms:
            try:
                vals={'name': code,
                      'category_id': 1,
                      'active': True,
                      'factor_inv': factor,
                      'uom_type': uom_type
                      }
                status = socket.execute(db, 2, pwd, 'uom.uom', 'create', vals)
                uoms.update({code:status})
                print (vals, "   :", status)
            except:
                f.write(code)
                f.write('\n')

#305
