
import xmlrpc.client
import ssl
import csv


url = "http://localhost:8069/xmlrpc/object"
db = 'pricepaper'
pwd = 'confianzpricepaper'


socket = xmlrpc.client.ServerProxy(url,context=ssl._create_unverified_context())

categories = socket.execute(db, 2, pwd, 'product.category', 'search_read', [], ['id','categ_code'])
categories = {category['categ_code']: category['id'] for category in categories}

input_file = csv.DictReader(open('ivinct.csv'))

with open("ERROR_ct.csv", "wb") as f:
    for line in input_file:
        try:
            vals={'categ_code': line.get('CATEGORY').strip(),
                  'name': line.get('CATEGORY-DESC').strip().title(),
                  }
            if line.get('CATEGORY').strip() not in categories:
                status = socket.execute(db, 2, pwd, 'product.category', 'create', vals)
                print (status)
            else:
                category_id = categories.get(line.get('CATEGORY').strip())
                status = socket.execute(db, 2, pwd, 'product.category', 'write', category_id, vals)
                print (status)
        except:
            print ('Exception')
            f.write(line.get('CATEGORY').strip())
            f.write('\n')
