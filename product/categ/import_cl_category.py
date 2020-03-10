
import xmlrpc.client
import ssl
import csv



url = "http://localhost:8069/xmlrpc/object"
db = 'pricepaper'
pwd = 'confianzpricepaper'

input_file = 'ivincl.csv'


socket = xmlrpc.client.ServerProxy(url,context=ssl._create_unverified_context())

categories = socket.execute(db, 2, pwd, 'product.category', 'search_read', [], ['id','categ_code'])
categories = {category['categ_code']: category['id'] for category in categories}


input_file = csv.DictReader(open(input_file))

with open("ERROR_cl.csv", "wb") as f, open("parent_missing_cl.csv", "wb") as f1:
    for line in input_file:
        if line.get('MAJOR-CLASS').strip() in categories:
            try:
                vals={'categ_code': line.get('PROD-CODE').strip(),
                      'name': line.get('CLASS-DESCRIPTION').strip().title(),
                      'parent_id': categories.get(line.get('MAJOR-CLASS').strip()),
                      }
                if line.get('PROD-CODE').strip() not in categories:
                    status = socket.execute(db, 2, pwd, 'product.category', 'create', vals)
                    print (status)
                else:
                    category_id = categories.get(line.get('PROD-CODE').strip())
                    status = socket.execute(db, 2, pwd, 'product.category', 'write', category_id, vals)
                    print (status)
            except:
                print ('Exception')
                f.write(line.get('PROD-CODE', '').strip())
                f.write('\n')
        else:
            f1.write(line.get('PROD-COD').strip())
            f1.write('\n')
